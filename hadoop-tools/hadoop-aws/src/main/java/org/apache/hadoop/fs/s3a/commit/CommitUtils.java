/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.commit;

import com.amazonaws.services.s3.model.PartETag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static com.google.common.base.Preconditions.*;

/**
 * Static utility methods related to S3A commitment processing, both
 * staging and magic.
 */
public final class CommitUtils {

  public static final String E_BAD_PATH
      = "Path does not represent a magic-commit path";

  public static final String E_NORMAL_FS
      = "Filesystem does not have support for 'magic' committer enabled";

  public static final String E_WRONG_FS
      = "Output path is not on an S3A Filesystem";

  public static final String E_NO_MAGIC_PATH_ELEMENT
      = "No " + MAGIC_DIR_NAME + " element in path";

  private CommitUtils() {
  }

  /**
   * Take an absolute path, split it into a list of elements.
   * If empty, the path is the root path.
   * @param path input path
   * @return a possibly empty list of elements.
   * @throws IllegalArgumentException if the path is invalid -relative, empty...
   */
  public static List<String> splitPathToElements(Path path) {
    String uriPath = path.toUri().getPath();
    checkArgument(!uriPath.isEmpty(), "empty path");
    checkArgument(uriPath.charAt(0) == '/', "path is relative");
    if ("/".equals(uriPath)) {
      // special case: empty list
      return new ArrayList<>(0);
    }
    path.depth();
    List<String> elements = new ArrayList<>();
    int len = uriPath.length();
    int firstElementChar = 1;
    int endOfElement = uriPath.indexOf('/', firstElementChar);
    while (endOfElement > 0) {
      elements.add(uriPath.substring(firstElementChar, endOfElement));
      firstElementChar = endOfElement + 1;
      endOfElement = firstElementChar == len ? -1
          : uriPath.indexOf('/', firstElementChar);
    }
    // expect a possible child element here
    if (firstElementChar != len) {
      elements.add(uriPath.substring(firstElementChar));
    }
    return elements;
  }

  /**
   * Is a path in the magic tree?
   * @param elements element list
   * @return true if a path is considered pending
   */
  public static boolean isMagicPath(List<String> elements) {
    return elements.contains(MAGIC_DIR_NAME);
  }

  /**
   * Does the list of pending elements contain a base path marker?
   * @param pendingPathElements element list, already stripped out
   * from the pending tree.
   * @return true if a path has a pending directory
   */
  public static boolean containsBasePath(List<String> pendingPathElements) {
    return pendingPathElements.contains(BASE_PATH);
  }

  /**
   * Get the index of the pending path element.
   * @param elements full path element list
   * @return the index.
   * @throws IllegalArgumentException if there is no pending element
   */
  public static int magicElementIndex(List<String> elements) {
    return getElementIndex(MAGIC_DIR_NAME, elements);
  }

  protected static int getElementIndex(String name, List<String> elements) {
    int index = elements.indexOf(name);
    checkArgument(index >= 0, E_NO_MAGIC_PATH_ELEMENT);
    return index;
  }

  /**
   * Get the parent path elements of the magic path.
   * The list may be immutable or may be a view of the underlying list.
   * Both the parameter list and the returned list MUST NOT be modified.
   * @param elements full path element list
   * @return the parent elements; may be empty
   */
  public static List<String> magicPathParents(List<String> elements) {
    return elements.subList(0, magicElementIndex(elements));
  }

  /**
   * Get the child path elements under the magic path.
   * The list may be immutable or may be a view of the underlying list.
   * Both the parameter list and the returned list MUST NOT be modified.
   * @param elements full path element list
   * @return the child elements; may be empty
   */
  public static List<String> magicPathChildren(List<String> elements) {
    int index = magicElementIndex(elements);
    int len = elements.size();
    if (index == len - 1) {
      // empty index
      return Collections.emptyList();
    } else {
      return elements.subList(index + 1, len);
    }
  }

  /**
   * Get any child path elements under any {@code __base} path,
   * or an empty list if there is either: no {@code __base} path element,
   * or no child entries under it.
   * The list may be immutable or may be a view of the underlying list.
   * Both the parameter list and the returned list MUST NOT be modified.
   * @param elements full path element list
   * @return the child elements; may be empty
   */
  public static List<String> basePathChildren(List<String> elements) {
    int index = elements.indexOf(BASE_PATH);
    if (index < 0) {
      return Collections.emptyList();
    }
    int len = elements.size();
    if (index == len - 1) {
      // empty index
      return Collections.emptyList();
    } else {
      return elements.subList(index + 1, len);
    }
  }

  public static String elementsToKey(List<String> elements) {
    return StringUtils.join("/", elements);
  }

  /**
   * Get the filename of a path: the last element.
   * @param elements element list.
   * @return the filename; the last element.
   */
  public static String filename(List<String> elements) {
    return lastElement(elements);
  }

  /**
   * Last element of a (non-empty) list.
   * @param strings strings in
   * @return the last one.
   */
  public static String lastElement(List<String> strings) {
    checkArgument(!strings.isEmpty(), "empty list");
    return strings.get(strings.size() - 1);
  }

  /**
   * Get the magic subdirectory of a destination directory.
   * @param destDir the destination directory
   * @return a new path.
   */
  public static Path magicSubdir(Path destDir) {
    return new Path(destDir.getParent(), MAGIC_DIR_NAME);
  }

  /**
   * Calculates the final destination of a file.
   * This is the parent of any {@code __pending} element, and the filename
   * of the path. That is: all intermediate child path elements are discarded.
   * Why so? paths under the pending path include job attempt and task attempt
   * subdirectories, which need to be skipped.
   *
   * If there is a {@code __base} directory in the children, then it becomes
   * a base for unflattened paths, that is: all its children are pulled into
   * the final destination.
   * @param elements element list.
   * @return the path
   */
  public static List<String> finalDestination(List<String> elements) {
    if (isMagicPath(elements)) {
      List<String> destDir = magicPathParents(elements);
      List<String> children = magicPathChildren(elements);
      checkArgument(!children.isEmpty(), "No path found under " +
          MAGIC_DIR_NAME);
      ArrayList<String> dest = new ArrayList<>(destDir);
      if (containsBasePath(children)) {
        // there's a base marker in the path
        List<String> baseChildren = basePathChildren(children);
        checkArgument(!baseChildren.isEmpty(),
            "No path found under " + BASE_PATH);
        dest.addAll(baseChildren);
      } else {
        dest.add(filename(children));
      }
      return dest;
    } else {
      return elements;
    }
  }

  /**
   * Convert an ordered list of strings to a list of index etag parts.
   * @param tagIds list of tags
   * @return same list, now in numbered tuples
   */
  public static List<PartETag> toPartEtags(List<String> tagIds) {
    List<PartETag> etags = new ArrayList<>(tagIds.size());
    for (int i = 0; i < tagIds.size(); i++) {
      etags.add(new PartETag(i + 1, tagIds.get(i)));
    }
    return etags;
  }

  /**
   * Verify that that path is a delayed commit path.
   * @param fs filesystem
   * @param path path
   * @throws PathCommitException if the path isn't a delayed commit path
   */
  public static void verifyIsDelayedCommitPath(S3AFileSystem fs,
      Path path) throws PathCommitException {
    verifyIsMagicCommitFS(fs);
    if (!fs.isDelayedCompletePath(path)) {
      throw new PathCommitException(path, E_BAD_PATH);
    }
  }

  /**
   * Verify that an FS is a delayed commit FS.
   * @param fs filesystem
   * @throws PathCommitException if the FS isn't a delayed commit FS.
   */
  public static void verifyIsMagicCommitFS(S3AFileSystem fs)
      throws PathCommitException {
    if (!fs.isDelayedCompleteEnabled()) {
      throw new PathCommitException(fs.getUri().toString(),
          E_NORMAL_FS);
    }
  }

  /**
   * Verify that an FS is an S3A FS.
   * @param fs filesystem
   * @param path path to to use in exception
   * @throws PathCommitException if the FS is not an S3A FS.
   */
  public static void verifyIsS3AFS(FileSystem fs, Path path)
      throws PathCommitException {
    if (!(fs instanceof S3AFileSystem)) {
      throw new PathCommitException(path, E_WRONG_FS);
    }
  }

  /**
   * Get the S3A FS of a path.
   * @param path path to examine
   * @param conf config
   * @param magicCommitRequired is delayed complete requires of the FS
   * @throws PathCommitException output path isn't to an S3A FS instance, or
   * if {@code magicCommitRequired} is set, if doesn't support these commits.
   * @throws IOException failure to instantiate the FS.
   */
  public static S3AFileSystem getS3AFileSystem(Path path,
      Configuration conf,
      boolean magicCommitRequired)
      throws IOException, PathCommitException {
    FileSystem fs = path.getFileSystem(conf);
    verifyIsS3AFS(fs, path);
    S3AFileSystem s3a = (S3AFileSystem) fs;
    if (magicCommitRequired) {
      verifyIsMagicCommitFS(s3a);
    }
    return s3a;
  }

  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  public static Path getMagicJobAttemptsPath(Path out) {
    return new Path(out, MAGIC_DIR_NAME);
  }

  /**
   * Get the Application Attempt Id for this job.
   * @param context the context to look in
   * @return the Application Attempt Id for a given job.
   */
  public static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  public static Path getMagicJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getMagicJobAttemptsPath(out),
        formatAppAttemptDir(appAttemptId));
  }

  /**
   * Format the application attempt directory.
   * @param attemptId attempt ID
   * @return the directory name for the application attempt
   */
  public static String formatAppAttemptDir(int attemptId) {
    return String.format("app-attempt-%04d", attemptId);
  }

  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks.
   * @return the path where the output of pending task attempts are stored.
   */
  public static Path getMagicTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getMagicJobAttemptPath(
        getAppAttemptId(context), out), "tasks");
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * This path is marked as a base path for relocations, so subdirectory
   * information is preserved.
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public static Path getMagicTaskAttemptPath(TaskAttemptContext context,
      Path out) {
    Path p1 = new Path(getMagicTaskAttemptsPath(context, out),
        String.valueOf(context.getTaskAttemptID()));
    return new Path(p1, BASE_PATH);
  }

  /**
   * Compute a path for temporary data associated with a job.
   * This data is <i>not pending</i>
   * @param appAttemptId the ID of the application attempt for this job.
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempJobAttemptPath(int appAttemptId, Path out) {
    return new Path(new Path(out, TEMP_DATA_PATH),
        formatAppAttemptDir(appAttemptId));
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context task context
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempTaskAttemptPath(TaskAttemptContext context,
      Path out) {
    return new Path(getTempJobAttemptPath(getAppAttemptId(context), out),
        String.valueOf(context.getTaskAttemptID()));
  }

}
