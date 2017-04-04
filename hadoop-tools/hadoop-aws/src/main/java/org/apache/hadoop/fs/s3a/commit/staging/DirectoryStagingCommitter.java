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

package org.apache.hadoop.fs.s3a.commit.staging;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.s3a.commit.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This commits to a directory.
 * The conflict policy is
 * <ul>
 *   <li>FAIL: fail the commit</li>
 *   <li>APPEND: add extra data to the destination.</li>
 *   <li>REPLACE: delete the destination directory.</li>
 * </ul>
 */
public class DirectoryStagingCommitter extends StagingS3GuardCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectoryStagingCommitter.class);

  public DirectoryStagingCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
  }

  public DirectoryStagingCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    super.setupJob(context);
    Path outputPath = getOutputPath(context);
    FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
    if (getConflictResolutionMode(context) == ConflictResolution.FAIL
        && fs.exists(outputPath)) {
      throw new PathExistsException(outputPath.toString());
    }
  }

  /**
   * Pre-commit actions for a job.
   * Here: look at the conflict resolution mode and choose
   * an action based on the current policy.
   * @param context job context
   * @param pending pending commits
   * @throws IOException any failure
   */
  @Override
  protected void preCommitJob(JobContext context,
      List<SinglePendingCommit> pending) throws IOException {
    Path outputPath = getOutputPath(context);
    FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
    switch (getConflictResolutionMode(context)) {
    case FAIL:
      // this was checked in setupJob, but this avoids some cases where
      // output was created while the job was processing
      if (fs.exists(outputPath)) {
        throw new PathExistsException(outputPath.toString());
      }
      break;
    case APPEND:
      // do nothing
      break;
    case REPLACE:
      LOG.debug("{}: removing output path to be replaced: {}",
          getRole(), outputPath);
      fs.delete(outputPath, true /* recursive */);
      break;
    default:
      throw new IOException(getRole() + ": unknown conflict resolution mode: "
          + getConfictModeOption(context));
    }
  }
}
