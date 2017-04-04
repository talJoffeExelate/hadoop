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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * A committer factory which chooses the committer based on the
 * specific option chosen in a per-bucket basis from the property
 * {@link CommitConstants#FS_S3A_COMMITTER_NAME}.
 */
public class DynamicCommitterFactory extends Abstract3GuardCommitterFactory {

  /**
   * Name of this class: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory";

  /**
   * Dynamically create a job committer.
   * @param fileSystem destination FS.
   * @param outputPath final output path for work
   * @param context job context
   * @return a committer
   * @throws IOException instantiation failure
   */
  @Override
  public PathOutputCommitter createJobCommitter(S3AFileSystem fileSystem,
      Path outputPath,
      JobContext context) throws IOException {
    Abstract3GuardCommitterFactory factory = chooseCommitter(fileSystem,
        outputPath, context);
    if (factory != null) {
      return factory.createJobCommitter(fileSystem, outputPath, context);
    } else {
      return createDefaultCommitter(outputPath, context);
    }
  }

  /**
   * Dynamically create a task committer.
   * @param fileSystem destination FS.
   * @param outputPath final output path for work
   * @param context job context
   * @return a committer
   * @throws IOException instantiation failure
   */
  @Override
  public PathOutputCommitter createTaskCommitter(S3AFileSystem fileSystem,
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    Abstract3GuardCommitterFactory factory = chooseCommitter(fileSystem,
        outputPath, context);
        if (factory != null) {
      return factory.createTaskCommitter(fileSystem, outputPath, context);
    } else {
      return createDefaultCommitter(outputPath, context);
    }
  }

  /**
   * Choose a committer from the FS configuration.
   * @param fileSystem FS
   * @param outputPath destination path
   * @param context task/job context.
   * @return A s3guard committer if chosen, or "null" for the classic value
   * @throws IOException on a failure to identify the committer
   */
  private Abstract3GuardCommitterFactory chooseCommitter(
      S3AFileSystem fileSystem,
      Path outputPath,
      JobContext context) throws IOException {
    Abstract3GuardCommitterFactory factory;

    // the FS conf will have had its per-bucket values resolved, unlike
    // job/task configurations.
    Configuration conf = fileSystem.getConf();
    String name = conf.getTrimmed(FS_S3A_COMMITTER_NAME,
        COMMITTER_NAME_DEFAULT);
    switch (name) {
    case COMMITTER_NAME_FILE:
      factory = null;
      break;
    case COMMITTER_NAME_DIRECTORY:
      factory = new DirectoryStagingCommitterFactory();
      break;
    case COMMITTER_NAME_PARTITION:
      factory = new PartitonedStagingCommitterFactory();
      break;
    case COMMITTER_NAME_MAGIC:
      factory = new MagicS3GuardCommitterFactory();
      break;
    default:
      throw new IOException("Unknown committer: \"" + name + "\"");
    }
    return factory;
  }
}
