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

import org.apache.hadoop.mapreduce.JobContext;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Dynamically create the output committer based on the filesystem type.
 * For S3A output, uses the {@link S3GuardCommitter}; for other filesystems
 * use the classic committer.
 */
public class S3GuardCommitterFactory extends Abstract3GuardCommitterFactory {
  /**
   * Name of this class: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.S3GuardCommitterFactory";

  protected AbstractS3GuardCommitter createTaskCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    return new S3GuardCommitter(outputPath, context);
  }

  protected AbstractS3GuardCommitter createJobCommitter(Path outputPath,
      JobContext context) throws IOException {
    return new S3GuardCommitter(outputPath, context);
  }
}
