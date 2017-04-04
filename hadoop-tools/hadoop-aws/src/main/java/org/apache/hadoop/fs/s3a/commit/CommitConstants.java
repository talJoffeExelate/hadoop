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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * Constants for working with committers.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class CommitConstants {

  private CommitConstants() {
  }

  /**
   * Flag to indicate whether support for the Magic committer is enabled
   * in the filesystem.
   * Value: {@value}.
   */
  public static final String MAGIC_COMMITTER_ENABLED
      = "fs.s3a.committer.magic.enabled";

  /**
   * Is the committer enabled by default? No.
   */
  public static final boolean DEFAULT_MAGIC_COMMITTER_ENABLED = false;

  /**
   * This is the "Pending" directory of the FileOutputCommitter;
   * data written here is, in that algorithm, renamed into place.
   * Value: {@value}.
   */
  public static final String PENDING_DIR_NAME = "_temporary";

  /**
   * Temp data which is not auto-committed: {@value}.
   * Uses a different name from normal just to make clear it is different.
   */
  public static final String TEMP_DATA_PATH = "__temp-data";


  /**
   * Flag to trigger creation of a marker file on job completion.
   */
  public static final String CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER
      = FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;

  /**
   * Marker file to create on success.
   */
  public static final String SUCCESS_FILE_NAME = "_SUCCESS";

  /** Default job marker option: {@value}. */
  public static final boolean DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER = true;

  /**
   * Directory committer: {@value}.
   */
  public static final String DIRECTORY_COMMITTER_FACTORY =
      DirectoryStagingCommitterFactory.NAME;

  /**
   * Partitioned committer: {@value}.
   */
  public static final String PARTITION_COMMITTER_FACTORY =
      PartitonedStagingCommitterFactory.NAME;

  /**
   * Dynamic committer: {@value}.
   */
  public static final String DYNAMIC_COMMITTER_FACTORY =
      DynamicCommitterFactory.NAME;

  /**
   * Magic committer: {@value}.
   */
  public static final String MAGIC_COMMITTER_FACTORY =
      MagicS3GuardCommitterFactory.NAME;

  /**
   * Property to identify the S3a committer when the dynamic committer is used:
   * {@value}.
   */
  public static final String FS_S3A_COMMITTER_NAME =
      "fs.s3a.committer.name";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * classic/file output committer: {@value}.
   */
  public static final String COMMITTER_NAME_FILE = "file";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * magic output committer: {@value}.
   */
  public static final String COMMITTER_NAME_MAGIC = "magic";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * directory output committer: {@value}.
   */
  public static final String COMMITTER_NAME_DIRECTORY = "directory";

  /**
   * Option for {@link #FS_S3A_COMMITTER_NAME}:
   * partition output committer: {@value}.
   */
  public static final String COMMITTER_NAME_PARTITION = "partition";

  /**
   * Default option for {@link #FS_S3A_COMMITTER_NAME}: {@value}.
   * This is <i>initially the classic one; it may change in future.</i>
   */
  public static final String COMMITTER_NAME_DEFAULT
      = COMMITTER_NAME_FILE;

  /**
   * Option for final files to have uniqueness through uuid or attempt info:
   * {@value}.
   * Can be used for conflict resolution, but it also means that the filenames
   * of output may not always be predictable in advance.
   */
  public static final String FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES =
      "fs.s3a.committer.staging.unique-filenames";
  /**
   * Default value for {@link #FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES}:
   * {@value}.
   */
  public static final boolean DEFAULT_COMMITTER_UNIQUE_FILENAMES = false;

  /**
   * A unique identifier to use for this work: {@value}.
   */
  public static final String FS_S3A_COMMITTER_STAGING_UUID =
      "fs.s3a.committer.staging.uuid";

  /**
   * Conflict mode resolution policy: {@value}.
   */
  public static final String FS_S3A_COMMITTER_STAGING_CONFLICT_MODE =
      "fs.s3a.committer.staging.conflict-mode";

  /** Conflict mode: {@value}. */
  public static final String CONFLICT_MODE_FAIL = "fail";

  /** Default conflict mode: {@value}. */
  public static final String DEFAULT_CONFLICT_MODE = CONFLICT_MODE_FAIL;

  /** Conflict mode: {@value}. */
  public static final String CONFLICT_MODE_APPEND = "append";

  /** Conflict mode: {@value}. */
  public static final String CONFLICT_MODE_REPLACE = "replace";

  /**
   * Number of threads in staging committers for parallel operations
   * (upload, commit, abort): {@value}.
   */
  public static final String FS_S3A_COMMITTER_STAGING_THREADS =
      "fs.s3a.committer.staging.threads";
  /**
   * Default value for {@link #FS_S3A_COMMITTER_STAGING_THREADS}: {@value}.
   */
  public static final int DEFAULT_STAGING_COMMITTER_THREADS = 8;

  /**
   * Path for pending data in the cluster FS: {@value}.
   * TODO: Support this somehow (i.e. not just /tmp)
   */
  public static final String FS_S3A_COMMITTER_STAGING_PENDING_PATH =
      "fs.s3a.committer.staging.pending.path";

}
