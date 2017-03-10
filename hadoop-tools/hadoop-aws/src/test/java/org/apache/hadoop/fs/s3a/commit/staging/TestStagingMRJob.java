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

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.LoggingTextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;

public class TestStagingMRJob extends StagingTestBase.MiniDFSTest {

  private static MiniMRYarnCluster MR_CLUSTER = null;

  @BeforeClass
  public static void setupMiniMRCluster() {
    MR_CLUSTER = new MiniMRYarnCluster(
        "test-s3-multipart-output-committer", 2);
    MR_CLUSTER.init(getConfiguration());
    MR_CLUSTER.start();
  }

  @AfterClass
  public static void stopMiniMRCluster() {
    if (MR_CLUSTER != null) {
      MR_CLUSTER.stop();
    }
    MR_CLUSTER = null;
  }

  public static class S3TextOutputFormat<K, V> extends LoggingTextOutputFormat<K, V> {
    private MockedStagingCommitter committer = null;

    @Override
    public synchronized OutputCommitter getOutputCommitter(
        TaskAttemptContext context) throws IOException {
      if (committer == null) {
        committer = new MockedStagingCommitter(
            getOutputPath(context), context);
      }
      return committer;
    }
  }

  public static class M extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testMRJob() throws Exception {
    FileSystem mockS3 = mock(FileSystem.class);
    FileSystem s3 = S3_OUTPUT_PATH.getFileSystem(getConfiguration());

    String commitUUID = UUID.randomUUID().toString();

    int numFiles = 3;
    Set<String> expectedFiles = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      File file = temp.newFile(String.valueOf(i) + ".text");
      try (FileOutputStream out = new FileOutputStream(file)) {
        out.write(("file " + i).getBytes(StandardCharsets.UTF_8));
      }
      expectedFiles.add(new Path(
          S3_OUTPUT_PATH, "part-m-0000" + i + "-" + commitUUID).toString());
    }

    Job mrJob = Job.getInstance(MR_CLUSTER.getConfig(), "test-committer-job");
    Configuration conf = mrJob.getConfiguration();

    mrJob.setOutputFormatClass(S3TextOutputFormat.class);
    S3TextOutputFormat.setOutputPath(mrJob, S3_OUTPUT_PATH);

    File mockResultsFile = temp.newFile("committer.bin");
    mockResultsFile.delete();
    String committerPath = "file:" + mockResultsFile;
    conf.set("mock-results-file", committerPath);
    conf.set(StagingCommitterConstants.UPLOAD_UUID, commitUUID);

    mrJob.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(mrJob,
        new Path(temp.getRoot().toURI()));

    mrJob.setMapperClass(M.class);
    mrJob.setNumReduceTasks(0);

    mrJob.submit();
    assertTrue("MR job should succeed", mrJob.waitForCompletion(true));

    StagingTestBase.ClientResults results;
    try (ObjectInputStream in = new ObjectInputStream(
        FileSystem.getLocal(conf).open(new Path(committerPath)))) {
      results = (StagingTestBase.ClientResults) in.readObject();
    }

    assertEquals("Should not delete files",
        0, results.deletes.size());

    assertEquals("Should not abort commits",
        0, results.aborts.size());

    assertEquals("Should commit task output files",
        numFiles, results.commits.size());

    Set<String> actualFiles = Sets.newHashSet();
    for (CompleteMultipartUploadRequest commit : results.commits) {
      actualFiles.add("s3a://" + commit.getBucketName() + "/" + commit.getKey());
    }

    assertEquals("Should commit the correct file paths",
        expectedFiles, actualFiles);
  }

}
