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

import com.amazonaws.services.s3.model.MultipartUpload;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

/**
 * Relays FS calls to the mocked FS, allows for some extra logging with
 * stack traces to be included. Why? Useful for tracking
 * down why there are extra calls to a method than a test would expect:
 * changes in implementation details often trigger such false-positive
 * test failures.
 */
public class MockS3AFileSystem extends S3AFileSystem {
  public static final String BUCKET = "bucket-name";
  public static final URI FS_URI = URI.create("s3a://" + BUCKET + "/");
  protected static final Logger LOG =
      LoggerFactory.getLogger(MockS3AFileSystem.class);

  private S3AFileSystem mock = null;
  public static final int LOG_NONE = 0;
  public static final int LOG_NAME = 1;
  public static final int LOG_STACK = 2;
  private int logEvents = LOG_NAME;

  public MockS3AFileSystem() {
  }

  public int getLogEvents() {
    return logEvents;
  }

  public void setLogEvents(int logEvents) {
    this.logEvents = logEvents;
  }

  private void event(String format, Object... args) {
    Throwable ex = null;
    switch (logEvents) {
    case LOG_STACK:
      ex = new Exception("stack");
        /* fall through */
    case LOG_NAME:
      String s = String.format(format, args);
      LOG.info(s, ex);
      break;
    case LOG_NONE:
    default:
      //nothing
    }
  }

  @Override
  public String getScheme() {
    return FS_URI.getScheme();
  }

  @Override
  public URI getUri() {
    return FS_URI;
  }

  @Override
  public String getBucket() {
    return BUCKET;
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("s3a://" + BUCKET + "/work");
  }

  public void setMock(S3AFileSystem mock) {
    this.mock = mock;
  }

  @Override
  public void initialize(URI name, Configuration originalConf)
      throws IOException {
//    mock.initialize(name, originalConf);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    event("exists(%s)", f);
    return mock.exists(f);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    event("open(%s)", f);
    return mock.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    event("create(%s)", f);
    return mock.create(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f,
      int bufferSize,
      Progressable progress) throws IOException {
    return mock.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    event("rename(%s, %s)", src, dst);
    return mock.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    event("delete(%s, %s)", f, recursive);
    return mock.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f)
      throws FileNotFoundException, IOException {
    event("listStatus(%s)", f);
    return mock.listStatus(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws IOException {
    event("listFiles(%s, %s)", f, recursive);
    return new EmptyIterator();
//    return mock.listFiles(f, recursive);
  }

  @Override
  public List<MultipartUpload> listMultipartUploads(String prefix)
      throws IOException {
    event("listMultipartUploads(%s)", prefix);
    return Collections.emptyList();
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    mock.setWorkingDirectory(newDir);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    event("mkdirs(%s)", f);
    return mock.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    event("getFileStatus(%s)", f);
    return mock.getFileStatus(f);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return mock.getDefaultBlockSize(f);
  }

  @Override
  public long getDefaultBlockSize() {
    return mock.getDefaultBlockSize();
  }

  private static class EmptyIterator implements
      RemoteIterator<LocatedFileStatus> {
    @Override
    public boolean hasNext() throws IOException {
      return false;
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      return null;
    }
  }
}
