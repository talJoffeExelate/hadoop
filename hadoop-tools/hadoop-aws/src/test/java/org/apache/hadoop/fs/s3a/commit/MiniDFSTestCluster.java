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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.service.AbstractService;

/**
 * MiniDFS Cluster, encapsulated for use in different test suites.
 */
public class MiniDFSTestCluster extends AbstractService {

  public MiniDFSTestCluster() {
    super("MiniDFSTestCluster");
  }

  private MiniDFSCluster cluster = null;
  private FileSystem clusterFS = null;
  private LocalFileSystem localFS = null;

  protected FileSystem getDFS() {
    return clusterFS;
  }

  protected FileSystem getFS() {
    return localFS;
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    conf.setBoolean("dfs.webhdfs.enabled", false);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    // if this fails with "directory is already locked" set umask to 0022
    cluster = new MiniDFSCluster(conf, 1, true, null);
    //cluster = new MiniDFSCluster.Builder(new Configuration()).build();
    clusterFS = cluster.getFileSystem();
    conf = new JobConf(clusterFS.getConf());
    localFS = FileSystem.getLocal(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    clusterFS = null;
    localFS = null;
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  public FileSystem getClusterFS() {
    return clusterFS;
  }

  public LocalFileSystem getLocalFS() {
    return localFS;
  }
}
