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

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.util.List;

/**
 * Multipart put tracker.
 * Base class does nothing except declare that any
 * MPU must complete in the close() operation.
 *
 */
@InterfaceAudience.Private
public class DefaultPutTracker {

  private final String destKey;

  /**
   * Instantiate.
   * @param destKey destination key
   */
  public DefaultPutTracker(String destKey) {
    this.destKey = destKey;
  }

  /**
   * Startup event.
   * @return true if the multipart should start immediately.
   * @throws IOException any IO problem.
   */
  public boolean inited() throws IOException {
    return false;
  }

  /**
   * Callback when the MPU is about to complete.
   * @param uploadId Upload ID
   * @param parts list of parts
   * @param bytesWritten bytes written
   * @return true if the commit is to be execute4d
   * @throws IOException any IO problem.
   * @throws IllegalStateException if there's a problem with the validity
   * of the request.
   */
  public boolean aboutToComplete(String uploadId,
      List<PartETag> parts,
      long bytesWritten)
      throws IOException {

    return true;
  }

  /**
   * get the destination key. The default implementation returns the
   * key passed in: there is no adjustment of the destination.
   * @return the destination to use in PUT requests.
   */
  public String getDestKey() {
    return destKey;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MultipartPutTracker{");
    sb.append("destKey='").append(destKey).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
