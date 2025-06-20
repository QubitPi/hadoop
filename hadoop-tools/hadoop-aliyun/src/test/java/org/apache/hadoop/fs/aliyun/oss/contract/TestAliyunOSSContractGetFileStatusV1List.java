/**
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
package org.apache.hadoop.fs.aliyun.oss.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.jupiter.api.AfterEach;

/**
 * Test getFileStatus and related listing operations,
 * using the v1 List Objects API.
 */
public class TestAliyunOSSContractGetFileStatusV1List
    extends AbstractContractGetFileStatusTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new AliyunOSSContract(conf);
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    getLogger().info("FS details {}", getFileSystem());
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    AliyunOSSTestUtils.disableFilesystemCaching(conf);
    conf.setInt(Constants.MAX_PAGING_KEYS_KEY, 2);
    // Use v1 List Objects API
    conf.setInt(Constants.LIST_VERSION, 1);
    return conf;
  }
}
