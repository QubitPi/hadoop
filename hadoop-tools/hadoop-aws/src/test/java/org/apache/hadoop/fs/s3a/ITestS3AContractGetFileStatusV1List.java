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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.jupiter.api.AfterEach;

import static org.apache.hadoop.fs.s3a.Constants.LIST_VERSION;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_LIST_V1_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfS3ExpressBucket;

/**
 * S3A contract tests for getFileStatus, using the v1 List Objects API.
 */
public class ITestS3AContractGetFileStatusV1List
    extends AbstractContractGetFileStatusTest {


  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
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
    disableFilesystemCaching(conf);
    skipIfNotEnabled(conf, KEY_LIST_V1_ENABLED,
        "Skipping V1 listing tests");
    skipIfS3ExpressBucket(conf);
    conf.setInt(Constants.MAX_PAGING_KEYS, 2);

    // Use v1 List Objects API
    conf.setInt(LIST_VERSION, 1);
    return conf;
  }
}
