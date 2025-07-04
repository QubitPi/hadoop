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

package org.apache.hadoop.fs.azurebfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.assumeScaleTestsEnabled;

/**
 * Contract test for distCp operation.
 */
public class ITestAbfsFileSystemContractDistCp extends AbstractContractDistCpTest {
  private final ABFSContractTestBinding binding;

  @Override
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  public ITestAbfsFileSystemContractDistCp() throws Exception {
    binding = new ABFSContractTestBinding();
    assumeTrue(binding.getAuthType() != AuthType.OAuth);
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
    assumeScaleTestsEnabled(binding.getRawConfiguration());
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbfsFileSystemContract createContract(Configuration conf) {
    return new AbfsFileSystemContract(conf, false);
  }
}
