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

package org.apache.hadoop.fs.azure.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.assumeScaleTestsEnabled;

/**
 * Contract test suite covering WASB integration with DistCp.
 */
public class ITestAzureNativeContractDistCp extends AbstractContractDistCpTest {

  @Override
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  @Override
  protected NativeAzureFileSystemContract createContract(Configuration conf) {
    return new NativeAzureFileSystemContract(conf);
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    assumeScaleTestsEnabled(getContract().getConf());
  }
}
