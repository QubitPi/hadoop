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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test verifies DataNode command line processing.
 */
public class TestDatanodeStartupOptions {

  private Configuration conf = null;

  /**
   * Process the given arg list as command line arguments to the DataNode
   * to make sure we get the expected result. If the expected result is
   * success then further validate that the parsed startup option is the
   * same as what was expected.
   *
   * @param expectSuccess
   * @param expectedOption
   * @param conf
   * @param arg
   */
  private static void checkExpected(boolean expectSuccess,
                                    StartupOption expectedOption,
                                    Configuration conf,
                                    String ... arg) {

    String[] args = new String[arg.length];
    int i = 0;
    for (String currentArg : arg) {
      args[i++] = currentArg;
    }

    boolean returnValue = DataNode.parseArguments(args, conf);
    StartupOption option = DataNode.getStartupOption(conf);
    assertThat(returnValue).isEqualTo(expectSuccess);

    if (expectSuccess) {
      assertThat(option).isEqualTo(expectedOption);
    }
  }

  /**
   * Reinitialize configuration before every test since DN stores the
   * parsed StartupOption in the configuration.
   */
  @BeforeEach
  public void initConfiguration() {
    conf = new HdfsConfiguration();
  }

  /**
   * A few options that should all parse successfully.
   */
  @Test
  @Timeout(value = 60)
  public void testStartupSuccess() {
    checkExpected(true, StartupOption.REGULAR, conf);
    checkExpected(true, StartupOption.REGULAR, conf, "-regular");
    checkExpected(true, StartupOption.REGULAR, conf, "-REGULAR");
    checkExpected(true, StartupOption.ROLLBACK, conf, "-rollback");
  }

  /**
   * A few options that should all fail to parse.
   */
  @Test
  @Timeout(value = 60)
  public void testStartupFailure() {
    checkExpected(false, StartupOption.REGULAR, conf, "unknownoption");
    checkExpected(false, StartupOption.REGULAR, conf, "-regular -rollback");
  }
}
