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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Used to test the functionality of {@link RouterAsyncUserProtocol}.
 */
public class TestRouterAsyncUserProtocol extends RouterAsyncProtocolTestBase {

  private RouterAsyncUserProtocol asyncUserProtocol;

  @BeforeEach
  public void setup() throws Exception {
    asyncUserProtocol = new RouterAsyncUserProtocol(getRouterAsyncRpcServer());
  }

  @Test
  public void testgetGroupsForUser() throws Exception {
    String[] group = new String[] {"bar", "group2"};
    UserGroupInformation.createUserForTesting("user",
        new String[] {"bar", "group2"});
    asyncUserProtocol.getGroupsForUser("user");
    String[] result = syncReturn(String[].class);
    assertArrayEquals(group, result);
  }
}
