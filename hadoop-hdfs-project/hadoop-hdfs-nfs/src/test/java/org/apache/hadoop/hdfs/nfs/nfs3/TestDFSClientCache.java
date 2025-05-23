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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestDFSClientCache {
  @AfterEach
  public void cleanup() {
    UserGroupInformation.reset();
  }

  @Test
  public void testEviction() throws IOException {
    NfsConfiguration conf = new NfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost");

    // Only one entry will be in the cache
    final int MAX_CACHE_SIZE = 1;

    DFSClientCache cache = new DFSClientCache(conf, MAX_CACHE_SIZE);

    int namenodeId = Nfs3Utils.getNamenodeId(conf);
    DFSClient c1 = cache.getDfsClient("test1", namenodeId);
    assertTrue(cache.getDfsClient("test1", namenodeId)
        .toString().contains("ugi=test1"));
    assertEquals(c1, cache.getDfsClient("test1", namenodeId));
    assertFalse(isDfsClientClose(c1));

    cache.getDfsClient("test2", namenodeId);
    assertTrue(isDfsClientClose(c1));
    assertTrue(cache.getClientCache().size() <= MAX_CACHE_SIZE,
        "cache size should be the max size or less");
  }

  @Test
  public void testGetUserGroupInformationSecure() throws IOException {
    String userName = "user1";
    String currentUser = "test-user";


    NfsConfiguration conf = new NfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost");
    UserGroupInformation currentUserUgi
            = UserGroupInformation.createRemoteUser(currentUser);
    currentUserUgi.setAuthenticationMethod(KERBEROS);
    UserGroupInformation.setLoginUser(currentUserUgi);

    DFSClientCache cache = new DFSClientCache(conf);
    UserGroupInformation ugiResult
            = cache.getUserGroupInformation(userName, currentUserUgi);

    assertThat(ugiResult.getUserName()).isEqualTo(userName);
    assertThat(ugiResult.getRealUser()).isEqualTo(currentUserUgi);
    assertThat(ugiResult.getAuthenticationMethod()).isEqualTo(
        UserGroupInformation.AuthenticationMethod.PROXY);
  }

  @Test
  public void testGetUserGroupInformation() throws IOException {
    String userName = "user1";
    String currentUser = "currentUser";

    UserGroupInformation currentUserUgi = UserGroupInformation
            .createUserForTesting(currentUser, new String[0]);
    NfsConfiguration conf = new NfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost");
    DFSClientCache cache = new DFSClientCache(conf);
    UserGroupInformation ugiResult
            = cache.getUserGroupInformation(userName, currentUserUgi);

    assertThat(ugiResult.getUserName()).isEqualTo(userName);
    assertThat(ugiResult.getRealUser()).isEqualTo(currentUserUgi);
    assertThat(ugiResult.getAuthenticationMethod()).isEqualTo(
        UserGroupInformation.AuthenticationMethod.PROXY);
  }

  private static boolean isDfsClientClose(DFSClient c) {
    try {
      c.exists("");
    } catch (IOException e) {
      return e.getMessage().equals("Filesystem closed");
    }
    return false;
  }
}
