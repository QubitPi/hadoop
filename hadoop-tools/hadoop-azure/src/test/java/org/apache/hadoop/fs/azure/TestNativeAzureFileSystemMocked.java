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

package org.apache.hadoop.fs.azure;

import org.junit.jupiter.api.Disabled;
import java.io.IOException;

/**
 * Run {@link NativeAzureFileSystemBaseTest} tests against a mocked store,
 * skipping tests of unsupported features
 */
public class TestNativeAzureFileSystemMocked extends
    NativeAzureFileSystemBaseTest {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.createMock();
  }

  // Ignore the following tests because taking a lease requires a real
  // (not mock) file system store. These tests don't work on the mock.
  @Override
  @Disabled
  public void testLeaseAsDistributedLock() {
  }

  @Override
  @Disabled
  public void testSelfRenewingLease() {
  }

  @Override
  @Disabled
  public void testRedoFolderRenameAll() {
  }

  @Override
  @Disabled
  public void testCreateNonRecursive() {
  }

  @Override
  @Disabled
  public void testSelfRenewingLeaseFileDelete() {
  }

  @Override
  @Disabled
  public void testRenameRedoFolderAlreadyDone() throws IOException{
  }
}
