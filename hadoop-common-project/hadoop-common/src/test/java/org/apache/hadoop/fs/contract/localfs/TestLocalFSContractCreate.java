/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.localfs;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestLocalFSContractCreate extends AbstractContractCreateTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LocalFSContract(conf);
  }

  @Test
  public void testSyncablePassthroughIfChecksumDisabled() throws Throwable {
    describe("Create an instance of the local fs, disable the checksum"
        + " and verify that Syncable now works");
    LocalFileSystem fs = (LocalFileSystem) getFileSystem();
    try (LocalFileSystem lfs = new LocalFileSystem(
        fs.getRawFileSystem())) {
      // disable checksumming output
      lfs.setWriteChecksum(false);
      // now the filesystem supports Sync with immediate update of file status
      validateSyncableSemantics(lfs, true, true, true);
    }
  }
}
