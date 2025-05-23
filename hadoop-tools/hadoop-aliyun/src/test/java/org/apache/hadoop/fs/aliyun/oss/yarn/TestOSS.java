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

package org.apache.hadoop.fs.aliyun.oss.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OSS tests through the {@link FileContext} API.
 */
public class TestOSS {
  private FileContext fc;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fc = AliyunOSSTestUtils.createTestFileContext(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fc != null) {
      fc.delete(getTestPath(), true);
    }
  }

  protected Path getTestPath() {
    return new Path(AliyunOSSTestUtils.generateUniqueTestPath());
  }

  @Test
  public void testOSSStatus() throws Exception {
    FsStatus fsStatus = fc.getFsStatus(null);
    assertNotNull(fsStatus);
    assertTrue(
        fsStatus.getUsed() >= 0, "Used capacity should be positive: " + fsStatus.getUsed());
    assertTrue(
        fsStatus.getRemaining() >= 0, "Remaining capacity should be positive: " + fsStatus
        .getRemaining());
    assertTrue(
        fsStatus.getCapacity() >= 0, "Capacity should be positive: " + fsStatus.getCapacity());
  }

  @Test
  @Timeout(90)
  public void testOSSCreateFileInSubDir() throws Exception {
    Path dirPath = getTestPath();
    fc.mkdir(dirPath, FileContext.DIR_DEFAULT_PERM, true);
    Path filePath = new Path(dirPath, "file");
    try (FSDataOutputStream file = fc.create(filePath, EnumSet.of(CreateFlag
        .CREATE))) {
      file.write(666);
    }
  }
}
