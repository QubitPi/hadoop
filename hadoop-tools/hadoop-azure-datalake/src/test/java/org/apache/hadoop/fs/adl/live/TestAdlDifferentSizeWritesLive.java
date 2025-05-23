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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.fs.adl.AdlConfKeys.WRITE_BUFFER_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verify data integrity with different data sizes with buffer size.
 */
public class TestAdlDifferentSizeWritesLive {
  private static Random rand = new Random();
  private int totalSize;
  private int chunkSize;

  public void initTestAdlDifferentSizeWritesLive(int pTotalSize, int pChunkSize) {
    this.totalSize = pTotalSize;
    this.chunkSize = pChunkSize;
  }

  public static byte[] getRandomByteArrayData(int size) {
    byte[] b = new byte[size];
    rand.nextBytes(b);
    return b;
  }

  public static Collection testDataForIntegrityTest() {
    return Arrays.asList(
        new Object[][] {{4 * 1024, 1 * 1024}, {4 * 1024, 7 * 1024},
            {4 * 1024, 10}, {2 * 1024, 10}, {1 * 1024, 10}, {100, 1},
            {4 * 1024, 1 * 1024}, {7 * 1024, 2 * 1024}, {9 * 1024, 2 * 1024},
            {10 * 1024, 3 * 1024}, {10 * 1024, 1 * 1024},
            {10 * 1024, 8 * 1024}});
  }

  @BeforeAll
  public static void cleanUpParent() throws IOException, URISyntaxException {
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      Path path = new Path("/test/dataIntegrityCheck/");
      FileSystem fs = AdlStorageConfiguration.createStorageConnector();
      fs.delete(path, true);
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }

  @MethodSource("testDataForIntegrityTest")
  @ParameterizedTest(name = "{index}: Data Size [{0}] ; Chunk Size [{1}]")
  public void testDataIntegrity(int pTotalSize, int pChunkSize) throws IOException {
    initTestAdlDifferentSizeWritesLive(pTotalSize, pChunkSize);
    Path path = new Path(
        "/test/dataIntegrityCheck/" + UUID.randomUUID().toString());
    FileSystem fs = null;
    AdlStorageConfiguration.getConfiguration()
        .setInt(WRITE_BUFFER_SIZE_KEY, 4 * 1024);
    try {
      fs = AdlStorageConfiguration.createStorageConnector();
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Can not initialize ADL FileSystem. "
          + "Please check test.fs.adl.name property.", e);
    }
    byte[] expectedData = getRandomByteArrayData(totalSize);

    FSDataOutputStream out = fs.create(path, true);
    int iteration = totalSize / chunkSize;
    int reminderIteration = totalSize % chunkSize;
    int offset = 0;
    for (int i = 0; i < iteration; ++i) {
      out.write(expectedData, offset, chunkSize);
      offset += chunkSize;
    }

    out.write(expectedData, offset, reminderIteration);
    out.close();

    byte[] actualData = new byte[totalSize];
    FSDataInputStream in = fs.open(path);
    in.readFully(0, actualData);
    in.close();
    assertArrayEquals(expectedData, actualData);
    assertTrue(fs.delete(path, true));
  }
}
