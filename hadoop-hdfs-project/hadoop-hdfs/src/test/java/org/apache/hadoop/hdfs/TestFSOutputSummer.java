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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

/**
 * This class tests if FSOutputSummer works correctly.
 */
public class TestFSOutputSummer {
  private static final long seed = 0xDEADBEEFL;
  private static final int BYTES_PER_CHECKSUM = 10;
  private static final int BLOCK_SIZE = 2*BYTES_PER_CHECKSUM;
  private static final int HALF_CHUNK_SIZE = BYTES_PER_CHECKSUM/2;
  private static final int FILE_SIZE = 2*BLOCK_SIZE-1;
  private static final short NUM_OF_DATANODES = 2;
  private final byte[] expected = new byte[FILE_SIZE];
  private final byte[] actual = new byte[FILE_SIZE];
  private FileSystem fileSys;

  /* create a file, write all data at once */
  private void writeFile1(Path name) throws Exception {
    FSDataOutputStream stm = fileSys.create(name, true, 
               fileSys.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 4096),
               NUM_OF_DATANODES, BLOCK_SIZE);
    stm.write(expected);
    stm.close();
    checkFile(name);
    cleanupFile(name);
  }
  
  /* create a file, write data chunk by chunk */
  private void writeFile2(Path name) throws Exception {
    FSDataOutputStream stm = fileSys.create(name, true, 
               fileSys.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 4096),
               NUM_OF_DATANODES, BLOCK_SIZE);
    int i=0;
    for( ;i<FILE_SIZE-BYTES_PER_CHECKSUM; i+=BYTES_PER_CHECKSUM) {
      stm.write(expected, i, BYTES_PER_CHECKSUM);
    }
    stm.write(expected, i, FILE_SIZE-3*BYTES_PER_CHECKSUM);
    stm.close();
    checkFile(name);
    cleanupFile(name);
  }
  
  /* create a file, write data with variable amount of data */
  private void writeFile3(Path name) throws Exception {
    FSDataOutputStream stm = fileSys.create(name, true, 
        fileSys.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 4096),
        NUM_OF_DATANODES, BLOCK_SIZE);
    stm.write(expected, 0, HALF_CHUNK_SIZE);
    stm.write(expected, HALF_CHUNK_SIZE, BYTES_PER_CHECKSUM+2);
    stm.write(expected, HALF_CHUNK_SIZE+BYTES_PER_CHECKSUM+2, 2);
    stm.write(expected, HALF_CHUNK_SIZE+BYTES_PER_CHECKSUM+4, HALF_CHUNK_SIZE);
    stm.write(expected, BLOCK_SIZE+4, BYTES_PER_CHECKSUM-4);
    stm.write(expected, BLOCK_SIZE+BYTES_PER_CHECKSUM, 
        FILE_SIZE-3*BYTES_PER_CHECKSUM);
    stm.close();
    checkFile(name);
    cleanupFile(name);
  }
  private void checkAndEraseData(byte[] actual, int from, byte[] expected,
      String message) throws Exception {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(actual[idx], expected[from + idx],
          message + " byte " + (from + idx)
          + " differs. expected " + expected[from + idx] + " actual " + actual[idx]);
      actual[idx] = 0;
    }
  }
  
  private void checkFile(Path name) throws Exception {
    FSDataInputStream stm = fileSys.open(name);
    // do a sanity check. Read the file
    stm.readFully(0, actual);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    stm.close();
    // do a sanity check. Get the file checksum
    fileSys.getFileChecksum(name);
  }

  private void cleanupFile(Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Test write operation for output stream in DFS.
   */
  @Test
  public void testFSOutputSummer() throws Exception {
    doTestFSOutputSummer("CRC32");
    doTestFSOutputSummer("CRC32C");
    doTestFSOutputSummer("NULL");
  }
  
  private void doTestFSOutputSummer(String checksumType) throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, checksumType);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(NUM_OF_DATANODES)
                                               .build();
    fileSys = cluster.getFileSystem();
    try {
      Path file = new Path("try.dat");
      Random rand = new Random(seed);
      rand.nextBytes(expected);
      writeFile1(file);
      writeFile2(file);
      writeFile3(file);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  @Test
  public void TestDFSCheckSumType() throws Exception{
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "NULL");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(NUM_OF_DATANODES)
                                               .build();
    fileSys = cluster.getFileSystem();
    try {
      Path file = new Path("try.dat");
      Random rand = new Random(seed);
      rand.nextBytes(expected);
      writeFile1(file);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
