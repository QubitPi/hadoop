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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** 
 * Tests if DataNode process exits if all Block Pool services exit. 
 */
public class TestDataNodeExit {
  private static final long WAIT_TIME_IN_MILLIS = 10;
  Configuration conf;
  MiniDFSCluster cluster = null;
  
  @BeforeEach
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 100);
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
      .build();
    for (int i = 0; i < 3; i++) {
      cluster.waitActive(i);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  private void stopBPServiceThreads(int numStopThreads, DataNode dn)
      throws Exception {
    List<BPOfferService> bpoList = dn.getAllBpOs();
    int expected = dn.getBpOsCount() - numStopThreads;
    int index = numStopThreads - 1;
    while (index >= 0) {
      bpoList.get(index--).stop();
    }
    int iterations = 3000; // Total 30 seconds MAX wait time
    while(dn.getBpOsCount() != expected && iterations > 0) {
      Thread.sleep(WAIT_TIME_IN_MILLIS);
      iterations--;
    }
    assertEquals(expected, dn.getBpOsCount(), "Mismatch in number of BPServices running");
  }

  @Test
  public void testBPServiceState() {
    List<DataNode> dataNodes = cluster.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      List<BPOfferService> bposList = dataNode.getAllBpOs();
      for (BPOfferService bpOfferService : bposList) {
        assertTrue(bpOfferService.isAlive());
      }
    }
  }

  /**
   * Test BPService Thread Exit
   */
  @Test
  public void testBPServiceExit() throws Exception {
    DataNode dn = cluster.getDataNodes().get(0);
    stopBPServiceThreads(1, dn);
    assertTrue(dn.isDatanodeUp(), "DataNode should not exit");
    stopBPServiceThreads(2, dn);
    assertFalse(dn.isDatanodeUp(), "DataNode should exit");
  }

  @Test
  public void testSendOOBToPeers() throws Exception {
    DataNode dn = cluster.getDataNodes().get(0);
    DataXceiverServer spyXserver = Mockito.spy(dn.getXferServer());
    NullPointerException npe = new NullPointerException();
    Mockito.doThrow(npe).when(spyXserver).sendOOBToPeers();
    dn.xserver = spyXserver;
    try {
      dn.shutdown();
    } catch (Exception e) {
      fail("DataNode shutdown should not have thrown exception " + e);
    }
  }
}
