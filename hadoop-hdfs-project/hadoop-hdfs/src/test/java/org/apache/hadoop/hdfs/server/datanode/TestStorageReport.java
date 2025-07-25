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

import java.io.IOException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStorageReport {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestStorageReport.class);

  private static final short REPL_FACTOR = 1;
  private static final StorageType storageType = StorageType.SSD; // pick non-default.

  private static Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  static String bpid;

  @BeforeEach
  public void startUpCluster() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPL_FACTOR)
        .storageTypes(new StorageType[] { storageType, storageType } )
        .build();
    fs = cluster.getFileSystem();
    bpid = cluster.getNamesystem().getBlockPoolId();
  }

  @AfterEach
  public void shutDownCluster() throws IOException {
    if (cluster != null) {
      fs.close();
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Ensure that storage type and storage state are propagated
   * in Storage Reports.
   */
  @Test
  public void testStorageReportHasStorageTypeAndState() throws IOException {

    // Make sure we are not testing with the default type, that would not
    // be a very good test.
    assertNotSame(storageType, StorageType.DEFAULT);
    NameNode nn = cluster.getNameNode();
    DataNode dn = cluster.getDataNodes().get(0);

    // Insert a spy object for the NN RPC.
    DatanodeProtocolClientSideTranslatorPB nnSpy =
        InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);

    // Trigger a heartbeat so there is an interaction with the spy
    // object.
    DataNodeTestUtils.triggerHeartbeat(dn);

    // Verify that the callback passed in the expected parameters.
    ArgumentCaptor<StorageReport[]> captor =
        ArgumentCaptor.forClass(StorageReport[].class);

    Mockito.verify(nnSpy).sendHeartbeat(
        any(DatanodeRegistration.class),
        captor.capture(),
        anyLong(), anyLong(), anyInt(), anyInt(), anyInt(),
        any(), Mockito.anyBoolean(),
        Mockito.any(SlowPeerReports.class),
        Mockito.any(SlowDiskReports.class));

    StorageReport[] reports = captor.getValue();

    for (StorageReport report: reports) {
      assertThat(report.getStorage().getStorageType()).isEqualTo(storageType);
      assertThat(report.getStorage().getState()).isEqualTo(DatanodeStorage.State.NORMAL);
    }
  }
}
