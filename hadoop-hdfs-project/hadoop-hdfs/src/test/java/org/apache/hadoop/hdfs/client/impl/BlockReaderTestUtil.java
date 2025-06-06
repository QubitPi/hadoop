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

package org.apache.hadoop.hdfs.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * A helper class to setup the cluster, and get to BlockReader and DataNode for a block.
 */
public class BlockReaderTestUtil {
  /**
   * Returns true if we should run tests that generate large files (> 1GB)
   */
  static public boolean shouldTestLargeFiles() {
    String property = System.getProperty("hdfs.test.large.files");
    if (property == null) return false;
    if (property.isEmpty()) return true;
    return Boolean.parseBoolean(property);
  }

  private HdfsConfiguration conf = null;
  private MiniDFSCluster cluster = null;

  /**
   * Setup the cluster
   */
  public BlockReaderTestUtil(int replicationFactor) throws Exception {
    this(replicationFactor, new HdfsConfiguration());
  }

  public BlockReaderTestUtil(MiniDFSCluster cluster, HdfsConfiguration conf) {
    this.conf = conf;
    this.cluster = cluster;
  }

  public BlockReaderTestUtil(int replicationFactor, HdfsConfiguration config) throws Exception {
    this.conf = config;
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    cluster = new MiniDFSCluster.Builder(conf).format(true).build();
    cluster.waitActive();
  }

  /**
   * Shutdown cluster
   */
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  public HdfsConfiguration getConf() {
    return conf;
  }

  /**
   * Create a file of the given size filled with random data.
   * @return  File data.
   */
  public byte[] writeFile(Path filepath, int sizeKB)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();

    // Write a file with the specified amount of data
    DataOutputStream os = fs.create(filepath);
    byte data[] = new byte[1024 * sizeKB];
    new Random().nextBytes(data);
    os.write(data);
    os.close();
    return data;
  }

  /**
   * Get the list of Blocks for a file.
   */
  public List<LocatedBlock> getFileBlocks(Path filepath, int sizeKB)
      throws IOException {
    // Return the blocks we just wrote
    DFSClient dfsclient = getDFSClient();
    return dfsclient.getNamenode().getBlockLocations(
      filepath.toString(), 0, sizeKB * 1024).getLocatedBlocks();
  }

  /**
   * Get the DFSClient.
   */
  public DFSClient getDFSClient() throws IOException {
    InetSocketAddress nnAddr = new InetSocketAddress("localhost", cluster.getNameNodePort());
    return new DFSClient(nnAddr, conf);
  }

  /**
   * Exercise the BlockReader and read length bytes.
   *
   * It does not verify the bytes read.
   */
  public void readAndCheckEOS(BlockReader reader, int length, boolean expectEof)
      throws IOException {
    byte buf[] = new byte[1024];
    int nRead = 0;
    while (nRead < length) {
      DFSClient.LOG.info("So far read " + nRead + " - going to read more.");
      int n = reader.read(buf, 0, buf.length);
      assertTrue(n > 0);
      nRead += n;
    }

    if (expectEof) {
      DFSClient.LOG.info("Done reading, expect EOF for next read.");
      assertEquals(-1, reader.read(buf, 0, buf.length));
    }
  }

  /**
   * Get a BlockReader for the given block.
   */
  public BlockReader getBlockReader(LocatedBlock testBlock, int offset, int lenToRead)
      throws IOException {
    return getBlockReader(cluster.getFileSystem(), testBlock, offset, lenToRead);
  }

  /**
   * Get a BlockReader for the given block.
   */
  public static BlockReader getBlockReader(final DistributedFileSystem fs,
      LocatedBlock testBlock, int offset, long lenToRead) throws IOException {
    InetSocketAddress targetAddr = null;
    ExtendedBlock block = testBlock.getBlock();
    DatanodeInfo[] nodes = testBlock.getLocations();
    targetAddr = NetUtils.createSocketAddr(nodes[0].getXferAddr());

    return new BlockReaderFactory(fs.getClient().getConf()).
      setInetSocketAddress(targetAddr).
      setBlock(block).
      setFileName(targetAddr.toString()+ ":" + block.getBlockId()).
      setBlockToken(testBlock.getBlockToken()).
      setStartOffset(offset).
      setLength(lenToRead).
      setVerifyChecksum(true).
      setClientName("BlockReaderTestUtil").
      setDatanodeInfo(nodes[0]).
      setClientCacheContext(ClientContext.getFromConf(fs.getConf())).
      setCachingStrategy(CachingStrategy.newDefaultStrategy()).
      setConfiguration(fs.getConf()).
      setAllowShortCircuitLocalReads(true).
      setRemotePeerFactory(new RemotePeerFactory() {
        @Override
        public Peer newConnectedPeer(InetSocketAddress addr,
            Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
            throws IOException {
          Peer peer = null;
          Socket sock = NetUtils.
              getDefaultSocketFactory(fs.getConf()).createSocket();
          try {
            sock.connect(addr, HdfsConstants.READ_TIMEOUT);
            sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);
            peer = DFSUtilClient.peerFromSocket(sock);
          } finally {
            if (peer == null) {
              IOUtils.closeStream(sock);
            }
          }
          return peer;
        }
      }).
      build();
  }

  /**
   * Get a DataNode that serves our testBlock.
   */
  public DataNode getDataNode(LocatedBlock testBlock) {
    DatanodeInfo[] nodes = testBlock.getLocations();
    int ipcport = nodes[0].getIpcPort();
    return cluster.getDataNode(ipcport);
  }

  public static void enableHdfsCachingTracing() {
    enableTraceLog(CacheReplicationMonitor.class);
    enableTraceLog(CacheManager.class);
    enableTraceLog(FsDatasetCache.class);
  }

  public static void enableBlockReaderFactoryTracing() {
    enableTraceLog(BlockReaderFactory.class);
    enableTraceLog(ShortCircuitCache.class);
    enableTraceLog(ShortCircuitReplica.class);
    enableTraceLog(BlockReaderLocal.class);
  }

  public static void enableShortCircuitShmTracing() {
    enableTraceLog(DfsClientShmManager.class);
    enableTraceLog(ShortCircuitRegistry.class);
    enableTraceLog(ShortCircuitShm.class);
    enableTraceLog(DataNode.class);
  }

  private static void enableTraceLog(Class<?> clazz) {
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(clazz), Level.TRACE);
  }
}
