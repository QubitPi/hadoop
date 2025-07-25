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
 */

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_BUFFER_SIZE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disablePrefetching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getInputStreamStatistics;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getS3AInputStream;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.isUsingDefaultExternalDataFile;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticMinimum;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMaximumStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MAX;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MIN;
import static org.apache.hadoop.io.Sizes.*;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Look at the performance of S3a Input Stream Reads.
 */
public class ITestS3AInputStreamPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AInputStreamPerformance.class);
  private static final int READAHEAD_128K = S_128K;

  private S3AFileSystem s3aFS;
  private Path testData;
  private FileStatus testDataStatus;
  private FSDataInputStream in;
  private S3AInputStreamStatistics streamStatistics;
  public static final int BLOCK_SIZE = S_32K;
  public static final int BIG_BLOCK_SIZE = S_256K;

  private static final IOStatisticsSnapshot IOSTATS = snapshotIOStatistics();


  /** Tests only run if the there is a named test file that can be read. */
  private boolean testDataAvailable = true;
  private String assumptionMessage = "test file";

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = disablePrefetching(super.createScaleConfiguration());
    if (isUsingDefaultExternalDataFile(conf)) {
      S3ATestUtils.removeBaseAndBucketOverrides(
          conf,
          ENDPOINT);
    }
    return conf;
  }

  /**
   * Open the FS and the test data. The input stream is always set up here.
   * @throws IOException IO Problems.
   */
  @BeforeEach
  public void openFS() throws IOException {
    Configuration conf = getConf();
    conf.setInt(SOCKET_SEND_BUFFER, S_16K);
    conf.setInt(SOCKET_RECV_BUFFER, S_16K);
    // look up the test file, no requirement to be set.
    String testFile =  conf.getTrimmed(KEY_CSVTEST_FILE,
        PublicDatasetTestUtils.DEFAULT_EXTERNAL_FILE);
    if (testFile.isEmpty()) {
      assumptionMessage = "Empty test property: " + KEY_CSVTEST_FILE;
      LOG.warn(assumptionMessage);
      testDataAvailable = false;
    } else {
      testData = new Path(testFile);
      LOG.info("Using {} as input stream source", testData);
      Path path = this.testData;
      bindS3aFS(path);
      try {
        testDataStatus = s3aFS.getFileStatus(this.testData);
      } catch (IOException e) {
        LOG.warn("Failed to read file {} specified in {}",
            testFile, KEY_CSVTEST_FILE, e);
        throw e;
      }
    }
  }

  private void bindS3aFS(Path path) throws IOException {
    s3aFS = (S3AFileSystem) FileSystem.newInstance(path.toUri(), getConf());
  }

  /**
   * Cleanup: close the stream, close the FS.
   */
  @AfterEach
  public void cleanup() {
    describe("cleanup");
    IOUtils.closeStream(in);
    if (in != null) {
      final IOStatistics stats = in.getIOStatistics();
      LOG.info("Stream statistics {}",
          ioStatisticsToPrettyString(stats));
      IOSTATS.aggregate(stats);
    }
    if (s3aFS != null) {
      final IOStatistics stats = s3aFS.getIOStatistics();
      LOG.info("FileSystem statistics {}",
          ioStatisticsToPrettyString(stats));
      FILESYSTEM_IOSTATS.aggregate(stats);
      IOUtils.closeStream(s3aFS);
    }
  }

  @AfterAll
  public static void dumpIOStatistics() {
    LOG.info("Aggregate Stream Statistics {}", IOSTATS);
  }

  /**
   * Declare that the test requires the CSV test dataset.
   */
  private void requireCSVTestData() {
    assume(assumptionMessage, testDataAvailable);
  }

  /**
   * Open the test file with the read buffer specified in the setting.
   * {@link #KEY_READ_BUFFER_SIZE}; use the {@code Normal} policy
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile() throws IOException {
    return openTestFile(S3AInputPolicy.Normal, 0);
  }

  /**
   * Open the test file with the read buffer specified in the setting
   * {@link #KEY_READ_BUFFER_SIZE}.
   * This includes the {@link #requireCSVTestData()} assumption; so
   * if called before any FS op, will automatically skip the test
   * if the CSV file is absent.
   *
   * @param inputPolicy input policy to use
   * @param readahead readahead/buffer size
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile(S3AInputPolicy inputPolicy, long readahead)
      throws IOException {
    requireCSVTestData();
    return openDataFile(s3aFS, testData, inputPolicy, readahead, testDataStatus.getLen());
  }

  /**
   * Open a test file with the read buffer specified in the setting
   * {@link org.apache.hadoop.fs.s3a.S3ATestConstants#KEY_READ_BUFFER_SIZE}.
   *
   * @param path path to open
   * @param inputPolicy input policy to use
   * @param readahead readahead/buffer size
   * @param length
   * @return the stream, wrapping an S3a one
   * @throws IOException IO problems
   */
  private FSDataInputStream openDataFile(S3AFileSystem fs,
      Path path,
      S3AInputPolicy inputPolicy,
      long readahead,
      final long length) throws IOException {
    int bufferSize = getConf().getInt(KEY_READ_BUFFER_SIZE,
        DEFAULT_READ_BUFFER_SIZE);
    final FutureDataInputStreamBuilder builder = fs.openFile(path)
        .opt(FS_OPTION_OPENFILE_READ_POLICY,
            inputPolicy.toString())
        .optLong(FS_OPTION_OPENFILE_LENGTH, length)
        .opt(FS_OPTION_OPENFILE_BUFFER_SIZE, bufferSize)
        .optLong(READAHEAD_RANGE, readahead);

    FSDataInputStream stream = awaitFuture(builder.build());
    streamStatistics = getInputStreamStatistics(stream);
    return stream;
  }

  /**
   * Assert that the stream was only ever opened once.
   */
  protected void assertStreamOpenedExactlyOnce() {
    assertOpenOperationCount(1);
  }

  /**
   * Make an assertion count about the number of open operations.
   * @param expected the expected number
   */
  private void assertOpenOperationCount(long expected) {
    assertEquals(expected, streamStatistics.getOpenOperations(),
        "open operations in\n" + in);
  }

  /**
   * Log how long an IOP took, by dividing the total time by the
   * count of operations, printing in a human-readable form.
   * @param operation operation being measured
   * @param timer timing data
   * @param count IOP count.
   */
  protected void logTimePerIOP(String operation,
      NanoTimer timer,
      long count) {
    LOG.info("Time per {}: {} nS",
        operation, toHuman(timer.duration() / count));
  }

  @Test
  public void testTimeToOpenAndReadWholeFileBlocks() throws Throwable {
    skipIfClientSideEncryption();
    requireCSVTestData();
    int blockSize = _1MB;
    describe("Open the test file %s and read it in blocks of size %d",
        testData, blockSize);
    long len = testDataStatus.getLen();
    in = openTestFile();
    byte[] block = new byte[blockSize];
    NanoTimer timer2 = new NanoTimer();
    long count = 0;
    // implicitly rounding down here
    long blockCount = len / blockSize;
    long totalToRead = blockCount * blockSize;
    long minimumBandwidth = S_128K;
    int maxResetCount = 4;
    int resetCount = 0;
    for (long i = 0; i < blockCount; i++) {
      int offset = 0;
      int remaining = blockSize;
      long blockId = i + 1;
      NanoTimer blockTimer = new NanoTimer();
      int reads = 0;
      while (remaining > 0) {
        NanoTimer readTimer = new NanoTimer();
        int bytesRead = in.read(block, offset, remaining);
        reads++;
        if (bytesRead == 1) {
          break;
        }
        remaining -= bytesRead;
        offset += bytesRead;
        count += bytesRead;
        readTimer.end();
        if (bytesRead != 0) {
          LOG.debug("Bytes in read #{}: {} , block bytes: {}," +
                  " remaining in block: {}" +
                  " duration={} nS; ns/byte: {}, bandwidth={} MB/s",
              reads, bytesRead, blockSize - remaining, remaining,
              readTimer.duration(),
              readTimer.nanosPerOperation(bytesRead),
              readTimer.bandwidthDescription(bytesRead));
        } else {
          LOG.warn("0 bytes returned by read() operation #{}", reads);
        }
      }
      blockTimer.end("Reading block %d in %d reads", blockId, reads);
      String bw = blockTimer.bandwidthDescription(blockSize);
      LOG.info("Bandwidth of block {}: {} MB/s: ", blockId, bw);
      if (bandwidth(blockTimer, blockSize) < minimumBandwidth) {
        LOG.warn("Bandwidth {} too low on block {}: resetting connection",
            bw, blockId);
        Assertions.assertThat(resetCount)
            .describedAs("Bandwidth of %s too low after  %s attempts",
                bw, resetCount)
            .isLessThanOrEqualTo(maxResetCount);
        resetCount++;
        // reset the connection
        getS3AInputStream(in).resetConnection();
      }
    }
    timer2.end("Time to read %d bytes in %d blocks", totalToRead, blockCount);
    LOG.info("Overall Bandwidth {} MB/s; reset connections {}",
        timer2.bandwidth(totalToRead), resetCount);
    logStreamStatistics();
  }

  /**
   * Work out the bandwidth in bytes/second.
   * @param timer timer measuring the duration
   * @param bytes bytes
   * @return the number of bytes/second of the recorded operation
   */
  public static double bandwidth(NanoTimer timer, long bytes) {
    return bytes * 1.0e9 / timer.duration();
  }

  @Test
  public void testLazySeekEnabled() throws Throwable {
    describe("Verify that seeks do not trigger any IO");
    in = openTestFile();
    long len = testDataStatus.getLen();
    NanoTimer timer = new NanoTimer();
    long blockCount = len / BLOCK_SIZE;
    for (long i = 0; i < blockCount; i++) {
      in.seek(in.getPos() + BLOCK_SIZE - 1);
    }
    in.seek(0);
    blockCount++;
    timer.end("Time to execute %d seeks", blockCount);
    logTimePerIOP("seek()", timer, blockCount);
    logStreamStatistics();
    assertOpenOperationCount(0);
    assertEquals(0, streamStatistics.getBytesRead(), "bytes read");
  }

  @Test
  public void testReadaheadOutOfRange() throws Throwable {
    try {
      in = openTestFile();
      in.setReadahead(-1L);
      fail("Stream should have rejected the request "+ in);
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testReadWithNormalPolicy() throws Throwable {
    describe("Read big blocks with a big readahead");
    skipIfClientSideEncryption();
    executeSeekReadSequence(BIG_BLOCK_SIZE, BIG_BLOCK_SIZE * 2,
        S3AInputPolicy.Normal);
    assertStreamOpenedExactlyOnce();
  }

  @Test
  public void testDecompressionSequential128K() throws Throwable {
    describe("Decompress with a 128K readahead");
    skipIfClientSideEncryption();
    executeDecompression(READAHEAD_128K, S3AInputPolicy.Sequential);
    assertStreamOpenedExactlyOnce();
  }

  /**
   * Execute a decompression + line read with the given input policy.
   * @param readahead byte readahead
   * @param inputPolicy read policy
   * @throws IOException IO Problems
   */
  private void executeDecompression(long readahead,
      S3AInputPolicy inputPolicy) throws IOException {
    CompressionCodecFactory factory
        = new CompressionCodecFactory(getConf());
    CompressionCodec codec = factory.getCodec(testData);
    Assertions.assertThat(codec)
        .describedAs("No codec found for %s", testData)
        .isNotNull();
    long bytesRead = 0;
    int lines = 0;

    FSDataInputStream objectIn = openTestFile(inputPolicy, readahead);
    IOStatistics readerStatistics = null;
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (LineReader lineReader = new LineReader(
        codec.createInputStream(objectIn), getConf())) {
      readerStatistics = lineReader.getIOStatistics();
      Text line = new Text();
      int read;
      while ((read = lineReader.readLine(line)) > 0) {
        bytesRead += read;
        lines++;
      }
    } catch (EOFException eof) {
      // done
    }
    timer.end("Time to read %d lines [%d bytes expanded, %d raw]" +
        " with readahead = %d",
        lines,
        bytesRead,
        testDataStatus.getLen(),
        readahead);
    logTimePerIOP("line read", timer, lines);
    logStreamStatistics();
    assertNotNull(readerStatistics, "No IOStatistics through line reader");
    LOG.info("statistics from reader {}",
        ioStatisticsToString(readerStatistics));
  }

  private void logStreamStatistics() {
    LOG.info(String.format("Stream Statistics%n{}"), streamStatistics);
  }

  /**
   * Execute a seek+read sequence.
   * @param blockSize block size for seeks
   * @param readahead what the readahead value of the stream should be
   * @throws IOException IO problems
   */
  protected void executeSeekReadSequence(long blockSize,
      long readahead,
      S3AInputPolicy policy) throws IOException {
    in = openTestFile(policy, readahead);
    long len = testDataStatus.getLen();
    NanoTimer timer = new NanoTimer();
    long blockCount = len / blockSize;
    LOG.info("Reading {} blocks, readahead = {}",
        blockCount, readahead);
    for (long i = 0; i < blockCount; i++) {
      in.seek(in.getPos() + blockSize - 1);
      // this is the read
      assertTrue(in.read() >= 0);
    }
    timer.end("Time to execute %d seeks of distance %d with readahead = %d",
        blockCount,
        blockSize,
        readahead);
    logTimePerIOP("seek(pos + " + blockCount+"); read()", timer, blockCount);
    LOG.info("Effective bandwidth {} MB/S",
        timer.bandwidthDescription(streamStatistics.getBytesRead() -
            streamStatistics.getBytesSkippedOnSeek()));
    logStreamStatistics();
  }

  private static final int[][] RANDOM_IO_SEQUENCE = {
      {S_2M, S_128K},
      {S_128K, S_128K},
      {S_5M, S_64K},
      {_1MB, _1MB},
  };

  @Test
  public void testRandomIORandomPolicy() throws Throwable {
    skipIfClientSideEncryption();
    executeRandomIO(S3AInputPolicy.Random, (long) RANDOM_IO_SEQUENCE.length);
    assertEquals(0, streamStatistics.getAborted(),
        "streams aborted in " + streamStatistics);
  }

  @Test
  public void testRandomIONormalPolicy() throws Throwable {
    skipIfClientSideEncryption();
    long expectedOpenCount = RANDOM_IO_SEQUENCE.length;
    executeRandomIO(S3AInputPolicy.Normal, expectedOpenCount);
    assertEquals(1, streamStatistics.getAborted(),
        "streams aborted in " + streamStatistics);
    assertEquals(2, streamStatistics.getPolicySetCount(),
        "policy changes in " + streamStatistics);
    assertEquals(S3AInputPolicy.Random.ordinal(),
        streamStatistics.getInputPolicy(), "input policy in " + streamStatistics);
    IOStatistics ioStatistics = streamStatistics.getIOStatistics();
    verifyStatisticCounterValue(
        ioStatistics,
        StreamStatisticNames.STREAM_READ_ABORTED,
        1);
    verifyStatisticCounterValue(
        ioStatistics,
        StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
        2);
  }

  /**
   * Execute the random IO {@code readFully(pos, bytes[])} sequence defined by
   * {@link #RANDOM_IO_SEQUENCE}. The stream is closed afterwards; that's used
   * in the timing too
   * @param policy read policy
   * @param expectedOpenCount expected number of stream openings
   * @throws IOException IO problems
   * @return the timer
   */
  private ContractTestUtils.NanoTimer executeRandomIO(S3AInputPolicy policy,
      long expectedOpenCount)
      throws IOException {
    describe("Random IO with policy \"%s\"", policy);
    byte[] buffer = new byte[S_1M];
    long totalBytesRead = 0;
    final long len = testDataStatus.getLen();
    in = openTestFile(policy, 0);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    for (int[] action : RANDOM_IO_SEQUENCE) {
      long position = action[0];
      int range = action[1];
      // if a read goes past EOF, fail with details
      // this will happen if the test datafile is too small.
      Assertions.assertThat(position + range)
          .describedAs("readFully(pos=%d range=%d) of %s",
              position, range, testDataStatus)
          .isLessThanOrEqualTo(len);
      in.readFully(position, buffer, 0, range);
      totalBytesRead += range;
    }
    int reads = RANDOM_IO_SEQUENCE.length;
    timer.end("Time to execute %d reads of total size %d bytes",
        reads,
        totalBytesRead);
    in.close();
    assertOpenOperationCount(expectedOpenCount);
    logTimePerIOP("byte read", timer, totalBytesRead);
    LOG.info("Effective bandwidth {} MB/S",
        timer.bandwidthDescription(streamStatistics.getBytesRead() -
            streamStatistics.getBytesSkippedOnSeek()));
    logStreamStatistics();
    IOStatistics iostats = in.getIOStatistics();
    long maxHttpGet = lookupMaximumStatistic(iostats,
        ACTION_HTTP_GET_REQUEST + SUFFIX_MAX);
    assertThatStatisticMinimum(iostats,
        ACTION_HTTP_GET_REQUEST + SUFFIX_MIN)
        .isGreaterThan(0)
        .isLessThan(maxHttpGet);
    MeanStatistic getMeanStat = lookupMeanStatistic(iostats,
        ACTION_HTTP_GET_REQUEST + SUFFIX_MEAN);
    Assertions.assertThat(getMeanStat.getSamples())
        .describedAs("sample count of %s", getMeanStat)
        .isEqualTo(expectedOpenCount);

    return timer;
  }

  S3AInputStream getS3aStream() {
    return (S3AInputStream) in.getWrappedStream();
  }

  @Test
  public void testRandomReadOverBuffer() throws Throwable {
    describe("read over a buffer, making sure that the requests" +
        " spans readahead ranges");
    int datasetLen = S_32K;
    S3AFileSystem fs = getFileSystem();
    Path dataFile = path("testReadOverBuffer.bin");
    byte[] sourceData = dataset(datasetLen, 0, 64);
    // relies on the field 'fs' referring to the R/W FS
    writeDataset(fs, dataFile, sourceData, datasetLen, S_16K, true);
    byte[] buffer = new byte[datasetLen];
    int readahead = S_8K;
    int halfReadahead = S_4K;
    in = openDataFile(fs, dataFile, S3AInputPolicy.Random, readahead, datasetLen);

    LOG.info("Starting initial reads");
    S3AInputStream s3aStream = getS3aStream();
    assertEquals(readahead, s3aStream.getReadahead());
    byte[] oneByte = new byte[1];
    assertEquals(1, in.read(0, oneByte, 0, 1));
    // make some assertions about the current state
    assertEquals(readahead - 1, s3aStream.remainingInCurrentRequest(), "remaining in\n" + in);
    assertEquals(0, s3aStream.getContentRangeStart(), "range start in\n" + in);
    assertEquals(readahead, s3aStream.getContentRangeFinish(), "range finish in\n" + in);

    assertStreamOpenedExactlyOnce();

    describe("Starting sequence of positioned read calls over\n%s", in);
    NanoTimer readTimer = new NanoTimer();
    int currentPos = halfReadahead;
    int offset = currentPos;
    int bytesRead = 0;
    int readOps = 0;

    // make multiple read() calls
    while (bytesRead < halfReadahead) {
      int length = buffer.length - offset;
      int read = in.read(currentPos, buffer, offset, length);
      bytesRead += read;
      offset += read;
      readOps++;
      assertEquals(1, streamStatistics.getOpenOperations(),
          "open operations on request #" + readOps
          + " after reading " + bytesRead
          + " current position in stream " + currentPos
          + " in\n" + fs
          + "\n " + in);
      for (int i = currentPos; i < currentPos + read; i++) {
        assertEquals(
           sourceData[i], buffer[i], "Wrong value from byte " + i);
      }
      currentPos += read;
    }
    assertStreamOpenedExactlyOnce();
    // assert at the end of the original block
    assertEquals(readahead, currentPos);
    readTimer.end("read %d in %d operations", bytesRead, readOps);
    bandwidth(readTimer, bytesRead);
    LOG.info("Time per byte(): {} nS",
        toHuman(readTimer.nanosPerOperation(bytesRead)));
    LOG.info("Time per read(): {} nS",
        toHuman(readTimer.nanosPerOperation(readOps)));

    describe("read last byte");
    // read one more
    int read = in.read(currentPos, buffer, bytesRead, 1);
    assertTrue(read >= 0, "-1 from last read");
    assertOpenOperationCount(2);
    assertEquals(sourceData[currentPos], (int) buffer[currentPos],
        "Wrong value from read ");
    currentPos++;


    // now scan all the way to the end of the file, using single byte read()
    // calls
    describe("read() to EOF over \n%s", in);
    long readCount = 0;
    NanoTimer timer = new NanoTimer();
    LOG.info("seeking");
    in.seek(currentPos);
    LOG.info("reading");
    while(currentPos < datasetLen) {
      int r = in.read();
      assertTrue(r >= 0, "Negative read() at position " + currentPos + " in\n" + in);
      buffer[currentPos] = (byte)r;
      assertEquals(sourceData[currentPos], r, "Wrong value from read from\n" + in);
      currentPos++;
      readCount++;
    }
    timer.end("read %d bytes", readCount);
    bandwidth(timer, readCount);
    LOG.info("Time per read(): {} nS",
        toHuman(timer.nanosPerOperation(readCount)));

    assertEquals(-1, in.read(), "last read in " + in);
  }
}
