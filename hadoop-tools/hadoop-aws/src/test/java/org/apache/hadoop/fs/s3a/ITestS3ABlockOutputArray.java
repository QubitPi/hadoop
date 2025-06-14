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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.UploadContentProviders;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.io.IOUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.apache.hadoop.fs.StreamCapabilities.ABORTABLE_STREAM;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.assertCompleteAbort;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.assertNoopAbort;

/**
 * Tests small file upload functionality for
 * {@link S3ABlockOutputStream} with the blocks buffered in byte arrays.
 *
 * File sizes are kept small to reduce test duration on slow connections;
 * multipart tests are kept in scale tests.
 */
public class ITestS3ABlockOutputArray extends AbstractS3ATestBase {
  private static final int BLOCK_SIZE = 256 * 1024;

  private static byte[] dataset;

  @BeforeAll
  public static void setupDataset() {
    dataset = ContractTestUtils.dataset(BLOCK_SIZE, 0, 256);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.set(FAST_UPLOAD_BUFFER, getBlockOutputBufferName());
    return conf;
  }

  protected String getBlockOutputBufferName() {
    return FAST_UPLOAD_BUFFER_ARRAY;
  }

  @Test
  public void testZeroByteUpload() throws IOException {
    verifyUpload("0", 0);
  }

  @Test
  public void testRegularUpload() throws IOException {
    verifyUpload("regular", 1024);
  }

  @Test
  public void testWriteAfterStreamClose() throws Throwable {
    assertThrows(IOException.class, () -> {
      Path dest = path("testWriteAfterStreamClose");
      describe(" testWriteAfterStreamClose");
      FSDataOutputStream stream = getFileSystem().create(dest, true);
      byte[] data = ContractTestUtils.dataset(16, 'a', 26);
      try {
        stream.write(data);
        stream.close();
        stream.write(data);
      } finally {
        IOUtils.closeStream(stream);
      }
    });
  }

  @Test
  public void testBlocksClosed() throws Throwable {
    Path dest = path("testBlocksClosed");
    describe(" testBlocksClosed");
    FSDataOutputStream stream = getFileSystem().create(dest, true);
    BlockOutputStreamStatistics statistics
        = S3ATestUtils.getOutputStreamStatistics(stream);
    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    stream.write(data);
    LOG.info("closing output stream");
    stream.close();
    assertEquals(1, statistics.getBlocksAllocated(),
        "total allocated blocks in " + statistics);
    assertEquals(0, statistics.getBlocksActivelyAllocated(),
        "actively allocated blocks in " + statistics);
    LOG.info("end of test case");
  }

  private void verifyUpload(String name, int fileSize) throws IOException {
    Path dest = path(name);
    describe(name + " upload to " + dest);
    ContractTestUtils.createAndVerifyFile(
        getFileSystem(),
        dest,
        fileSize);
  }

  /**
   * Create a factory for used in mark/reset tests.
   * @param fileSystem source FS
   * @return the factory
   */
  protected S3ADataBlocks.BlockFactory createFactory(S3AFileSystem fileSystem) {
    return new S3ADataBlocks.ArrayBlockFactory(fileSystem.createStoreContext());
  }

  private void markAndResetDatablock(S3ADataBlocks.BlockFactory factory)
      throws Exception {
    S3AInstrumentation instrumentation =
        new S3AInstrumentation(new URI("s3a://example"));
    BlockOutputStreamStatistics outstats
        = instrumentation.newOutputStreamStatistics(null);
    S3ADataBlocks.DataBlock block = factory.create(1, BLOCK_SIZE, outstats);
    block.write(dataset, 0, dataset.length);
    S3ADataBlocks.BlockUploadData uploadData = block.startUpload();
    final UploadContentProviders.BaseContentProvider cp = uploadData.getContentProvider();
    InputStream stream = cp.newStream();
    assertNotNull(stream);
    assertEquals(0, stream.read());
    stream.mark(BLOCK_SIZE);
    // read a lot
    long l = 0;
    while (stream.read() != -1) {
      // do nothing
      l++;
    }
    stream.reset();
    assertEquals(1, stream.read());
  }

  @Test
  public void testMarkReset() throws Throwable {
    markAndResetDatablock(createFactory(getFileSystem()));
  }

  @Test
  public void testAbortAfterWrite() throws Throwable {
    describe("Verify abort after a write does not create a file");
    Path dest = path(getMethodName());
    FileSystem fs = getFileSystem();
    ContractTestUtils.assertHasPathCapabilities(fs, dest, ABORTABLE_STREAM);
    FSDataOutputStream stream = fs.create(dest, true);
    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    try {
      ContractTestUtils.assertCapabilities(stream,
          new String[]{ABORTABLE_STREAM},
          null);
      stream.write(data);
      assertCompleteAbort(stream.abort());
      // second attempt is harmless
      assertNoopAbort(stream.abort());

      // the path should not exist
      ContractTestUtils.assertPathsDoNotExist(fs, "aborted file", dest);
    } finally {
      IOUtils.closeStream(stream);
      // check the path doesn't exist "after" closing stream
      ContractTestUtils.assertPathsDoNotExist(fs, "aborted file", dest);
    }
    // and it can be called on the stream after being closed.
    assertNoopAbort(stream.abort());
  }

  /**
   * A stream which was abort()ed after being close()d for a
   * successful write will return indicating nothing happened.
   */
  @Test
  public void testAbortAfterCloseIsHarmless() throws Throwable {
    describe("Verify abort on a closed stream is harmless "
        + "and that the result indicates that nothing happened");
    Path dest = path(getMethodName());
    FileSystem fs = getFileSystem();
    byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    try (FSDataOutputStream stream = fs.create(dest, true)) {
      stream.write(data);
      assertCompleteAbort(stream.abort());
      stream.close();
      assertNoopAbort(stream.abort());
    }
  }

}
