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
package org.apache.hadoop.io.compress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipCompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCodec {

  private static final Logger LOG= LoggerFactory.getLogger(TestCodec.class);

  private Configuration conf = new Configuration();
  private int count = 10000;
  private int seed = new Random().nextInt();

  @AfterEach
  public void after() {
    ZlibFactory.loadNativeZLib();
  }
  @Test
  public void testDefaultCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DefaultCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DefaultCodec");
  }

  @Test
  public void testGzipCodec() throws IOException {
    Configuration conf = new Configuration();
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
      codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
    }
    // without hadoop-native installed.
    ZlibFactory.setNativeZlibLoaded(false);
    assertFalse(ZlibFactory.isNativeZlibLoaded(conf));
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Test
  @Timeout(value = 20)
  public void testBZip2Codec() throws IOException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "java-builtin");
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
  }
  
  @Test
  @Timeout(value = 20)
  public void testBZip2NativeCodec() throws IOException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "system-native");
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
        codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
        codecTest(conf, seed, count, 
                  "org.apache.hadoop.io.compress.BZip2Codec");
        conf.set("io.compression.codec.bzip2.library", "java-builtin");
        codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
        codecTest(conf, seed, count, 
                  "org.apache.hadoop.io.compress.BZip2Codec");
      } else {
        LOG.warn("Native hadoop library available but native bzip2 is not");
      }
    }
  }
  
  @Test
  public void testSnappyCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.SnappyCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.SnappyCodec");
  }
  
  @Test
  public void testLz4Codec() throws IOException {
    conf.setBoolean(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
        false);
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
    conf.setBoolean(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
        true);
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
  }

  @Test
  public void testDeflateCodec() throws IOException {
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DeflateCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DeflateCodec");
  }

  @Test
  public void testGzipCodecWithParam() throws IOException {
    Configuration conf = new Configuration(this.conf);
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.BEST_COMPRESSION);
    ZlibFactory.setCompressionStrategy(conf, CompressionStrategy.HUFFMAN_ONLY);
    codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
    codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
  }

  private static void codecTest(Configuration conf, int seed, int count, 
                                String codecClass) 
    throws IOException {
    
    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec)
        ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    LOG.info("Created a Codec object of type: " + codecClass);

    // Generate data
    DataOutputBuffer data = new DataOutputBuffer();
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for(int i=0; i < count; ++i) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      
      key.write(data);
      value.write(data);
    }
    LOG.info("Generated " + count + " records");
    
    // Compress data
    DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
    int leasedCompressorsBefore = codec.getCompressorType() == null ? -1
        : CodecPool.getLeasedCompressorsCount(codec);
    try (CompressionOutputStream deflateFilter =
      codec.createOutputStream(compressedDataBuffer);
      DataOutputStream deflateOut =
        new DataOutputStream(new BufferedOutputStream(deflateFilter))) {
      deflateOut.write(data.getData(), 0, data.getLength());
      deflateOut.flush();
      deflateFilter.finish();
    }
    if (leasedCompressorsBefore > -1) {
      assertEquals(leasedCompressorsBefore, CodecPool.getLeasedCompressorsCount(codec),
          "leased compressor not returned to the codec pool");
    }
    LOG.info("Finished compressing data");
    
    // De-compress data
    DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, 
                                 compressedDataBuffer.getLength());
    DataInputBuffer originalData = new DataInputBuffer();
    int leasedDecompressorsBefore =
        CodecPool.getLeasedDecompressorsCount(codec);
    try (CompressionInputStream inflateFilter =
      codec.createInputStream(deCompressedDataBuffer);
      DataInputStream inflateIn =
        new DataInputStream(new BufferedInputStream(inflateFilter))) {

      // Check
      originalData.reset(data.getData(), 0, data.getLength());
      DataInputStream originalIn =
          new DataInputStream(new BufferedInputStream(originalData));
      for(int i=0; i < count; ++i) {
        RandomDatum k1 = new RandomDatum();
        RandomDatum v1 = new RandomDatum();
        k1.readFields(originalIn);
        v1.readFields(originalIn);
      
        RandomDatum k2 = new RandomDatum();
        RandomDatum v2 = new RandomDatum();
        k2.readFields(inflateIn);
        v2.readFields(inflateIn);
        assertTrue(k1.equals(k2) && v1.equals(v2),
            "original and compressed-then-decompressed-output not equal");
      
        // original and compressed-then-decompressed-output have the same
        // hashCode
        Map<RandomDatum, String> m = new HashMap<RandomDatum, String>();
        m.put(k1, k1.toString());
        m.put(v1, v1.toString());
        String result = m.get(k2);
        assertEquals(result, k1.toString(), "k1 and k2 hashcode not equal");
        result = m.get(v2);
        assertEquals(result, v1.toString(), "v1 and v2 hashcode not equal");
      }
    }
    assertEquals(leasedDecompressorsBefore,
        CodecPool.getLeasedDecompressorsCount(codec),
        "leased decompressor not returned to the codec pool");

    // De-compress data byte-at-a-time
    originalData.reset(data.getData(), 0, data.getLength());
    deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, 
                                 compressedDataBuffer.getLength());
    try (CompressionInputStream inflateFilter =
      codec.createInputStream(deCompressedDataBuffer);
      DataInputStream originalIn =
        new DataInputStream(new BufferedInputStream(originalData))) {

      // Check
      int expected;
      do {
        expected = originalIn.read();
        assertEquals(expected, inflateFilter.read(),
            "Inflated stream read by byte does not match");
      } while (expected != -1);
    }

    LOG.info("SUCCESS! Completed checking " + count + " records");
  }

  @Test
  public void testSplitableCodecs() throws Exception {
    testSplitableCodec(BZip2Codec.class);
  }

  private void testSplitableCodec(
      Class<? extends SplittableCompressionCodec> codecClass)
      throws IOException {
    final long DEFLBYTES = 2 * 1024 * 1024;
    final Configuration conf = new Configuration();
    final Random rand = new Random();
    final long seed = rand.nextLong();
    LOG.info("seed: " + seed);
    rand.setSeed(seed);
    SplittableCompressionCodec codec =
      ReflectionUtils.newInstance(codecClass, conf);
    final FileSystem fs = FileSystem.getLocal(conf);
    final FileStatus infile =
      fs.getFileStatus(writeSplitTestFile(fs, rand, codec, DEFLBYTES));
    if (infile.getLen() > Integer.MAX_VALUE) {
      fail("Unexpected compression: " + DEFLBYTES + " -> " + infile.getLen());
    }
    final int flen = (int) infile.getLen();
    final Text line = new Text();
    final Decompressor dcmp = CodecPool.getDecompressor(codec);
    try {
      for (int pos = 0; pos < infile.getLen(); pos += rand.nextInt(flen / 8)) {
        // read from random positions, verifying that there exist two sequential
        // lines as written in writeSplitTestFile
        final SplitCompressionInputStream in =
          codec.createInputStream(fs.open(infile.getPath()), dcmp,
              pos, flen, SplittableCompressionCodec.READ_MODE.BYBLOCK);
        if (in.getAdjustedStart() >= flen) {
          break;
        }
        LOG.info("SAMPLE " + in.getAdjustedStart() + "," + in.getAdjustedEnd());
        final LineReader lreader = new LineReader(in);
        lreader.readLine(line); // ignore; likely partial
        if (in.getPos() >= flen) {
          break;
        }
        lreader.readLine(line);
        final int seq1 = readLeadingInt(line);
        lreader.readLine(line);
        if (in.getPos() >= flen) {
          break;
        }
        final int seq2 = readLeadingInt(line);
        assertEquals(seq1 + 1, seq2, "Mismatched lines");
      }
    } finally {
      CodecPool.returnDecompressor(dcmp);
    }
    // remove on success
    fs.delete(infile.getPath().getParent(), true);
  }

  private static int readLeadingInt(Text txt) throws IOException {
    DataInputStream in =
      new DataInputStream(new ByteArrayInputStream(txt.getBytes()));
    return in.readInt();
  }

  /** Write infLen bytes (deflated) to file in test dir using codec.
   * Records are of the form
   * &lt;i&gt;&lt;b64 rand&gt;&lt;i+i&gt;&lt;b64 rand&gt;
   */
  private static Path writeSplitTestFile(FileSystem fs, Random rand,
      CompressionCodec codec, long infLen) throws IOException {
    final int REC_SIZE = 1024;
    final Path wd = new Path(GenericTestUtils.getTempPath(
        codec.getClass().getSimpleName())).makeQualified(
            fs.getUri(), fs.getWorkingDirectory());
    final Path file = new Path(wd, "test" + codec.getDefaultExtension());
    final byte[] b = new byte[REC_SIZE];
    final Base64 b64 = new Base64(0, null);
    Compressor cmp = CodecPool.getCompressor(codec);
    try (DataOutputStream fout =
             new DataOutputStream(codec.createOutputStream(fs.create(file,
                 true), cmp))) {
      final DataOutputBuffer dob = new DataOutputBuffer(REC_SIZE * 4 / 3 + 4);
      int seq = 0;
      while (infLen > 0) {
        rand.nextBytes(b);
        final byte[] b64enc = b64.encode(b); // ensures rand printable, no LF
        dob.reset();
        dob.writeInt(seq);
        System.arraycopy(dob.getData(), 0, b64enc, 0, dob.getLength());
        fout.write(b64enc);
        fout.write('\n');
        ++seq;
        infLen -= b64enc.length;
      }
      LOG.info("Wrote " + seq + " records to " + file);
    } finally {
      CodecPool.returnCompressor(cmp);
    }
    return file;
  }

  @Test
  public void testCodecPoolGzipReuse() throws Exception {
    Configuration conf = new Configuration();
    assumeTrue(ZlibFactory.isNativeZlibLoaded(conf));
    GzipCodec gzc = ReflectionUtils.newInstance(GzipCodec.class, conf);
    DefaultCodec dfc = ReflectionUtils.newInstance(DefaultCodec.class, conf);
    Compressor c1 = CodecPool.getCompressor(gzc);
    Compressor c2 = CodecPool.getCompressor(dfc);
    CodecPool.returnCompressor(c1);
    CodecPool.returnCompressor(c2);
    assertTrue(c2 != CodecPool.getCompressor(gzc), "Got mismatched ZlibCompressor");
  }

  private static void gzipReinitTest(Configuration conf, CompressionCodec codec)
      throws IOException {
    // Add codec to cache
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.BEST_COMPRESSION);
    ZlibFactory.setCompressionStrategy(conf,
        CompressionStrategy.DEFAULT_STRATEGY);
    Compressor c1 = CodecPool.getCompressor(codec);
    CodecPool.returnCompressor(c1);
    // reset compressor's compression level to perform no compression
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.NO_COMPRESSION);
    Compressor c2 = CodecPool.getCompressor(codec, conf);
    // ensure same compressor placed earlier
    assertTrue(c1 == c2, "Got mismatched ZlibCompressor");
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    CompressionOutputStream cos = null;
    // write trivially compressable data
    byte[] b = new byte[1 << 15];
    Arrays.fill(b, (byte) 43);
    try {
      cos = codec.createOutputStream(bos, c2);
      cos.write(b);
    } finally {
      if (cos != null) {
        cos.close();
      }
      CodecPool.returnCompressor(c2);
    }
    byte[] outbytes = bos.toByteArray();
    // verify data were not compressed
    assertTrue(outbytes.length >= b.length,
        "Compressed bytes contrary to configuration");
  }

  private static void codecTestWithNOCompression (Configuration conf,
                      String codecClass) throws IOException {
    // Create a compressor with NO_COMPRESSION and make sure that
    // output is not compressed by comparing the size with the
    // original input

    CompressionCodec codec = null;
    ZlibFactory.setCompressionLevel(conf, CompressionLevel.NO_COMPRESSION);
    try {
      codec = (CompressionCodec)
        ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    Compressor c = codec.createCompressor();
    // ensure same compressor placed earlier
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    CompressionOutputStream cos = null;
    // write trivially compressable data
    byte[] b = new byte[1 << 15];
    Arrays.fill(b, (byte) 43);
    try {
      cos = codec.createOutputStream(bos, c);
      cos.write(b);
    } finally {
      if (cos != null) {
        cos.close();
      }
    }
    byte[] outbytes = bos.toByteArray();
    // verify data were not compressed
    assertTrue(outbytes.length >= b.length,
        "Compressed bytes contrary to configuration(NO_COMPRESSION)");
  }

  @Test
  public void testCodecInitWithCompressionLevel() throws Exception {
    Configuration conf = new Configuration();
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      LOG.info("testCodecInitWithCompressionLevel with native");
      codecTestWithNOCompression(conf,
                            "org.apache.hadoop.io.compress.GzipCodec");
      codecTestWithNOCompression(conf,
                         "org.apache.hadoop.io.compress.DefaultCodec");
    }
    conf = new Configuration();
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    LOG.info("testCodecInitWithCompressionLevel without native libs");
    codecTestWithNOCompression( conf,
                         "org.apache.hadoop.io.compress.DefaultCodec");
    codecTestWithNOCompression(conf,
            "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Test
  public void testCodecPoolCompressorReinit() throws Exception {
    Configuration conf = new Configuration();
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      GzipCodec gzc = ReflectionUtils.newInstance(GzipCodec.class, conf);
      gzipReinitTest(conf, gzc);
    } else {
      LOG.warn("testCodecPoolCompressorReinit skipped: native libs not loaded");
    }
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    DefaultCodec dfc = ReflectionUtils.newInstance(DefaultCodec.class, conf);
    gzipReinitTest(conf, dfc);
  }

  @Test
  public void testSequenceFileDefaultCodec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DefaultCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DefaultCodec", 1000000);
  }

  @Test
  @Timeout(value = 20)
  public void testSequenceFileBZip2Codec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "java-builtin");
    sequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000);
  }

  @Test
  @Timeout(value = 20)
  public void testSequenceFileZStandardCodec() throws Exception {
    assumeTrue(ZStandardCodec.isNativeCodeLoaded());
    Configuration conf = new Configuration();
    sequenceFileCodecTest(conf, 0,
        "org.apache.hadoop.io.compress.ZStandardCodec", 100);
    sequenceFileCodecTest(conf, 100,
        "org.apache.hadoop.io.compress.ZStandardCodec", 100);
    sequenceFileCodecTest(conf, 200000,
        "org.apache.hadoop.io.compress.ZStandardCodec", 1000000);
  }

  @Test
  @Timeout(value = 20)
  public void testSequenceFileBZip2NativeCodec() throws IOException, 
                        ClassNotFoundException, InstantiationException, 
                        IllegalAccessException {
    Configuration conf = new Configuration();
    conf.set("io.compression.codec.bzip2.library", "system-native");
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      if (Bzip2Factory.isNativeBzip2Loaded(conf)) {
        sequenceFileCodecTest(conf, 0, 
                              "org.apache.hadoop.io.compress.BZip2Codec", 100);
        sequenceFileCodecTest(conf, 100, 
                              "org.apache.hadoop.io.compress.BZip2Codec", 100);
        sequenceFileCodecTest(conf, 200000, 
                              "org.apache.hadoop.io.compress.BZip2Codec", 
                              1000000);
      } else {
        LOG.warn("Native hadoop library available but native bzip2 is not");
      }
    }
  }

  @Test
  public void testSequenceFileDeflateCodec() throws IOException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DeflateCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DeflateCodec", 1000000);
  }

  @Test
  public void testSequenceFileGzipCodec() throws IOException, ClassNotFoundException,
          InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();
    if (ZlibFactory.isNativeZlibLoaded(conf)) {
      sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.GzipCodec", 5);
      sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.GzipCodec", 100);
      sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.GzipCodec", 1000000);
    }
    // without hadoop-native installed.
    ZlibFactory.setNativeZlibLoaded(false);
    assertFalse(ZlibFactory.isNativeZlibLoaded(conf));
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.GzipCodec", 5);
    sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.GzipCodec", 100);
    sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.GzipCodec", 1000000);
  }

  private static void sequenceFileCodecTest(Configuration conf, int lines, 
                                String codecClass, int blockSize) 
    throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

    Path filePath = new Path("SequenceFileCodecTest." + codecClass);
    // Configuration
    conf.setInt("io.seqfile.compress.blocksize", blockSize);
    
    // Create the SequenceFile
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Creating SequenceFile with codec \"" + codecClass + "\"");
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, filePath, 
        Text.class, Text.class, CompressionType.BLOCK, 
        (CompressionCodec)Class.forName(codecClass).newInstance());
    
    // Write some data
    LOG.info("Writing to SequenceFile...");
    for (int i=0; i<lines; i++) {
      Text key = new Text("key" + i);
      Text value = new Text("value" + i);
      writer.append(key, value);
    }
    writer.close();
    
    // Read the data back and check
    LOG.info("Reading from the SequenceFile...");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);
    
    Writable key = (Writable)reader.getKeyClass().newInstance();
    Writable value = (Writable)reader.getValueClass().newInstance();
    
    int lc = 0;
    try {
      while (reader.next(key, value)) {
        assertEquals("key" + lc, key.toString());
        assertEquals("value" + lc, value.toString());
        lc ++;
      }
    } finally {
      reader.close();
    }
    assertEquals(lines, lc);

    // Delete temporary files
    fs.delete(filePath, false);

    LOG.info("SUCCESS! Completed SequenceFileCodecTest with codec \"" + codecClass + "\"");
  }
  
  /**
   * Regression test for HADOOP-8423: seeking in a block-compressed
   * stream would not properly reset the block decompressor state.
   */
  @Test
  public void testSnappyMapFile() throws Exception {
    codecTestMapFile(SnappyCodec.class, CompressionType.BLOCK, 100);
  }
  
  private void codecTestMapFile(Class<? extends CompressionCodec> clazz,
      CompressionType type, int records) throws Exception {
    
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Creating MapFiles with " + records  + 
            " records using codec " + clazz.getSimpleName());
    Path path = new Path(GenericTestUtils.getTempPath(
        clazz.getSimpleName() + "-" + type + "-" + records));

    LOG.info("Writing " + path);
    createMapFile(conf, fs, path, clazz.newInstance(), type, records);
    MapFile.Reader reader = new MapFile.Reader(path, conf);
    Text key1 = new Text("002");
    assertNotNull(reader.get(key1, new Text()));
    Text key2 = new Text("004");
    assertNotNull(reader.get(key2, new Text()));
  }
  
  private static void createMapFile(Configuration conf, FileSystem fs, Path path, 
      CompressionCodec codec, CompressionType type, int records) throws IOException {
    MapFile.Writer writer = 
        new MapFile.Writer(conf, path,
            MapFile.Writer.keyClass(Text.class),
            MapFile.Writer.valueClass(Text.class),
            MapFile.Writer.compression(type, codec));
    Text key = new Text();
    for (int j = 0; j < records; j++) {
        key.set(String.format("%03d", j));
        writer.append(key, key);
    }
    writer.close();
  }

  public static void main(String[] args) throws IOException {
    int count = 10000;
    String codecClass = "org.apache.hadoop.io.compress.DefaultCodec";

    String usage = "TestCodec [-count N] [-codec <codec class>]";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i=0; i < args.length; ++i) {       // parse command line
      if (args[i] == null) {
        continue;
      } else if (args[i].equals("-count")) {
        count = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-codec")) {
        codecClass = args[++i];
      }
    }

    Configuration conf = new Configuration();
    int seed = 0;
    // Note that exceptions will propagate out.
    codecTest(conf, seed, count, codecClass);
  }

  @Test
  public void testGzipCompatibility() throws IOException {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info("seed: " + seed);

    DataOutputBuffer dflbuf = new DataOutputBuffer();
    GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);
    byte[] b = new byte[r.nextInt(128 * 1024 + 1)];
    r.nextBytes(b);
    gzout.write(b);
    gzout.close();

    DataInputBuffer gzbuf = new DataInputBuffer();
    gzbuf.reset(dflbuf.getData(), dflbuf.getLength());

    Configuration conf = new Configuration();
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);
    Decompressor decom = codec.createDecompressor();
    assertNotNull(decom);
    assertEquals(BuiltInGzipDecompressor.class, decom.getClass());
    InputStream gzin = codec.createInputStream(gzbuf, decom);

    dflbuf.reset();
    IOUtils.copyBytes(gzin, dflbuf, 4096);
    final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
    assertArrayEquals(b, dflchk);
  }

  @Test
  public void testGzipCompatibilityWithCompressor() throws IOException {
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    Configuration hadoopConf = new Configuration();
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, hadoopConf);
    Random r = new Random();

    for (int i = 0; i < 100; i++){
      Compressor compressor = codec.createCompressor();
      assertThat(compressor).withFailMessage("should be BuiltInGzipCompressor")
        .isInstanceOf(BuiltInGzipCompressor.class);

      long randonSeed = r.nextLong();
      r.setSeed(randonSeed);
      LOG.info("seed: {}", randonSeed);

      int inputSize = r.nextInt(256 * 1024 + 1);
      byte[] b = new byte[inputSize];
      r.nextBytes(b);

      compressor.setInput(b, 0, b.length);
      compressor.finish();

      byte[] output = new byte[inputSize + 1024];
      int outputOff = 0;

      while (!compressor.finished()) {
        byte[] buf = new byte[r.nextInt(1024)];
        int compressed = compressor.compress(buf, 0, buf.length);
        System.arraycopy(buf, 0, output, outputOff, compressed);
        outputOff += compressed;
      }

      DataInputBuffer gzbuf = new DataInputBuffer();
      gzbuf.reset(output, outputOff);

      Decompressor decom = codec.createDecompressor();
      assertThat(decom).as("decompressor should not be null").isNotNull();
      assertThat(decom).withFailMessage("should be BuiltInGzipDecompressor")
        .isInstanceOf(BuiltInGzipDecompressor.class);
      try (InputStream gzin = codec.createInputStream(gzbuf, decom);
           DataOutputBuffer dflbuf = new DataOutputBuffer()) {
        dflbuf.reset();
        IOUtils.copyBytes(gzin, dflbuf, 4096);
        final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
        assertThat(b).as("check decompressed output").isEqualTo(dflchk);
      }
    }
  }

  @Test
  public void testGzipCompatibilityWithCompressorAndGZIPOutputStream() throws IOException {
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    Configuration hadoopConf = new Configuration();
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, hadoopConf);
    Random r = new Random();

    for (int i = 0; i < 100; i++){
      Compressor compressor = codec.createCompressor();
      assertThat(compressor).withFailMessage("should be BuiltInGzipCompressor")
        .isInstanceOf(BuiltInGzipCompressor.class);

      long randonSeed = r.nextLong();
      r.setSeed(randonSeed);
      LOG.info("seed: {}", randonSeed);

      int inputSize = r.nextInt(256 * 1024 + 1);
      byte[] b = new byte[inputSize];
      r.nextBytes(b);

      compressor.setInput(b, 0,  b.length);
      compressor.finish();

      byte[] output = new byte[inputSize + 1024];
      int outputOff = 0;

      while (!compressor.finished()) {
        byte[] buf = new byte[r.nextInt(1024)];
        int compressed = compressor.compress(buf, 0, buf.length);
        System.arraycopy(buf, 0, output, outputOff, compressed);
        outputOff += compressed;
      }

      try (DataOutputBuffer dflbuf = new DataOutputBuffer();
           GZIPOutputStream gzout = new GZIPOutputStream(dflbuf)) {
        gzout.write(b);
        gzout.close();

        final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
        LOG.info("output: {}", outputOff);
        LOG.info("dflchk: {}", dflchk.length);

        assertEquals(outputOff, dflchk.length);

        uncompressGzipOutput(b, output, outputOff, codec);
        uncompressGzipOutput(b, dflchk, dflchk.length, codec);
      }
    }
  }

  @Test
  public void testGzipCompatibilityWithCompressorStreamAndGZIPOutputStream() throws IOException {
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    Configuration hadoopConf = new Configuration();
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, hadoopConf);
    Random r = new Random();

    for (int i = 0; i < 100; i++){
      Compressor compressor = codec.createCompressor();
      try (DataOutputBuffer dflbuf = new DataOutputBuffer();) {
        assertThat(compressor).withFailMessage("should be BuiltInGzipCompressor")
          .isInstanceOf(BuiltInGzipCompressor.class);
        CompressionOutputStream compressionOutputStream =
            codec.createOutputStream(dflbuf, compressor);

        long randonSeed = r.nextLong();
        r.setSeed(randonSeed);
        LOG.info("seed: {}", randonSeed);

        int inputSize = r.nextInt(256 * 1024 + 1);
        byte[] b = new byte[inputSize];
        r.nextBytes(b);

        compressionOutputStream.write(b);
        compressionOutputStream.close();

        final byte[] output = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
        dflbuf.reset();

        try (GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);) {
          gzout.write(b);
          gzout.close();

          final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
          LOG.info("output: {}", output.length);
          LOG.info("dflchk: {}", dflchk.length);

          assertThat(output.length).as("check compressed data length").isEqualTo(dflchk.length);

          uncompressGzipOutput(b, output, output.length, codec);
          uncompressGzipOutput(b, dflchk, dflchk.length, codec);
        }
      }
    }
  }

  private void uncompressGzipOutput(
          byte[] origin, byte[] output, int outputLen, CompressionCodec codec) throws IOException {
    DataInputBuffer gzbuf = new DataInputBuffer();
    gzbuf.reset(output, outputLen);

    Decompressor decom = codec.createDecompressor();
    assertThat(decom).as("decompressor should not be null").isNotNull();
    assertThat(decom).withFailMessage("should be BuiltInGzipDecompressor")
      .isInstanceOf(BuiltInGzipDecompressor.class);
    InputStream gzin = codec.createInputStream(gzbuf, decom);

    DataOutputBuffer dflbuf = new DataOutputBuffer();
    dflbuf.reset();
    IOUtils.copyBytes(gzin, dflbuf, 4096);
    final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
    assertThat(origin).as("check decompressed output").isEqualTo(dflchk);
  }

  void GzipConcatTest(Configuration conf,
      Class<? extends Decompressor> decomClass) throws IOException {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    LOG.info(decomClass + " seed: " + seed);

    final int CONCAT = r.nextInt(4) + 3;
    final int BUFLEN = 128 * 1024;
    DataOutputBuffer dflbuf = new DataOutputBuffer();
    DataOutputBuffer chkbuf = new DataOutputBuffer();
    byte[] b = new byte[BUFLEN];
    for (int i = 0; i < CONCAT; ++i) {
      GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);
      r.nextBytes(b);
      int len = r.nextInt(BUFLEN);
      int off = r.nextInt(BUFLEN - len);
      chkbuf.write(b, off, len);
      gzout.write(b, off, len);
      gzout.close();
    }
    final byte[] chk = Arrays.copyOf(chkbuf.getData(), chkbuf.getLength());

    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);
    Decompressor decom = codec.createDecompressor();
    assertNotNull(decom);
    assertEquals(decomClass, decom.getClass());
    DataInputBuffer gzbuf = new DataInputBuffer();
    gzbuf.reset(dflbuf.getData(), dflbuf.getLength());
    InputStream gzin = codec.createInputStream(gzbuf, decom);

    dflbuf.reset();
    IOUtils.copyBytes(gzin, dflbuf, 4096);
    final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
    assertArrayEquals(chk, dflchk);
  }

  @Test
  public void testBuiltInGzipConcat() throws IOException {
    Configuration conf = new Configuration();
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    GzipConcatTest(conf, BuiltInGzipDecompressor.class);
  }

  @Test
  public void testNativeGzipConcat() throws IOException {
    Configuration conf = new Configuration();
    assumeTrue(ZlibFactory.isNativeZlibLoaded(conf));
    GzipConcatTest(conf, GzipCodec.GzipZlibDecompressor.class);
  }

  @Test
  public void testGzipCodecRead() throws IOException {
    // Create a gzipped file and try to read it back, using a decompressor
    // from the CodecPool.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    ZlibFactory.setNativeZlibLoaded(false);
    // Ensure that the CodecPool has a BuiltInZlibInflater in it.
    Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
    assertNotNull(zlibDecompressor, "zlibDecompressor is null!");
    assertTrue(zlibDecompressor instanceof BuiltInZlibInflater,
        "ZlibFactory returned unexpected inflator");
    CodecPool.returnDecompressor(zlibDecompressor);

    // Now create a GZip text file.
    Path f = new Path(GenericTestUtils.getTempPath("testGzipCodecRead.txt.gz"));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
      new GZIPOutputStream(new FileOutputStream(f.toString()))));
    final String msg = "This is the message in the file!";
    bw.write(msg);
    bw.close();

    // Now read it back, using the CodecPool to establish the
    // decompressor to use.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(f);
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    FileSystem fs = FileSystem.getLocal(conf);
    InputStream is = fs.open(f);
    is = codec.createInputStream(is, decompressor);
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line = br.readLine();
    assertEquals(msg, line, "Didn't get the same message back!");
    br.close();
  }

  private void verifyGzipFile(String filename, String msg) throws IOException {
    BufferedReader r = new BufferedReader(new InputStreamReader(
        new GZIPInputStream(new FileInputStream(filename))));
    try {
      String line = r.readLine();
      assertEquals(msg, line, "Got invalid line back from " + filename);
    } finally {
      r.close();
      new File(filename).delete();
    }
  }

  @Test
  public void testGzipLongOverflow() throws IOException {
    LOG.info("testGzipLongOverflow");

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    ZlibFactory.setNativeZlibLoaded(false);
    assertFalse(ZlibFactory.isNativeZlibLoaded(conf),
        "ZlibFactory is using native libs against request");

    // Ensure that the CodecPool has a BuiltInZlibInflater in it.
    Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
    assertNotNull(zlibDecompressor, "zlibDecompressor is null!");
    assertTrue(zlibDecompressor instanceof BuiltInZlibInflater,
        "ZlibFactory returned unexpected inflator");
    CodecPool.returnDecompressor(zlibDecompressor);

    // Now create a GZip text file.
    Path f = new Path(GenericTestUtils.getTempPath("testGzipLongOverflow.bin.gz"));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
      new GZIPOutputStream(new FileOutputStream(f.toString()))));

    final int NBUF = 1024 * 4 + 1;
    final char[] buf = new char[1024 * 1024];
    for (int i = 0; i < buf.length; i++) buf[i] = '\0';
    for (int i = 0; i < NBUF; i++) {
      bw.write(buf);
    }
    bw.close();

    // Now read it back, using the CodecPool to establish the
    // decompressor to use.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(f);
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    FileSystem fs = FileSystem.getLocal(conf);
    InputStream is = fs.open(f);
    is = codec.createInputStream(is, decompressor);
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    for (int j = 0; j < NBUF; j++) {
      int n = br.read(buf);
      assertEquals(n, buf.length, "got wrong read length!");
      for (int i = 0; i < buf.length; i++)
        assertEquals(buf[i], '\0', "got wrong byte!");
    }
    br.close();
  }

  private void testGzipCodecWrite(boolean useNative) throws IOException {
    // Create a gzipped file using a compressor from the CodecPool,
    // and try to read it back via the regular GZIPInputStream.

    // Use native libs per the parameter
    Configuration hadoopConf = new Configuration();
    if (useNative) {
      assumeTrue(ZlibFactory.isNativeZlibLoaded(hadoopConf));
    } else {
      assertFalse(ZlibFactory.isNativeZlibLoaded(hadoopConf),
          "ZlibFactory is using native libs against request");
    }

    // Ensure that the CodecPool has a BuiltInZlibDeflater in it.
    Compressor zlibCompressor = ZlibFactory.getZlibCompressor(hadoopConf);
    assertNotNull(zlibCompressor, "zlibCompressor is null!");
    assertTrue(useNative ? zlibCompressor instanceof ZlibCompressor
        : zlibCompressor instanceof BuiltInZlibDeflater,
        "ZlibFactory returned unexpected deflator");

    CodecPool.returnCompressor(zlibCompressor);

    // Create a GZIP text file via the Compressor interface.
    CompressionCodecFactory ccf = new CompressionCodecFactory(hadoopConf);
    CompressionCodec codec = ccf.getCodec(new Path("foo.gz"));
    assertTrue(codec instanceof GzipCodec,
        "Codec for .gz file is not GzipCodec");

    final String fileName = new Path(GenericTestUtils.getTempPath(
        "testGzipCodecWrite.txt.gz")).toString();

    BufferedWriter w = null;
    Compressor gzipCompressor = CodecPool.getCompressor(codec);
    // When it gives us back a Compressor, we should be able to use this
    // to write files we can then read back with Java's gzip tools.
    OutputStream os = new CompressorStream(new FileOutputStream(fileName),
        gzipCompressor);

    // Call `write` multiple times.
    int bufferSize = 10000;
    char[] inputBuffer = new char[bufferSize];
    Random rand = new Random();
    for (int i = 0; i < bufferSize; i++) {
      inputBuffer[i] = (char) ('a' + rand.nextInt(26));
    }
    w = new BufferedWriter(new OutputStreamWriter(os), bufferSize);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      w.write(inputBuffer);
      sb.append(inputBuffer);
    }
    w.close();
    CodecPool.returnCompressor(gzipCompressor);
    verifyGzipFile(fileName, sb.toString());


    // Create a gzip text file via codec.getOutputStream().
    w = new BufferedWriter(new OutputStreamWriter(
            codec.createOutputStream(new FileOutputStream(fileName))));
    for (int i = 0; i < 10; i++) {
      w.write(inputBuffer);
    }
    w.close();
    verifyGzipFile(fileName, sb.toString());
  }

  @Test
  public void testGzipCodecWriteJava() throws IOException {
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    testGzipCodecWrite(false);
  }

  @Test
  public void testGzipNativeCodecWrite() throws IOException {
    testGzipCodecWrite(true);
  }

  @Test
  public void testCodecPoolAndGzipCompressor() {
    // BuiltInZlibDeflater should not be used as the GzipCodec compressor.
    // Assert that this is the case.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    ZlibFactory.setNativeZlibLoaded(false);
    assertFalse(ZlibFactory.isNativeZlibLoaded(conf),
        "ZlibFactory is using native libs against request");

    // This should give us a BuiltInZlibDeflater.
    Compressor zlibCompressor = ZlibFactory.getZlibCompressor(conf);
    assertNotNull(zlibCompressor, "zlibCompressor is null!");
    assertTrue(zlibCompressor instanceof BuiltInZlibDeflater,
        "ZlibFactory returned unexpected deflator");
    // its createOutputStream() just wraps the existing stream in a
    // java.util.zip.GZIPOutputStream.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(new Path("foo.gz"));
    assertTrue(codec instanceof GzipCodec,
        "Codec for .gz file is not GzipCodec");

    // make sure we don't get a null compressor
    Compressor codecCompressor = codec.createCompressor();
    if (null == codecCompressor) {
      fail("Got null codecCompressor");
    }

    // Asking the CodecPool for a compressor for GzipCodec
    // should not return null
    Compressor poolCompressor = CodecPool.getCompressor(codec);
    if (null == poolCompressor) {
      fail("Got null poolCompressor");
    }
    // return a couple compressors
    CodecPool.returnCompressor(zlibCompressor);
    CodecPool.returnCompressor(poolCompressor);
    Compressor poolCompressor2 = CodecPool.getCompressor(codec);
    if (poolCompressor.getClass() == BuiltInGzipCompressor.class) {
      if (poolCompressor == poolCompressor2) {
        fail("Reused java gzip compressor in pool");
      }
    } else {
      if (poolCompressor != poolCompressor2) {
        fail("Did not reuse native gzip compressor in pool");
      }
    }
  }

  @Test
  public void testCodecPoolAndGzipDecompressor() {
    // BuiltInZlibInflater should not be used as the GzipCodec decompressor.
    // Assert that this is the case.

    // Don't use native libs for this test.
    Configuration conf = new Configuration();
    ZlibFactory.setNativeZlibLoaded(false);
    assertFalse(ZlibFactory.isNativeZlibLoaded(conf),
        "ZlibFactory is using native libs against request");

    // This should give us a BuiltInZlibInflater.
    Decompressor zlibDecompressor = ZlibFactory.getZlibDecompressor(conf);
    assertNotNull(zlibDecompressor, "zlibDecompressor is null!");
    assertTrue(zlibDecompressor instanceof BuiltInZlibInflater,
        "ZlibFactory returned unexpected inflator");
    // its createOutputStream() just wraps the existing stream in a
    // java.util.zip.GZIPOutputStream.
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(new Path("foo.gz"));
    assertTrue(codec instanceof GzipCodec,
        "Codec for .gz file is not GzipCodec");

    // make sure we don't get a null decompressor
    Decompressor codecDecompressor = codec.createDecompressor();
    if (null == codecDecompressor) {
      fail("Got null codecDecompressor");
    }

    // Asking the CodecPool for a decompressor for GzipCodec
    // should not return null
    Decompressor poolDecompressor = CodecPool.getDecompressor(codec);
    if (null == poolDecompressor) {
      fail("Got null poolDecompressor");
    }
    // return a couple decompressors
    CodecPool.returnDecompressor(zlibDecompressor);
    CodecPool.returnDecompressor(poolDecompressor);
    Decompressor poolDecompressor2 = CodecPool.getDecompressor(codec);
    if (poolDecompressor.getClass() == BuiltInGzipDecompressor.class) {
      if (poolDecompressor == poolDecompressor2) {
        fail("Reused java gzip decompressor in pool");
      }
    } else {
      if (poolDecompressor != poolDecompressor2) {
        fail("Did not reuse native gzip decompressor in pool");
      }
    }
  }

  @Test
  @Timeout(value = 20)
  public void testGzipCompressorWithEmptyInput() throws IOException {
    // don't use native libs
    ZlibFactory.setNativeZlibLoaded(false);
    Configuration conf = new Configuration();
    CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);

    Compressor compressor = codec.createCompressor();
    assertThat(compressor).withFailMessage("should be BuiltInGzipCompressor")
            .isInstanceOf(BuiltInGzipCompressor.class);

    byte[] b = new byte[0];
    compressor.setInput(b, 0, b.length);
    compressor.finish();

    byte[] output = new byte[100];
    int outputOff = 0;

    while (!compressor.finished()) {
      byte[] buf = new byte[100];
      int compressed = compressor.compress(buf, 0, buf.length);
      System.arraycopy(buf, 0, output, outputOff, compressed);
      outputOff += compressed;
    }

    DataInputBuffer gzbuf = new DataInputBuffer();
    gzbuf.reset(output, outputOff);

    Decompressor decom = codec.createDecompressor();
    assertThat(decom).as("decompressor should not be null").isNotNull();
    assertThat(decom).withFailMessage("should be BuiltInGzipDecompressor")
            .isInstanceOf(BuiltInGzipDecompressor.class);
    try (InputStream gzin = codec.createInputStream(gzbuf, decom);
         DataOutputBuffer dflbuf = new DataOutputBuffer()) {
      dflbuf.reset();
      IOUtils.copyBytes(gzin, dflbuf, 4096);
      final byte[] dflchk = Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
      assertThat(b).as("check decompressed output").isEqualTo(dflchk);
    }
  }
}
