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
package org.apache.hadoop.hdfs.util;

import static org.apache.hadoop.test.PlatformAssumptions.assumeWindows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

public class TestAtomicFileOutputStream {

  private static final String TEST_STRING = "hello world";
  private static final String TEST_STRING_2 = "goodbye world";

  private static final File TEST_DIR = PathUtils.getTestDir(TestAtomicFileOutputStream.class);
  
  private static final File DST_FILE = new File(TEST_DIR, "test.txt");
  
  @BeforeEach
  public void cleanupTestDir() throws IOException {
    assertTrue(TEST_DIR.exists() || TEST_DIR.mkdirs());
    FileUtil.fullyDeleteContents(TEST_DIR);
  }
  
  /**
   * Test case where there is no existing file
   */
  @Test
  public void testWriteNewFile() throws IOException {
    OutputStream fos = new AtomicFileOutputStream(DST_FILE);
    assertFalse(DST_FILE.exists());
    fos.write(TEST_STRING.getBytes());
    fos.flush();
    assertFalse(DST_FILE.exists());
    fos.close();
    assertTrue(DST_FILE.exists());
    
    String readBackData = DFSTestUtil.readFile(DST_FILE);
    assertEquals(TEST_STRING, readBackData);
  }

  /**
   * Test case where there is no existing file
   */
  @Test
  public void testOverwriteFile() throws IOException {
    assertTrue(DST_FILE.createNewFile(), "Creating empty dst file");
    
    OutputStream fos = new AtomicFileOutputStream(DST_FILE);
    
    assertTrue(DST_FILE.exists(), "Empty file still exists");
    fos.write(TEST_STRING.getBytes());
    fos.flush();
    
    // Original contents still in place
    assertEquals("", DFSTestUtil.readFile(DST_FILE));

    fos.close();

    // New contents replace original file
    String readBackData = DFSTestUtil.readFile(DST_FILE);
    assertEquals(TEST_STRING, readBackData);
  }
  
  /**
   * Test case where the flush() fails at close time - make sure
   * that we clean up after ourselves and don't touch any
   * existing file at the destination
   */
  @Test
  public void testFailToFlush() throws IOException {
    // Create a file at destination
    FileOutputStream fos = new FileOutputStream(DST_FILE);
    fos.write(TEST_STRING_2.getBytes());
    fos.close();
    
    OutputStream failingStream = createFailingStream();
    failingStream.write(TEST_STRING.getBytes());
    try {
      failingStream.close();
      fail("Close didn't throw exception");
    } catch (IOException ioe) {
      // expected
    }
    
    // Should not have touched original file
    assertEquals(TEST_STRING_2, DFSTestUtil.readFile(DST_FILE));

    assertEquals(DST_FILE.getName(), Joiner.on(",").join(TEST_DIR.list()),
        "Temporary file should have been cleaned up");
  }

  @Test
  public void testFailToRename() throws IOException {
    assumeWindows();
    OutputStream fos = null;
    try {
      fos = new AtomicFileOutputStream(DST_FILE);
      fos.write(TEST_STRING.getBytes());
      FileUtil.setWritable(TEST_DIR, false);
      final OutputStream toClose = fos;
      IOException ex = assertThrows(IOException.class, () -> {
        toClose.close();
      });
      assertTrue(ex.getMessage().contains("failure in native rename"));
      fos = null;
    } finally {
      IOUtils.cleanupWithLogger(null, fos);
      FileUtil.setWritable(TEST_DIR, true);
    }
  }

  /**
   * Create a stream that fails to flush at close time
   */
  private OutputStream createFailingStream() throws FileNotFoundException {
    return new AtomicFileOutputStream(DST_FILE) {
      @Override
      public void flush() throws IOException {
        throw new IOException("injected failure");
      }
    };
  }
}
