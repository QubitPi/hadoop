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

package org.apache.hadoop.io;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;
import java.util.Random;


/** Unit tests for BoundedByteArrayOutputStream */
public class TestBoundedByteArrayOutputStream {

  private static final int SIZE = 1024;
  private static final byte[] INPUT = new byte[SIZE];
  static {
      new Random().nextBytes(INPUT);
  }

  @Test
  public void testBoundedStream() throws IOException {
    
    BoundedByteArrayOutputStream stream = 
      new BoundedByteArrayOutputStream(SIZE);

    // Write to the stream, get the data back and check for contents
    stream.write(INPUT, 0, SIZE);
    assertTrue(Arrays.equals(INPUT, stream.getBuffer()),
        "Array Contents Mismatch");
    
    // Try writing beyond end of buffer. Should throw an exception
    boolean caughtException = false;
    
    try {
      stream.write(INPUT[0]);
    } catch (Exception e) {
      caughtException = true;
    }
    
    assertTrue(caughtException,
        "Writing beyond limit did not throw an exception");
    
    //Reset the stream and try, should succeed 
    stream.reset();
    assertTrue((stream.getLimit() == SIZE),
        "Limit did not get reset correctly");
    stream.write(INPUT, 0, SIZE);
    assertTrue(Arrays.equals(INPUT, stream.getBuffer()),
        "Array Contents Mismatch");
  
    // Try writing one more byte, should fail
    caughtException = false;
    try {
      stream.write(INPUT[0]);
    } catch (Exception e) {
      caughtException = true;
    }
  
    // Reset the stream, but set a lower limit. Writing beyond
    // the limit should throw an exception
    stream.reset(SIZE - 1);
    assertTrue((stream.getLimit() == SIZE -1),
        "Limit did not get reset correctly");
    caughtException = false;
    
    try {
      stream.write(INPUT, 0, SIZE);
    } catch (Exception e) {
      caughtException = true;
    }
    
    assertTrue(caughtException,
        "Writing beyond limit did not throw an exception");
  }
  
  
  static class ResettableBoundedByteArrayOutputStream 
  extends BoundedByteArrayOutputStream {

    public ResettableBoundedByteArrayOutputStream(int capacity) {
      super(capacity);
    }

    public void resetBuffer(byte[] buf, int offset, int length) {
      super.resetBuffer(buf, offset, length);
    }
    
  }

  @Test
  public void testResetBuffer() throws IOException {
    
    ResettableBoundedByteArrayOutputStream stream = 
      new ResettableBoundedByteArrayOutputStream(SIZE);

    // Write to the stream, get the data back and check for contents
    stream.write(INPUT, 0, SIZE);
    assertTrue(Arrays.equals(INPUT, stream.getBuffer()),
        "Array Contents Mismatch");
    
    // Try writing beyond end of buffer. Should throw an exception
    boolean caughtException = false;
    
    try {
      stream.write(INPUT[0]);
    } catch (Exception e) {
      caughtException = true;
    }
    
    assertTrue(caughtException,
        "Writing beyond limit did not throw an exception");
    
    //Reset the stream and try, should succeed
    byte[] newBuf = new byte[SIZE];
    stream.resetBuffer(newBuf, 0, newBuf.length);
    assertTrue((stream.getLimit() == SIZE),
        "Limit did not get reset correctly");
    stream.write(INPUT, 0, SIZE);
    assertTrue(Arrays.equals(INPUT, stream.getBuffer()),
        "Array Contents Mismatch");
  
    // Try writing one more byte, should fail
    caughtException = false;
    try {
      stream.write(INPUT[0]);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException,
        "Writing beyond limit did not throw an exception");
  }

}
