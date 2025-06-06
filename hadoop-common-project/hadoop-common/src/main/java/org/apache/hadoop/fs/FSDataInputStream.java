/*
 * 
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
package org.apache.hadoop.fs;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.util.IdentityHashStore;

/** Utility that wraps a {@link FSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link java.io.BufferedInputStream}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataInputStream extends DataInputStream
    implements Seekable, PositionedReadable, 
      ByteBufferReadable, HasFileDescriptor, CanSetDropBehind, CanSetReadahead,
      HasEnhancedByteBufferAccess, CanUnbuffer, StreamCapabilities,
      ByteBufferPositionedReadable, IOStatisticsSource {
  /**
   * Map ByteBuffers that we have handed out to readers to ByteBufferPool 
   * objects
   */
  private final IdentityHashStore<ByteBuffer, ByteBufferPool>
    extendedReadBuffers
      = new IdentityHashStore<>(0);

  public FSDataInputStream(InputStream in) {
    super(in);
    if( !(in instanceof Seekable) || !(in instanceof PositionedReadable) ) {
      throw new IllegalArgumentException(in.getClass().getCanonicalName() +
          " is not an instance of Seekable or PositionedReadable");
    }
  }
  
  /**
   * Seek to the given offset.
   *
   * @param desired offset to seek to
   */
  @Override
  public void seek(long desired) throws IOException {
    ((Seekable)in).seek(desired);
  }

  /**
   * Get the current position in the input stream.
   *
   * @return current position in the input stream
   */
  @Override
  public long getPos() throws IOException {
    return ((Seekable)in).getPos();
  }
  
  /**
   * Read bytes from the given position in the stream to the given buffer.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    maximum number of bytes to read
   * @return total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the stream has been
   *         reached
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    return ((PositionedReadable)in).read(position, buffer, offset, length);
  }

  /**
   * Read bytes from the given position in the stream to the given buffer.
   * Continues to read until <code>length</code> bytes have been read.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    the number of bytes to read
   * @throws IOException IO problems
   * @throws EOFException If the end of stream is reached while reading.
   *                      If an exception is thrown an undetermined number
   *                      of bytes in the buffer may have been written. 
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    ((PositionedReadable)in).readFully(position, buffer, offset, length);
  }
  
  /**
   * See {@link #readFully(long, byte[], int, int)}.
   */
  @Override
  public void readFully(long position, byte[] buffer)
    throws IOException {
    ((PositionedReadable)in).readFully(position, buffer, 0, buffer.length);
  }
  
  /**
   * Seek to the given position on an alternate copy of the data.
   *
   * @param  targetPos  position to seek to
   * @return true if a new source is found, false otherwise
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return ((Seekable)in).seekToNewSource(targetPos); 
  }
  
  /**
   * Get a reference to the wrapped input stream. Used by unit tests.
   *
   * @return the underlying input stream
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public InputStream getWrappedStream() {
    return in;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (in instanceof ByteBufferReadable) {
      return ((ByteBufferReadable)in).read(buf);
    }

    throw new UnsupportedOperationException("Byte-buffer read unsupported " +
            "by " + in.getClass().getCanonicalName());
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    if (in instanceof HasFileDescriptor) {
      return ((HasFileDescriptor) in).getFileDescriptor();
    } else if (in instanceof FileInputStream) {
      return ((FileInputStream) in).getFD();
    } else {
      return null;
    }
  }

  @Override
  public void setReadahead(Long readahead)
      throws IOException, UnsupportedOperationException {
    try {
      ((CanSetReadahead)in).setReadahead(readahead);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException(in.getClass().getCanonicalName() +
          " does not support setting the readahead caching strategy.");
    }
  }

  @Override
  public void setDropBehind(Boolean dropBehind)
      throws IOException, UnsupportedOperationException {
    try {
      ((CanSetDropBehind)in).setDropBehind(dropBehind);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("this stream does not " +
          "support setting the drop-behind caching setting.");
    }
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength,
      EnumSet<ReadOption> opts) 
          throws IOException, UnsupportedOperationException {
    try {
      return ((HasEnhancedByteBufferAccess)in).read(bufferPool,
          maxLength, opts);
    }
    catch (ClassCastException e) {
      ByteBuffer buffer = ByteBufferUtil.
          fallbackRead(this, bufferPool, maxLength);
      if (buffer != null) {
        extendedReadBuffers.put(buffer, bufferPool);
      }
      return buffer;
    }
  }

  private static final EnumSet<ReadOption> EMPTY_READ_OPTIONS_SET =
      EnumSet.noneOf(ReadOption.class);

  final public ByteBuffer read(ByteBufferPool bufferPool, int maxLength)
          throws IOException, UnsupportedOperationException {
    return read(bufferPool, maxLength, EMPTY_READ_OPTIONS_SET);
  }
  
  @Override
  public void releaseBuffer(ByteBuffer buffer) {
    try {
      ((HasEnhancedByteBufferAccess)in).releaseBuffer(buffer);
    }
    catch (ClassCastException e) {
      ByteBufferPool bufferPool = extendedReadBuffers.remove( buffer);
      if (bufferPool == null) {
        throw new IllegalArgumentException("tried to release a buffer " +
            "that was not created by this stream.");
      }
      bufferPool.putBuffer(buffer);
    }
  }

  @Override
  public void unbuffer() {
    StreamCapabilitiesPolicy.unbuffer(in);
  }

  @Override
  public boolean hasCapability(String capability) {
    return StoreImplementationUtils.hasCapability(in, capability);
  }

  /**
   * String value. Includes the string value of the inner stream
   * @return the stream
   */
  @Override
  public String toString() {
    return super.toString() + ": " + in;
  }

  @Override
  public int read(long position, ByteBuffer buf) throws IOException {
    if (in instanceof ByteBufferPositionedReadable) {
      return ((ByteBufferPositionedReadable) in).read(position, buf);
    }
    throw new UnsupportedOperationException("Byte-buffer pread unsupported " +
        "by " + in.getClass().getCanonicalName());
  }

  /**
   * Delegate to the underlying stream.
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @throws IOException on a failure from the nested stream.
   * @throws UnsupportedOperationException if the inner stream does not
   * support this operation.
   */
  @Override
  public void readFully(long position, ByteBuffer buf) throws IOException {
    if (in instanceof ByteBufferPositionedReadable) {
      ((ByteBufferPositionedReadable) in).readFully(position, buf);
    } else {
      throw new UnsupportedOperationException("Byte-buffer pread " +
              "unsupported by " + in.getClass().getCanonicalName());
    }
  }

  /**
   * Get the IO Statistics of the nested stream, falling back to
   * null if the stream does not implement the interface
   * {@link IOStatisticsSource}.
   * @return an IOStatistics instance or null
   */
  @Override
  public IOStatistics getIOStatistics() {
    return IOStatisticsSupport.retrieveIOStatistics(in);
  }

  @Override
  public int minSeekForVectorReads() {
    return ((PositionedReadable) in).minSeekForVectorReads();
  }

  @Override
  public int maxReadSizeForVectorReads() {
    return ((PositionedReadable) in).maxReadSizeForVectorReads();
  }

  @Override
  public void readVectored(List<? extends FileRange> ranges,
                           IntFunction<ByteBuffer> allocate) throws IOException {
    ((PositionedReadable) in).readVectored(ranges, allocate);
  }

  @Override
  public void readVectored(final List<? extends FileRange> ranges,
      final IntFunction<ByteBuffer> allocate,
      final Consumer<ByteBuffer> release) throws IOException {
    ((PositionedReadable) in).readVectored(ranges, allocate, release);
  }
}
