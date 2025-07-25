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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COMMA;

/**
 * Manages Azure Blob blocks for append operations.
 */
public class AzureBlobBlockManager extends AzureBlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  /** Cached list of committed block IDs */
  private final StringBuilder committedBlockEntries = new StringBuilder();

  /** The list to store blockId, position, and status. */
  private final LinkedList<BlockEntry> blockEntryList = new LinkedList<>();

  private int blockIdLength = 0;

  /**
   * Constructs an AzureBlobBlockManager.
   *
   * @param abfsOutputStream the output stream
   * @param blockFactory the block factory
   * @param bufferSize the buffer size
   * @throws AzureBlobFileSystemException if an error occurs
   */
  public AzureBlobBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize)
      throws AzureBlobFileSystemException {
    super(abfsOutputStream, blockFactory, bufferSize);
    if (abfsOutputStream.getPosition() > 0 && !abfsOutputStream.isAppendBlob()) {
      List<String> committedBlocks = getBlockList(abfsOutputStream.getTracingContext());
      if (!committedBlocks.isEmpty()) {
        committedBlockEntries.append(String.join(COMMA, committedBlocks));
      }
    }
    LOG.debug("Created a new Blob Block Manager for AbfsOutputStream instance {} for path {}",
        abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
  }

  /**
   * Retrieves the length of the block ID.
   *
   * @return the length of the block ID in bytes.
   */
  public int getBlockIdLength() {
    return blockIdLength;
  }

  /**
   * Creates a new block.
   *
   * @param position the position
   * @return the created block
   * @throws IOException if an I/O error occurs
   */
  @Override
  protected synchronized AbfsBlock createBlockInternal(long position)
      throws IOException {
    if (getActiveBlock() == null) {
      setBlockCount(getBlockCount() + 1);
      AbfsBlock activeBlock = new AbfsBlobBlock(getAbfsOutputStream(), position, getBlockIdLength(), getBlockCount());
      activeBlock.setBlockEntry(addNewEntry(activeBlock.getBlockId(), activeBlock.getOffset()));
      getAbfsOutputStream().getMessageDigest().reset();
      setActiveBlock(activeBlock);
    }
    return getActiveBlock();
  }

  /**
   * Returns block id's which are committed for the blob.
   *
   * @param tracingContext Tracing context object.
   * @return list of committed block id's.
   * @throws AzureBlobFileSystemException if an error occurs
   */
  private List<String> getBlockList(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    List<String> committedBlockIdList = new ArrayList<>();
    AbfsBlobClient blobClient = getAbfsOutputStream().getClientHandler().getBlobClient();
    final AbfsRestOperation op = blobClient
        .getBlockList(getAbfsOutputStream().getPath(), tracingContext);
    if (op != null && op.getResult() != null) {
      committedBlockIdList = op.getResult().getBlockIdList();
      if (!committedBlockIdList.isEmpty()) {
        blockIdLength = Base64.decodeBase64(committedBlockIdList.get(0)).length;
      }
    }
    return committedBlockIdList;
  }

  /**
   * Adds a new block entry to the block entry list.
   * The block entry is added only if the position of the new block
   * is greater than the position of the last block in the list.
   *
   * @param blockId The ID of the new block to be added.
   * @param position The position of the new block in the stream.
   * @return The newly added {@link BlockEntry}.
   * @throws IOException If the position of the new block is not greater than the last block in the list.
   */
  private synchronized BlockEntry addNewEntry(String blockId, long position) throws IOException {
    if (!blockEntryList.isEmpty()) {
      BlockEntry lastEntry = blockEntryList.getLast();
      if (position <= lastEntry.getPosition()) {
        throw new IOException("New block position " + position  + " must be greater than the last block position "
            + lastEntry.getPosition() + " for path " + getAbfsOutputStream().getPath());
      }
    }
    BlockEntry blockEntry = new BlockEntry(blockId, position, AbfsBlockStatus.NEW);
    blockEntryList.addLast(blockEntry);
    LOG.debug("Added block {} at position {} with status NEW.", blockId, position);
    return blockEntry;
  }

  /**
   * Updates the status of an existing block entry to SUCCESS.
   * This method is used to mark a block as successfully processed.
   *
   * @param block The {@link AbfsBlock} whose status needs to be updated to SUCCESS.
   */
  protected synchronized void updateEntry(AbfsBlock block) {
    BlockEntry blockEntry = block.getBlockEntry();
    blockEntry.setStatus(AbfsBlockStatus.SUCCESS);
    LOG.debug("Added block {} at position {} with status SUCCESS.", block.getBlockId(), blockEntry.getPosition());
  }

  /**
   * Prepares the list of blocks to commit.
   *
   * @return whether we have some data to commit or not.
   * @throws IOException if an I/O error occurs
   */
  protected synchronized boolean hasBlocksToCommit() throws IOException {
    // Adds all the committed blocks if available to the list of blocks to be added in putBlockList.
    if (blockEntryList.isEmpty()) {
      return false; // No entries to commit
    }

    while (!blockEntryList.isEmpty()) {
      BlockEntry current = blockEntryList.poll();
      if (current.getStatus() != AbfsBlockStatus.SUCCESS) {
        LOG.debug(
            "Block {} with position {} has status {}, flush cannot proceed.",
            current.getBlockId(), current.getPosition(), current.getStatus());
        throw new IOException("Flush failed. Block " + current.getBlockId()
            + " with position " + current.getPosition() + " has status "
            + current.getStatus() + "for path " + getAbfsOutputStream().getPath());
      }
      if (!blockEntryList.isEmpty()) {
        BlockEntry next = blockEntryList.getFirst();
        if (current.getPosition() >= next.getPosition()) {
          String errorMessage =
              "Position check failed. Current block position is greater than or equal to the next block's position.\n"
                  + "Current Block Entry:\n"
                  + "Block ID: " + current.getBlockId()
                  + ", Position: " + current.getPosition()
                  + ", Status: " + current.getStatus()
                  + ", Path: " + getAbfsOutputStream().getPath()
                  + ", StreamID: " + getAbfsOutputStream().getStreamID()
                  + ", Next block position: " + next.getPosition()
                  + "\n";
          throw new IOException(errorMessage);
        }
      }
      // Append the current block's ID to the committedBlockBuilder
      if (committedBlockEntries.length() > 0) {
        committedBlockEntries.append(COMMA);
      }
      committedBlockEntries.append(current.getBlockId());
      LOG.debug("Block {} added to committed entries.", current.getBlockId());
    }
    return true;
  }

  /**
   * Returns the block ID list.
   *
   * @return the block ID list
   */
  protected String getBlockIdToCommit() {
    return committedBlockEntries.toString();
  }

  @Override
  public void close(){
    super.close();
    committedBlockEntries.setLength(0);
  }
}
