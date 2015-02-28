/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/**
 * InMemoryObjectEntry represents a GCS StorageObject in-memory by maintaining byte[] contents
 * alongside channels and metadata allowing interaction with the data in a similar way to the
 * equivalent GCS API calls. Intended to be used only internally by the InMemoryGoogleCloudStorage
 * class.
 */
class InMemoryObjectEntry {
  // This will become non-null only once the associated writeStream has been closed.
  private byte[] completedContents = null;

  // At creation time, this is initialized to act as the writable data buffer underlying the
  // WritableByteChannel callers use to populate this ObjectEntry's data. This is not exposed
  // outside of this ObjectEntry, and serves only to provide a way to "commit" the underlying
  // data buffer into a byte[] when the externally exposed channel is closed.
  private ByteArrayOutputStream writeStream;

  // This is the externally exposed handle for populating the byte data of this ObjectEntry,
  // which simply wraps the internal ByteArrayOutputStream since the GoogleCloudStorage
  // interface uses WritableByteChannels for writing.
  private WritableByteChannel writeChannel;

  // The metadata associated with this ObjectEntry;
  private GoogleCloudStorageItemInfo info;

  public InMemoryObjectEntry(String bucketName, String objectName, long createTimeMillis,
      Map<String, byte[]> metadata) {
    // Override close() to commit its completed byte array into completedContents to reflect
    // the behavior that any readable contents are only well-defined if the writeStream is closed.
    writeStream = new ByteArrayOutputStream() {
      @Override
      public synchronized void close() throws IOException {
        synchronized (InMemoryObjectEntry.this) {
          completedContents = toByteArray();
          writeStream = null;
          writeChannel = null;
          info = new GoogleCloudStorageItemInfo(
              info.getResourceId(),
              info.getCreationTime(),
              completedContents.length,
              null,
              null,
              info.getMetadata(),
              0L,
              0L);
        }
      }
    };

    // We have to delegate because we can't extend from the inner class returned by,
    // Channels.newChannel, and the default version doesn't enforce ClosedChannelException
    // when trying to write to a closed channel; probably because it relies on the underlying
    // output stream throwing when closed. The ByteArrayOutputStream doesn't enforce throwing
    // when closed, so we have to manually do it.
    final WritableByteChannel delegateWriteChannel = Channels.newChannel(writeStream);
    writeChannel = new WritableByteChannel() {
      @Override
      public int write(ByteBuffer src) throws IOException {
        if (!isOpen()) {
          throw new ClosedChannelException();
        }
        return delegateWriteChannel.write(src);
      }

      @Override
      public void close() throws IOException {
        delegateWriteChannel.close();
      }

      @Override
      public boolean isOpen() {
        return delegateWriteChannel.isOpen();
      }
    };
    // Size 0 initially because this object exists, but contains no data.
    info = new GoogleCloudStorageItemInfo(
        new StorageResourceId(bucketName, objectName),
        createTimeMillis,
        0,
        null,
        null,
        ImmutableMap.copyOf(metadata),
        0L,
        0L);
  }

  /**
   * For internal use in getShallowCopy(2).
   */
  private InMemoryObjectEntry() {
  }

  /**
   * Returns true if the initial WritableByteChannel associated with this InMemoryObjectEntry has
   * been closed. If false, it is illegal to read or get a copy of this InMemoryObjectEntry. If
   * true, there is no longer any way to write/modify its byte contents.
   */
  private synchronized boolean isCompleted() {
    return completedContents != null;
  }

  /**
   * Returns the objectName initially provided at construction-time of this InMemoryObjectEntry; it
   * will never change over the lifetime of this InMemoryObjectEntry.
   */
  public synchronized String getObjectName() {
    return info.getObjectName();
  }

  /**
   * Returns the bucketName initially provided at construction-time of this InMemoryObjectEntry; it
   * will never change over the lifetime of this InMemoryObjectEntry.
   */
  public synchronized String getBucketName() {
    return info.getBucketName();
  }

  /**
   * Returns a copy of this InMemoryObjectEntry which shares the underlying completedContents data;
   * it is illegal to call this method if the write channel has not yet been closed.
   */
  public synchronized InMemoryObjectEntry getShallowCopy(
      String bucketName, String objectName) throws IOException {
    if (!isCompleted()) {
      throw new IOException("Cannot getShallowCopy() before the writeChannel has been closed!");
    }
    InMemoryObjectEntry copy = new InMemoryObjectEntry();
    copy.completedContents = completedContents;
    copy.writeStream = writeStream;
    copy.writeChannel = writeChannel;
    // TODO(user): Check if the creation time should change too, add copy constructor.
    copy.info = new GoogleCloudStorageItemInfo(
        new StorageResourceId(bucketName, objectName),
        info.getCreationTime(),
        info.getSize(),
        null,
        null,
        info.getMetadata(),
        0L,
        0L);
    return copy;
  }

  /**
   * Returns a WritableByteChannel for this InMemoryObjectEntry's byte contents; the same channel is
   * returned on each call, and it is illegal to call this method if the channel has ever been
   * closed already.
   */
  public synchronized WritableByteChannel getWriteChannel() throws IOException {
    if (isCompleted()) {
      throw new IOException("Cannot getWriteChannel() once it has already been closed!");
    }
    return writeChannel;
  }

  /**
   * Returns a SeekableReadableByteChannel pointing at this InMemoryObjectEntry's byte contents;
   * a previous writer must have already closed the associated WritableByteChannel to commit
   * the byte contents and make them available for reading.
   */
  public synchronized SeekableReadableByteChannel getReadChannel() throws IOException {
    if (!isCompleted()) {
      throw new IOException(
          String.format("Cannot getReadChannel() before writes have been committed! Object = %s",
              this.getObjectName()));
    }
    return new InMemoryObjectReadChannel(completedContents);
  }

  /**
   * Gets the {@code GoogleCloudStorageItemInfo} associated with this InMemoryObjectEntry, whose
   * 'size' is only updated when the initial writer has finished closing the channel.
   */
  public synchronized GoogleCloudStorageItemInfo getInfo() throws IOException {
    if (!isCompleted()) {
      throw new IOException(
          String.format(
              "Cannot getInfo() before writes have been committed! Object = %s",
              this.getObjectName()));
    }
    return info;
  }

  /**
   * Updates the metadata associated with this InMemoryObjectEntry. Any key in newMetadata which
   * has a corresponding null value will be removed from the object's metadata. All other values
   * will be added.
   */
  public synchronized void patchMetadata(Map<String, byte[]> newMetadata) throws IOException {
    if (!isCompleted()) {
      throw new IOException(
          String.format(
              "Cannot patchMetadata() before writes have been committed! Object = %s",
              this.getObjectName()));
    }
    Map<String, byte[]> mergedMetadata = Maps.newHashMap(info.getMetadata());

    for (Map.Entry<String, byte[]> entry : newMetadata.entrySet()) {
      if (entry.getValue() == null && mergedMetadata.containsKey(entry.getKey())) {
        mergedMetadata.remove(entry.getKey());
      } else if (entry.getValue() != null) {
        mergedMetadata.put(entry.getKey(), entry.getValue());
      }
    }

    info = new GoogleCloudStorageItemInfo(
        info.getResourceId(),
        info.getCreationTime(),
        completedContents.length,
        null,
        null,
        mergedMetadata,
        0L,
        0L);
  }
}
