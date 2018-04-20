/**
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GoogleHadoopSyncableOutputStream implements the {@code Syncable} interface by composing
 * objects created in separate underlying streams for each hsync() call.
 * <p>
 * Prior to the first hsync(), sync() or close() call, this channel will behave the same way as a
 * basic non-syncable channel, writing directly to the destination file.
 * <p>
 * On the first call to hsync()/sync(), the destination file is committed and a new temporary file
 * using a hidden-file prefix (underscore) is created with an additional suffix which differs for
 * each subsequent temporary file in the series; during this time readers can read the data
 * committed to the destination file, but not the bytes written to the temporary file since the
 * last hsync() call.
 * <p>
 * On each subsequent hsync()/sync() call, the temporary file closed(), composed onto the
 * destination file, then deleted, and a new temporary file is opened under a new filename for
 * further writes.
 * <p>
 * Caveats:
 *   1. Each hsync()/sync() requires many underlying read and mutation requests occurring
 *      sequentially, so latency is expected to be fairly high.
 *   2. There is a hard limit to the number of times hsync()/sync() can be called due to the
 *      GCS-level limit on the number of components a composite object can contain (1024). Any
 *      attempt to hsync() more than this number of times will result in an IOException and
 *      any data written since the last hsync() should be considered lost (unless manually
 *      recovered as long as the temporary file wasn't deleted under the hood).
 * <p>
 * If errors occur mid-stream, there may be one or more temporary files failing to be cleaned up,
 * and require manual intervention to discover and delete any such unused files. Data written
 * prior to the most recent successful hsync() is persistent and safe in such a case.
 * <p>
 * If multiple writers are attempting to write to the same destination file, generation ids used
 * with low-level precondition checks will cause all but a one writer to fail their precondition
 * checks during writes, and a single remaining writer will safely occupy the stream.
 */
public class GoogleHadoopSyncableOutputStream extends OutputStream implements Syncable {
  // Prefix used for all temporary files created by this stream.
  public static final String TEMPFILE_PREFIX = "_GCS_SYNCABLE_TEMPFILE_";

  // Maximum number of components a composite object can have; any attempts to compose onto
  // an object already having this many components will fail. This OutputStream will enforce
  // the limit before attempting the compose operation at all, so that the stream can be
  // considered still safe to use and eventually close() without losing data even if
  // intermediate attempts to hsync() throw exceptions due to the component limit.
  public static final int MAX_COMPOSITE_COMPONENTS = 1024;

  private static final Logger LOG =
      LoggerFactory.getLogger(GoogleHadoopSyncableOutputStream.class);

  // Temporary files don't need to contain the desired attributes of the final destination file
  // since metadata settings get clobbered on final compose() anyways; additionally, due to
  // the way we pick temp file names and already ensured directories for the destination file,
  // we can optimize tempfile creation by skipping various directory checks.
  private static final CreateFileOptions TEMPFILE_CREATE_OPTIONS =
      new CreateFileOptions(
          /* overwriteExisting= */ false,
          CreateFileOptions.DEFAULT_CONTENT_TYPE,
          CreateFileOptions.EMPTY_ATTRIBUTES,
          /* checkNoDirectoryConflict= */ false,
          /* ensureParentDirectoriesExist= */ false,
          /* existingGenerationId= */ 0L);

  // Deletion of temporary files occurs asynchronously for performance reasons, but in-flight
  // deletions are awaited on close() so as long as all output streams are closed, there should
  // be no remaining in-flight work occurring inside this threadpool.
  private static final ExecutorService TEMPFILE_CLEANUP_THREADPOOL =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("gcs-syncable-output-stream-cleanup-pool-%d")
              .setDaemon(true)
              .build());

  // Instance of GoogleHadoopFileSystemBase.
  private final GoogleHadoopFileSystemBase ghfs;

  // The final destination path for this stream.
  private final URI finalGcsPath;

  // Buffer size to pass through to delegate streams.
  private final int bufferSize;

  // Statistics tracker provided by the parent GoogleHadoopFileSystemBase for recording
  // numbers of bytes written.
  private final FileSystem.Statistics statistics;

  // Metadata/overwrite options to use on final file.
  private final CreateFileOptions fileOptions;

  // List of file-deletion futures accrued during the lifetime of this output stream.
  private final List<Future<Void>> deletionFutures;

  private final ExecutorService cleanupThreadpool;

  // Current GCS path pointing at the "tail" file which will be appended to the destination
  // on each hsync() call.
  private URI curGcsPath;

  // Current OutputStream pointing at the "tail" file which will be appended to the destination
  // on each hsync() call.
  private GoogleHadoopOutputStream curDelegate;

  // Stores the current component index corresponding curGcsPath. If close() is called, the total
  // number of components in the finalGcsPath will be curComponentIndex + 1.
  private int curComponentIndex;

  // The last known generationId of the final destination file, or possibly
  // StorageResourceId.UNKNOWN_GENERATION_ID if unknown.
  private long curDestGenerationId;

  /**
   * Creates a new GoogleHadoopSyncableOutputStream with initial stream initialized and expected
   * to begin at file-offset 0. This constructor is not suitable for "appending" to already
   * existing files.
   */
  public GoogleHadoopSyncableOutputStream(
      GoogleHadoopFileSystemBase ghfs, URI gcsPath, int bufferSize,
      FileSystem.Statistics statistics, CreateFileOptions createFileOptions)
      throws IOException {
    this(ghfs, gcsPath, bufferSize, statistics, createFileOptions, TEMPFILE_CLEANUP_THREADPOOL);
  }

  GoogleHadoopSyncableOutputStream(
      GoogleHadoopFileSystemBase ghfs, URI gcsPath, int bufferSize,
      FileSystem.Statistics statistics, CreateFileOptions createFileOptions,
      ExecutorService cleanupThreadpool)
      throws IOException {
    LOG.debug("GoogleHadoopSyncableOutputStream({}, {})", gcsPath, bufferSize);
    this.ghfs = ghfs;
    this.finalGcsPath = gcsPath;
    this.bufferSize = bufferSize;
    this.statistics = statistics;
    this.fileOptions = createFileOptions;
    this.deletionFutures = new ArrayList<>();
    this.cleanupThreadpool = cleanupThreadpool;

    // The first component of the stream will go straight to the destination filename to optimize
    // the case where no hsync() or a single hsync() is called during the lifetime of the stream;
    // committing the first component thus doesn't require any compose() call under the hood.
    this.curGcsPath = gcsPath;
    this.curDelegate = new GoogleHadoopOutputStream(
        ghfs, curGcsPath, bufferSize, statistics, fileOptions);

    // TODO(user): Make sure to initialize this to the correct value if a new stream is created to
    // "append" to an existing file.
    this.curComponentIndex = 0;

    this.curDestGenerationId = StorageResourceId.UNKNOWN_GENERATION_ID;
  }

  @Override
  public void write(int b) throws IOException {
    throwIfNotOpen();
    curDelegate.write(b);
  }

  @Override
  public void write(byte[] b, int offset, int len) throws IOException {
    throwIfNotOpen();
    curDelegate.write(b, offset, len);
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close(): Current tail file: {} final destination: {}", curGcsPath, finalGcsPath);
    if (!isOpen()) {
      LOG.debug("close(): Ignoring; stream already closed.");
      return;
    }
    commitCurrentFile();

    // null denotes stream closed.
    // TODO(user): Add checks which throw IOException if further operations are attempted on a
    // closed stream, except for multiple calls to close(), which should behave as no-ops.
    curGcsPath = null;
    curDelegate = null;

    LOG.debug("close(): Awaiting {} deletionFutures", deletionFutures.size());
    for (Future<?> deletion : deletionFutures) {
      try {
        deletion.get();
      } catch (ExecutionException | InterruptedException ee) {
        if (ee.getCause() instanceof IOException) {
          throw (IOException) ee.getCause();
        } else {
          throw new IOException(ee);
        }
      }
    }
    LOG.debug("close(): done");
  }

  public void sync() throws IOException {
    hsync();
  }

  /**
   * There is no way to flush data to become available for readers without a full-fledged
   * hsync(), so this method is a no-op.
   * This overrides Syncable.hflush(), but is not annotated as such because the method doesn't
   * exist in Hadoop 1.
   */
  public void hflush() throws IOException {
    LOG.warn(
        "hflush() is a no-op; readers will *not* yet see flushed data for {}", finalGcsPath);
    throwIfNotOpen();
  }

  /**
   * This overrides Syncable.hsync(), but is not annotated as such because the method doesn't
   * exist in Hadoop 1.
   *
   * @throws CompositeLimitExceededException if this hsync() call would require any future close()
   *     call to exceed the component limit. If CompositeLimitExceededException is thrown, no
   *     actual GCS operations are taken and it's safe to subsequently call close() on this
   *     stream as normal; it just means data written since the last successful hsync() has not
   *     yet been committed.
   */
  public void hsync() throws IOException {
    LOG.debug("hsync(): Committing tail file {} to final destination {}", curGcsPath, finalGcsPath);
    throwIfNotOpen();
    long startTime = System.nanoTime();

    // If we were to call close() instead of hsync() right now, the final object would have this
    // many components.
    int curNumComponents = curComponentIndex + 1;
    if (curNumComponents >= MAX_COMPOSITE_COMPONENTS) {
      throw new CompositeLimitExceededException(String.format(
          "Cannot hsync() '%s' because subsequent component count would exceed limit of %d",
          finalGcsPath, MAX_COMPOSITE_COMPONENTS));
    }

    commitCurrentFile();

    // Use a different temporary path for each temporary component to reduce the possible avenues of
    // race conditions in the face of low-level retries, etc.
    ++curComponentIndex;
    curGcsPath = getNextTemporaryPath();

    LOG.debug("hsync(): Opening next temporary tail file {} as component number {}",
        curGcsPath, curComponentIndex);
    curDelegate = new GoogleHadoopOutputStream(
        ghfs, curGcsPath, bufferSize, statistics, TEMPFILE_CREATE_OPTIONS);
    long endTime = System.nanoTime();
    LOG.debug("Took {} ns to hsync()", endTime - startTime);
  }

  private void commitCurrentFile() throws IOException {
    // TODO(user): Optimize the case where 0 bytes have been written in the current component
    // to return early.
    WritableByteChannel innerChannel = curDelegate.getInternalChannel();
    curDelegate.close();

    long generationId = StorageResourceId.UNKNOWN_GENERATION_ID;
    if (innerChannel instanceof GoogleCloudStorageItemInfo.Provider) {
      generationId = ((GoogleCloudStorageItemInfo.Provider) innerChannel)
          .getItemInfo().getContentGeneration();
      LOG.debug(
          "innerChannel is GoogleCloudStorageItemInfo.Provider; closed generationId {}.",
          generationId);
    } else {
      LOG.debug("innerChannel NOT instanceof provider: {}", innerChannel.getClass());
    }

    // On the first component, curGcsPath will equal finalGcsPath, and no compose() call is
    // necessary. Otherwise, we compose in-place into the destination object and then delete
    // the temporary object.
    if (!finalGcsPath.equals(curGcsPath)) {
      StorageResourceId destResourceId =
          StorageResourceId.fromObjectName(finalGcsPath.toString(), curDestGenerationId);
      final StorageResourceId tempResourceId =
          StorageResourceId.fromObjectName(curGcsPath.toString(), generationId);
      if (!destResourceId.getBucketName().equals(tempResourceId.getBucketName())) {
        throw new IllegalStateException(String.format(
            "Destination bucket in path '%s' doesn't match temp file bucket in path '%s'",
            finalGcsPath, curGcsPath));
      }
      GoogleCloudStorageItemInfo composedObject = ghfs.getGcsFs().getGcs().composeObjects(
          ImmutableList.of(destResourceId, tempResourceId),
          destResourceId,
          GoogleCloudStorageFileSystem.objectOptionsFromFileOptions(fileOptions));
      curDestGenerationId = composedObject.getContentGeneration();
      deletionFutures.add(cleanupThreadpool.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          ghfs.getGcsFs().getGcs().deleteObjects(ImmutableList.of(tempResourceId));
          return null;
        }
      }));
    } else {
      // First commit was direct to the destination; the generationId of the object we just
      // committed will be used as the destination generation id for future compose calls.
      curDestGenerationId = generationId;
    }
  }

  /**
   * Returns URI to be used for the next "tail" file in the series.
   */
  private URI getNextTemporaryPath() {
    Path basePath = ghfs.getHadoopPath(finalGcsPath);
    Path baseDir = basePath.getParent();
    Path tempPath = new Path(
        baseDir,
        String.format("%s%s.%d.%s",
            TEMPFILE_PREFIX, basePath.getName(), curComponentIndex, UUID.randomUUID().toString()));
    return ghfs.getGcsPath(tempPath);
  }

  private boolean isOpen() {
    return curDelegate != null;
  }

  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }
}
