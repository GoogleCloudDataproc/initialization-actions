/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.ListStatusFileNotFoundBehavior;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * InMemoryGlobberFileSystem overrides the behavior of {@link FileSystem} to manifest a temporary
 * FileSystem suitable only for list/get methods for retrieving file statuses, based on an
 * collection of provided file statuses at construction time. Can be used as a heavyweight cache
 * which behaves just like a normal filesystem for such metadata-read operations and lives in the
 * context of a single complex top-level method call, like globStatus.
 *
 * <p>Note that this class is <b>not</b> intended to be used as a general-usage {@link FileSystem}.
 */
class InMemoryGlobberFileSystem extends FileSystem {

  /**
   * Factory method for constructing and initializing an instance of InMemoryGlobberFileSystem which
   * is ready to list/get FileStatus entries corresponding to {@code fileStatuses}.
   */
  public static FileSystem createInstance(
      Configuration config, Path workingDirectory, Collection<FileStatus> fileStatuses) {
    checkNotNull(config, "configuration can not be null");

    FileSystem fileSystem = new InMemoryGlobberFileSystem(workingDirectory, fileStatuses);
    fileSystem.setConf(config);

    return fileSystem;
  }

  private final Path workingDirectory;
  private final URI uri;
  private final ListStatusFileNotFoundBehavior listStatusNotFoundBehavior =
      ListStatusFileNotFoundBehavior.get();
  private final Map<Path, FileStatus> fileStatusesByPath;
  private final Map<Path, List<FileStatus>> fileStatusesByParentPath;

  /**
   * Constructs an instance of InMemoryGlobberFileSystem using the provided collection of {@link
   * FileStatus} objects; {@code initialize()} will not re-initialize it.
   */
  private InMemoryGlobberFileSystem(Path workingDirectory, Collection<FileStatus> fileStatuses) {
    this.workingDirectory = workingDirectory;
    this.uri = workingDirectory.toUri();

    this.fileStatusesByPath = Maps.newHashMapWithExpectedSize(fileStatuses.size());
    this.fileStatusesByParentPath = Maps.newHashMapWithExpectedSize(fileStatuses.size() / 5);
    for (FileStatus fileStatus : fileStatuses) {
      this.fileStatusesByPath.put(fileStatus.getPath(), fileStatus);
      if (fileStatus.getPath().getParent() != null) {
        this.fileStatusesByParentPath
            .computeIfAbsent(fileStatus.getPath().getParent(), k -> new ArrayList<>())
            .add(fileStatus);
      }
    }
  }

  /** @inheritDoc */
  @Override
  public URI getUri() {
    return uri;
  }

  /** @inheritDoc */
  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  /** @inheritDoc */
  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    Path qualifiedPath = makeQualified(f);
    List<FileStatus> fileStatuses = fileStatusesByParentPath.get(qualifiedPath);
    if (fileStatuses == null) {
      return listStatusNotFoundBehavior.handle(
          String.format("%s (qualified: %s)", f, qualifiedPath));
    }
    return fileStatuses.toArray(new FileStatus[0]);
  }

  /** @inheritDoc */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path qualifiedPath = makeQualified(f);
    FileStatus fileStatus = fileStatusesByPath.get(f);
    if (fileStatus == null) {
      throw new FileNotFoundException(
          String.format("Path '%s' (qualified: '%s') does not exist.", f, qualifiedPath));
    }
    return fileStatus;
  }

  // Below are unsupported methods that are not used in 'globStatus' calls

  /** @inheritDoc */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public boolean delete(Path f) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public void setWorkingDirectory(Path newDir) {
    throw new UnsupportedOperationException();
  }

  /** @inheritDoc */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException();
  }
}
