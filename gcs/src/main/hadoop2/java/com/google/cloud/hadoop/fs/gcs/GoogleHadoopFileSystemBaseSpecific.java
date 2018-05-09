package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Hadoop 2 specific implementation of {@link FileSystem}.
 *
 * @see GoogleHadoopFileSystemBase
 */
abstract class GoogleHadoopFileSystemBaseSpecific extends FileSystem {

  /** @see GoogleHadoopFileSystemBase#getGcsPath(Path) */
  public abstract URI getGcsPath(Path hadoopPath);

  /** @see GoogleHadoopFileSystemBase#getGcsFs() */
  public abstract GoogleCloudStorageFileSystem getGcsFs();

  /** {@inheritDoc} */
  @Override
  public FSDataOutputStream createNonRecursive(
      Path hadoopPath,
      FsPermission permission,
      EnumSet<org.apache.hadoop.fs.CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    URI gcsPath = getGcsPath(checkNotNull(hadoopPath, "hadoopPath must not be null"));
    URI parentGcsPath = getGcsFs().getParentPath(gcsPath);
    GoogleCloudStorageItemInfo parentInfo = getGcsFs().getFileInfo(parentGcsPath).getItemInfo();
    if (!parentInfo.isRoot() && !parentInfo.isBucket() && !parentInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "Can not create '%s' file, because parent folder does not exist: %s",
              gcsPath, parentGcsPath));
    }
    return create(
        hadoopPath,
        permission,
        flags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE),
        bufferSize,
        replication,
        blockSize,
        progress);
  }
}
