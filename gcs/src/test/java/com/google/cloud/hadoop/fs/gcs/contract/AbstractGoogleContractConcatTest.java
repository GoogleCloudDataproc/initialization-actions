package com.google.cloud.hadoop.fs.gcs.contract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertFileHasLength;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractConcatTest;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

public abstract class AbstractGoogleContractConcatTest extends AbstractContractConcatTest {

  @Test
  public void testConcatMultiple() throws Throwable {
    int numFiles = GoogleCloudStorage.MAX_COMPOSE_OBJECTS * 3 / 2;
    Path testPath = path("test");

    byte[][] blocks = new byte[numFiles][0];
    Path[] srcs = new Path[numFiles];
    for (int i = 0; i < numFiles; i++) {
      Path srcFile = new Path(testPath, "" + i);
      blocks[i] = dataset(TEST_FILE_LEN, i, 255);
      createFile(getFileSystem(), srcFile, true, blocks[i]);
      srcs[i] = srcFile;
    }
    Path target = new Path(testPath, "target");

    createFile(getFileSystem(), target, false, new byte[0]);
    getFileSystem().concat(target, srcs);
    assertFileHasLength(getFileSystem(), target, TEST_FILE_LEN * numFiles);
    ContractTestUtils.validateFileContent(
        ContractTestUtils.readDataset(getFileSystem(), target, TEST_FILE_LEN * numFiles), blocks);
  }
}
