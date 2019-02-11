package com.google.cloud.hadoop.fs.gcs.contract;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.AfterClass;
import org.junit.Before;

/** GCS contract tests covering file concat. */
public class TestGoogleContractConcat extends AbstractGoogleContractConcatTest {

  private static final TestBucketHelper TEST_BUCKET_HELPER =
      new TestBucketHelper(GoogleContract.TEST_BUCKET_NAME_PREFIX);

  private static final AtomicReference<GoogleHadoopFileSystem> fs = new AtomicReference<>();

  @Before
  public void before() {
    fs.compareAndSet(null, (GoogleHadoopFileSystem) getFileSystem());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    TEST_BUCKET_HELPER.cleanup(fs.get().getGcsFs().getGcs());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new GoogleContract(conf, TEST_BUCKET_HELPER);
  }
}
