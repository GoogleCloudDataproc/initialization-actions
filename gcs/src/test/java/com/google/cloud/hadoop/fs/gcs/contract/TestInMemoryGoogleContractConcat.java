package com.google.cloud.hadoop.fs.gcs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestInMemoryGoogleContractConcat extends AbstractGoogleContractConcatTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new InMemoryGoogleContract(conf);
  }
}
