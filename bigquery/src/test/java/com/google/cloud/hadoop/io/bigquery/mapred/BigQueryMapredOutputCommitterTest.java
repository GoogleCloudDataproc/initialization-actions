package com.google.cloud.hadoop.io.bigquery.mapred;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Unit tests for {@link BigQueryMapredOutputCommitter}.
 */
@RunWith(JUnit4.class)
public class BigQueryMapredOutputCommitterTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Mock private JobContext mockJobContext;
  @Mock private TaskAttemptContext mockTaskAttemptContext;
  @Mock private org.apache.hadoop.mapreduce.OutputCommitter mockOutputCommitter;

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After public void tearDown() {
    verifyNoMoreInteractions(mockJobContext);
    verifyNoMoreInteractions(mockTaskAttemptContext);
    verifyNoMoreInteractions(mockOutputCommitter);
  }

  @Test public void testAbortJob() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    int status = 1;
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.abortJob(mockJobContext, status);

    verify(mockOutputCommitter).abortJob(
        any(JobContext.class), any(State.class));
  }

  @Test public void testAbortJobBadStatus() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    int status = -1;
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    expectedException.expect(IllegalArgumentException.class);
    outputCommitter.abortJob(mockJobContext, status);
  }

  @Test public void testAbortTask() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.abortTask(mockTaskAttemptContext);

    verify(mockOutputCommitter).abortTask(any(TaskAttemptContext.class));
  }

  @Test public void testCleanupJob() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.cleanupJob(mockJobContext);

    verify(mockOutputCommitter).cleanupJob(any(JobContext.class));
  }

  @Test public void testCommitJob() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.commitJob(mockJobContext);

    verify(mockOutputCommitter).commitJob(any(JobContext.class));
  }

  @Test public void testCommitTask() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.commitTask(mockTaskAttemptContext);

    verify(mockOutputCommitter).commitTask(any(TaskAttemptContext.class));
  }

  @Test public void testNeedsTaskCommit() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.needsTaskCommit(mockTaskAttemptContext);

    verify(mockOutputCommitter).needsTaskCommit(any(TaskAttemptContext.class));
  }

  @Test public void testSetupJob() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.setupJob(mockJobContext);

    verify(mockOutputCommitter).setupJob(any(JobContext.class));
  }

  @Test public void testSetupTask() throws IOException {
    BigQueryMapredOutputCommitter outputCommitter =
        new BigQueryMapredOutputCommitter();
    outputCommitter.setMapreduceOutputCommitter(mockOutputCommitter);

    outputCommitter.setupTask(mockTaskAttemptContext);

    verify(mockOutputCommitter).setupTask(any(TaskAttemptContext.class));
  }
}
