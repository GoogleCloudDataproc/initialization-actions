/**
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unittests for fine-grained edge cases in GoogleHadoopSyncableOutputStream.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopSyncableOutputStreamTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock private ExecutorService mockExecutorService;
  @Mock private Future<Void> mockFuture;

  private GoogleHadoopFileSystemBase ghfs;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);

    ghfs = (GoogleHadoopFileSystemBase) GoogleHadoopFileSystemTestHelper
        .createInMemoryGoogleHadoopFileSystem();
    ghfs.getConf().set(
        GoogleHadoopFileSystemBase.GCS_OUTPUTSTREAM_TYPE_KEY,
        GoogleHadoopFileSystemBase.OutputStreamType.SYNCABLE_COMPOSITE.toString());
  }

  @After
  public void tearDown() throws IOException {
    ghfs.close();

    verifyNoMoreInteractions(mockExecutorService);
    verifyNoMoreInteractions(mockFuture);
  }

  @Test
  public void testEndToEndHsync() throws IOException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object.txt");
    FSDataOutputStream fout = ghfs.create(objectPath);

    byte[] data1 = new byte[] { 0x0f, 0x0e, 0x0e, 0x0d };
    byte[] data2 = new byte[] { 0x0b, 0x0e, 0x0e, 0x0f };
    byte[] data3 = new byte[] { 0x04, 0x02 };
    byte[] data1Read = new byte[4];
    byte[] data2Read = new byte[4];
    byte[] data3Read = new byte[2];

    fout.write(data1, 0, data1.length);
    fout.sync();

    assertThat(ghfs.getFileStatus(objectPath).getLen()).isEqualTo(4);
    FSDataInputStream fin = ghfs.open(objectPath);
    fin.read(data1Read);
    fin.close();
    assertThat(data1Read).isEqualTo(data1);

    fout.write(data2, 0, data2.length);
    fout.sync();

    assertThat(ghfs.getFileStatus(objectPath).getLen()).isEqualTo(8);
    fin = ghfs.open(objectPath);
    fin.read(data1Read);
    fin.read(data2Read);
    fin.close();
    assertThat(data1Read).isEqualTo(data1);
    assertThat(data2Read).isEqualTo(data2);

    fout.write(data3, 0, data3.length);
    fout.close();

    assertThat(ghfs.getFileStatus(objectPath).getLen()).isEqualTo(10);
    fin = ghfs.open(objectPath);
    fin.read(data1Read);
    fin.read(data2Read);
    fin.read(data3Read);
    fin.close();
    assertThat(data1Read).isEqualTo(data1);
    assertThat(data2Read).isEqualTo(data2);
    assertThat(data3Read).isEqualTo(data3);
  }

  @Test
  public void testExceptionOnDelete() throws IOException, InterruptedException, ExecutionException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object2.txt");
    GoogleHadoopSyncableOutputStream fout = new GoogleHadoopSyncableOutputStream(
        ghfs,
        ghfs.getGcsPath(objectPath),
        4096,
        new FileSystem.Statistics(ghfs.getScheme()),
        CreateFileOptions.DEFAULT,
        mockExecutorService);

    IOException fakeIoException = new IOException("fake io exception");
    when(mockExecutorService.submit(any(Callable.class)))
        .thenReturn(mockFuture);
    when(mockFuture.get())
        .thenThrow(new ExecutionException(fakeIoException));

    byte[] data1 = new byte[] { 0x0f, 0x0e, 0x0e, 0x0d };
    byte[] data2 = new byte[] { 0x0b, 0x0e, 0x0e, 0x0f };
    byte[] data1Read = new byte[4];
    byte[] data2Read = new byte[4];

    fout.write(data1, 0, data1.length);
    fout.sync();  // This one commits straight into destination.

    fout.write(data2, 0, data2.length);
    fout.sync();  // This one enqueues the delete, but doesn't propagate exception yet.

    verify(mockExecutorService).submit(any(Callable.class));

    expectedException.expect(IOException.class);
    expectedException.expectMessage(fakeIoException.getMessage());

    try {
      fout.close();
    } finally {
      verify(mockExecutorService, times(2)).submit(any(Callable.class));
      verify(mockFuture).get();
    }
  }

  @Test
  public void testCloseTwice() throws IOException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object.txt");
    FSDataOutputStream fout = ghfs.create(objectPath);
    fout.close();
    fout.close();  // Fine to close twice.
  }

  @Test
  public void testWrite1AfterClose() throws IOException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object.txt");
    FSDataOutputStream fout = ghfs.create(objectPath);

    expectedException.expect(ClosedChannelException.class);
    fout.close();
    fout.write(42);
  }

  @Test
  public void testWriteAfterClose() throws IOException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object.txt");
    FSDataOutputStream fout = ghfs.create(objectPath);

    expectedException.expect(ClosedChannelException.class);
    fout.close();
    fout.write(new byte[] { 0x01 }, 0, 1);
  }

  @Test
  public void testSyncAfterClose() throws IOException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object.txt");
    FSDataOutputStream fout = ghfs.create(objectPath);

    expectedException.expect(ClosedChannelException.class);
    fout.close();
    fout.sync();
  }

  @Test
  public void testSyncCompositeLimitException() throws IOException {
    Path objectPath = new Path(ghfs.getFileSystemRoot(), "dir/object.txt");
    FSDataOutputStream fout = ghfs.create(objectPath);

    byte[] expected = new byte[GoogleHadoopSyncableOutputStream.MAX_COMPOSITE_COMPONENTS + 1];
    byte[] buf = new byte[1];
    for (int i = 0; i < GoogleHadoopSyncableOutputStream.MAX_COMPOSITE_COMPONENTS - 1; ++i) {
      buf[0] = (byte) i;
      expected[i] = buf[0];
      fout.write(buf, 0, 1);
      fout.sync();
    }

    // If the limit is N, then the Nth attempt to call sync() will fail, since it means the
    // base object already has N - 1 components, and we have 1 temporary object in-progress,
    // and a call to close() at this point brings the base object up to the limit of N.
    try {
      // Despite the exception we're expecting, the data here should still be safe.
      fout.write(new byte[] { 0x42 });
      expected[GoogleHadoopSyncableOutputStream.MAX_COMPOSITE_COMPONENTS - 1] = 0x42;

      fout.sync();
      Assert.fail("Expected CompositeLimitExceededException");
    } catch (CompositeLimitExceededException clee) {
      // Expected.
    }

    // Despite having thrown an exception, the stream is still safe to use and even write more data.
    fout.write(new byte[] { 0x11 });
    expected[GoogleHadoopSyncableOutputStream.MAX_COMPOSITE_COMPONENTS] = 0x11;
    fout.close();

    byte[] actual = new byte[expected.length];
    FSDataInputStream fin = ghfs.open(objectPath);
    fin.read(actual);
    fin.close();
    assertThat(actual).isEqualTo(expected);
  }
}
