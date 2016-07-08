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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;

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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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

    Assert.assertEquals(4, ghfs.getFileStatus(objectPath).getLen());
    FSDataInputStream fin = ghfs.open(objectPath);
    fin.read(data1Read);
    fin.close();
    Assert.assertArrayEquals(data1, data1Read);

    fout.write(data2, 0, data2.length);
    fout.sync();

    Assert.assertEquals(8, ghfs.getFileStatus(objectPath).getLen());
    fin = ghfs.open(objectPath);
    fin.read(data1Read);
    fin.read(data2Read);
    fin.close();
    Assert.assertArrayEquals(data1, data1Read);
    Assert.assertArrayEquals(data2, data2Read);

    fout.write(data3, 0, data3.length);
    fout.close();

    Assert.assertEquals(10, ghfs.getFileStatus(objectPath).getLen());
    fin = ghfs.open(objectPath);
    fin.read(data1Read);
    fin.read(data2Read);
    fin.read(data3Read);
    fin.close();
    Assert.assertArrayEquals(data1, data1Read);
    Assert.assertArrayEquals(data2, data2Read);
    Assert.assertArrayEquals(data3, data3Read);
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
}
