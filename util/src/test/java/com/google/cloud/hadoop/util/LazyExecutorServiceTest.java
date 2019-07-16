/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LazyExecutorService}. */
@RunWith(JUnit4.class)
public class LazyExecutorServiceTest {

  private LazyExecutorService lazyExecutorService;

  @Before
  public void before() {
    lazyExecutorService = new LazyExecutorService();
  }

  @Test
  public void testConstructorWithBackingService() {
    lazyExecutorService = new LazyExecutorService(new LazyExecutorService());
    assertThat(lazyExecutorService.isShutdown()).isFalse();
  }

  @Test
  public void testIsTerminated() {
    lazyExecutorService.shutdown();
    assertThat(lazyExecutorService.isTerminated()).isTrue();
  }

  @Test
  public void testAwaitTermination() throws Exception {
    assertThat(lazyExecutorService.awaitTermination(1, TimeUnit.MILLISECONDS)).isFalse();
    lazyExecutorService.shutdown();
    assertThat(lazyExecutorService.awaitTermination(1, TimeUnit.MILLISECONDS)).isTrue();
  }

  @Test
  public void testSubmitTask() {
    Future<?> future = lazyExecutorService.submit(() -> {});
    assertThat(future.isDone()).isTrue();
  }

  @Test
  public void testSubmitTaskWithResult() {
    Future<Object> future = lazyExecutorService.submit(() -> {}, /* result= */ null);
    assertThat(future.isDone()).isTrue();
  }

  @Test
  public void testInvokeAllSubmitAllTasks() throws Exception {
    List<Callable<Void>> tasks = ImmutableList.of(() -> null, () -> null);
    List<Future<Void>> futures = lazyExecutorService.invokeAll(tasks);
    assertThat(futures).hasSize(2);
  }

  @Test
  public void testInvokeAllSubmitAllTasksWithTimeout() throws Exception {
    List<Callable<Void>> tasks = ImmutableList.of(() -> null, () -> null);
    List<Future<Void>> futures = lazyExecutorService.invokeAll(tasks, 1, TimeUnit.MILLISECONDS);
    assertThat(futures).hasSize(2);
  }

  @Test
  public void testBackingService_shouldBeShutDownWithMainService() {
    LazyExecutorService backingService = new LazyExecutorService();
    lazyExecutorService = new LazyExecutorService(backingService);
    backingService.shutdown();
    assertThat(lazyExecutorService.isShutdown()).isTrue();
  }

  @Test
  public void testSubmitTaskToDeadExecutorService_shouldThrowRejectedExecutionException() {
    lazyExecutorService.shutdown();
    assertThrows(RejectedExecutionException.class, () -> lazyExecutorService.submit(() -> {}));
  }

  @Test
  public void testCancelledTask() {
    lazyExecutorService = new LazyExecutorService(new LazyExecutorService());
    Future<?> future = lazyExecutorService.submit(() -> {});
    lazyExecutorService.shutdownNow();

    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
    assertThrows(CancellationException.class, future::get);
    assertThrows(CancellationException.class, () -> future.get(1, TimeUnit.MILLISECONDS));
    assertThat(future.cancel(/* mayInterruptIfRunning= */ true)).isFalse();
  }
}
