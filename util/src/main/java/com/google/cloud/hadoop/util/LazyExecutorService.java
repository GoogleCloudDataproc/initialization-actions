// Copyright 2007 Google Inc. All Rights Reserved.

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.annotations.GwtIncompatible;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Defers execution to the time that a method that expresses interest in the result (get or isDone)
 * is called on the Future. Execution is performed by a backing ExecutorService.
 *
 * <p>In essence, a returned Future represents a "canned" method call and once the call has been
 * performed, the Future returns the cached result.
 *
 * <p>Both this class and the returned Future are thread-safe.
 *
 * @author tobe@google.com (Torbjorn Gannholm)
 * @author jlevy@google.com (Jared Levy)
 * @author cpovirk@google.com (Chris Povirk)
 */
@GwtIncompatible
public final class LazyExecutorService implements ExecutorService {
  private volatile boolean shutdown = false;
  private final ExecutorService backingService;
  private final CountDownLatch terminated = new CountDownLatch(1);

  /**
   * Creates an instance using a {@link MoreExecutors#newDirectExecutorService()} for the backing
   * service.
   */
  public LazyExecutorService() {
    backingService = MoreExecutors.newDirectExecutorService();
  }

  /**
   * Creates an instance using the given {@code ExecutorService} as the backing service.
   *
   * <p>The backing service will only be used to execute tasks and it may be shared by several
   * instances or used for other purposes. Shutdowns of this instance will not shut down the backing
   * service.
   *
   * <p>If you shut down the backing service, this instance will be shut down automatically and all
   * tasks submitted to this instance that have not yet been submitted to the backing service will
   * be considered cancelled.
   */
  public LazyExecutorService(ExecutorService backingService) {
    this.backingService = backingService;
  }

  /**
   * A set of all submitted uncompleted tasks so that we can cancel them on {@code shutdownNow()}.
   * The tasks need to be wrapped in weak references so that tasks that are just dropped can be
   * gc:ed. The set needs to be safe for concurrent access.
   */
  private final Set<ExecutingFuture<?>> pendingTasks =
      Collections.newSetFromMap(new MapMaker().weakKeys().<ExecutingFuture<?>, Boolean>makeMap());

  /**
   * Manages compound conditions involving changing the size of {@code pendingTasks} and the value
   * of {@code shutdown}.
   */
  private final ReentrantLock tasksAndTerminationLock = new ReentrantLock();

  /**
   * Should be called when a task is completed or cancelled.
   *
   * @param f The completed or cancelled task to remove.
   */
  private void removePendingTask(ExecutingFutureImpl<?> f) {
    pendingTasks.remove(f);
    updateTerminationState();
  }

  private void updateTerminationState() {
    tasksAndTerminationLock.lock();
    try {
      if (shutdown && pendingTasks.isEmpty()) {
        terminated.countDown();
      }
    } finally {
      tasksAndTerminationLock.unlock();
    }
  }

  /** Shuts this service down, but leaves the backing service untouched. */
  @Override
  public void shutdown() {
    shutdown = true;
    updateTerminationState();
  }

  /**
   * Trying to interpret the assumptions about the contract of this method in the light of this
   * implementation, it seems most reasonable to take the view that all tasks are running, even if
   * the processing has not actually started. Therefore, unfinished tasks will be cancelled and an
   * empty list will be returned.
   */
  @CanIgnoreReturnValue
  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
    // Cancel all unfinished tasks.
    // Get a snapshot because future.cancel modifies pendingTasks.
    Future<?>[] runningTasks = pendingTasks.toArray(new Future[0]);
    for (Future<?> future : runningTasks) {
      // Cancel may not succeed, but it's best effort.
      future.cancel(true);
    }
    return Lists.newLinkedList();
  }

  @Override
  public boolean isShutdown() {
    checkBackingService();
    return shutdown;
  }

  /**
   * Checks if this service has been implicitly shut down through a shutdown on the backing service
   * and make the state reflect that.
   */
  private void checkBackingService() {
    if (backingService.isShutdown()) {
      // This service is logically also shut down.
      shutdown();
      // Notify the unfinished Futures.
      ExecutingFuture<?>[] runningTasks = pendingTasks.toArray(new ExecutingFuture<?>[0]);
      for (ExecutingFuture<?> future : runningTasks) {
        future.backingServiceDied();
      }
    }
  }

  @Override
  public boolean isTerminated() {
    return isShutdown() && terminated.getCount() == 0;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    if (isTerminated()) {
      return true;
    }
    return terminated.await(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(final Callable<T> task) {
    checkNotNull(task, "Null task submitted.");
    tasksAndTerminationLock.lock();
    try {
      if (isShutdown()) {
        throw new RejectedExecutionException("ExecutorService is shutdown");
      }
      ExecutingFuture<T> future = new ExecutingFutureImpl<T>(task);
      pendingTasks.add(future);
      return future;
    } finally {
      tasksAndTerminationLock.unlock();
    }
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return submit(Executors.callable(task, result));
  }

  @Override
  public Future<?> submit(Runnable command) {
    return submit(Executors.callable(command));
  }

  /**
   * ExecutorService requires that this method should not return until all tasks are completed,
   * which precludes lazy execution. Tasks are run in parallel, as far as the backing service
   * allows.
   *
   * <p>This method makes sense from a cached result perspective but not from a lazy execution
   * perspective.
   */
  @CanIgnoreReturnValue
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    List<Future<T>> result = Lists.newLinkedList();
    try {
      for (Callable<T> task : tasks) {
        result.add(submit(task));
      }
      List<Callable<Void>> monitorTasks = createMonitorTasksFor(result, 0, null);
      backingService.invokeAll(monitorTasks);
    } finally {
      // Clean up. These are no-ops for completed tasks.
      for (Future<T> future : result) {
        future.cancel(true);
      }
    }
    return result;
  }

  /**
   * ExecutorService requires that this method should not return until all tasks are completed or
   * the timeout expires, which precludes lazy execution. Tasks are run in parallel, as far as the
   * backing service allows. Timeout is done as a best-effort in case of the default same thread
   * executor.
   *
   * <p>This method makes sense from a cached result perspective but not from a lazy execution
   * perspective.
   */
  @CanIgnoreReturnValue
  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
      throws InterruptedException {
    checkNotNull(unit);
    final List<Future<T>> result = Lists.newLinkedList();
    try {
      for (Callable<T> task : tasks) {
        result.add(submit(task));
      }
      List<Callable<Void>> monitorTasks = createMonitorTasksFor(result, timeout, unit);
      backingService.invokeAll(monitorTasks, timeout, unit);
    } finally {
      // Clean up, even when interrupted. These are no-ops on normal exit.
      for (Future<T> future : result) {
        future.cancel(true);
      }
    }
    return result;
  }

  private static <T> List<Callable<Void>> createMonitorTasksFor(
      List<Future<T>> futures, long timeout, @Nullable TimeUnit unit) {
    List<Callable<Void>> monitorTasks = Lists.newLinkedList();
    // A null unit means 0 means "unbounded."
    long deadline = unit == null ? 0 : System.nanoTime() + NANOSECONDS.convert(timeout, unit);
    // We need to add tasks for both starting and checking completion.
    // In the case of a direct executor, the starting tasks will be slow
    // and actually perform the task, while the checks are instant.
    // In the case of a ThreadPoolExecutor, the start tasks are instant and
    // the checks await completion.
    // We want to add all the start tasks before the completion check tasks.
    // TODO(user): This assumes tasks are executed in order. Verify or fix.
    for (Future<T> future : futures) {
      monitorTasks.add(new StartExecutionTask(future));
    }
    for (Future<T> future : futures) {
      monitorTasks.add(new CompletionCheckTask(future, deadline));
    }
    return monitorTasks;
  }

  private static class StartExecutionTask implements Callable<Void> {
    private final Future<?> future;

    StartExecutionTask(Future<?> future) {
      this.future = future;
    }

    @Override
    public Void call() {
      future.isDone();
      return null;
    }
  }

  private static class CompletionCheckTask implements Callable<Void> {
    private final Future<?> future;
    private final long deadline;

    CompletionCheckTask(Future<?> future, long deadline) {
      this.future = future;
      this.deadline = deadline;
    }

    @Override
    public Void call() {
      try {
        if (deadline == 0) {
          future.get();
        } else {
          /* The timeout here is just a safeguard. The timing is really done
           * in the invoking code. */
          future.get(deadline - System.nanoTime(), NANOSECONDS);
        }
      } catch (ExecutionException e) {
        // We don't care at this point.
      } catch (InterruptedException e) {
        // Propagate the interrupt.
        Thread.currentThread().interrupt();
        // Interrupt execution
        future.cancel(true);
      } catch (TimeoutException e) {
        // Interrupt execution
        future.cancel(true);
      }
      return null;
    }
  }

  /**
   * Always throws a RejectedExecutionException because using this method does not make sense from
   * either a lazy execution perspective or a cached result perspective.
   */
  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
    throw new RejectedExecutionException("Use another ExecutorService implementation.");
  }

  /**
   * Always throws a RejectedExecutionException because using this method does not make sense from
   * either a lazy execution perspective or a cached result perspective.
   */
  @Override
  public <T> T invokeAny(
      final Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
    throw new RejectedExecutionException("Use another ExecutorService implementation.");
  }

  /**
   * Always throws a RejectedExecutionException because using this method does not make sense from
   * either a lazy execution perspective or a cached result perspective.
   */
  @Override
  public void execute(Runnable command) {
    throw new RejectedExecutionException("Use submit instead of execute.");
  }

  private static interface ExecutingFuture<T> extends Future<T> {
    void backingServiceDied();
  }

  /**
   * Executes the task when get() or isDone() are called, unless the job has been cancelled or the
   * execution service is shutdown.
   */
  private class ExecutingFutureImpl<T> extends ForwardingFuture<T> implements ExecutingFuture<T> {
    private final AtomicReference<ExecutingFuture<T>> state;

    ExecutingFutureImpl(Callable<T> task) {
      state = new AtomicReference<ExecutingFuture<T>>(new Created(task));
    }

    @Override
    protected Future<T> delegate() {
      return state.get();
    }

    @Override
    public void backingServiceDied() {
      state.get().backingServiceDied();
    }

    /*
     * States and transitions are defined such that they guarantee that only
     * one thread will be changing the internal state of the object at a time
     * and no thread will access inconsistent internal state.
     *
     * <p>Simple state changes are just a CAS of the AtomicReference holding the
     * current state. Complex state changes are synchronized on the object whose
     * state is being changed and the object is in the special InbetweenStates
     * state until the internal state is consistent.
     */

    /** Initial state. */
    private class Created implements ExecutingFuture<T> {
      private final Callable<T> task;

      Created(Callable<T> task) {
        this.task = task;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        if (transitionToCancelled()) {
          return true;
        } else {
          return state.get().cancel(mayInterruptIfRunning);
        }
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        transitionToDelegated();
        return state.get().isDone();
      }

      @Override
      public T get() throws ExecutionException, InterruptedException {
        transitionToDelegated();
        return state.get().get();
      }

      @Override
      public T get(long timeout, TimeUnit unit)
          throws ExecutionException, InterruptedException, TimeoutException {
        transitionToDelegated();
        return state.get().get(timeout, unit);
      }

      @Override
      public void backingServiceDied() {
        transitionToCancelled();
      }

      private void transitionToDelegated() {
        InbetweenStates transitionGuard = new InbetweenStates();
        if (state.compareAndSet(this, transitionGuard)) {
          try {
            Future<T> backingFuture =
                backingService.submit(
                    new Callable<T>() {
                      @Override
                      public T call() throws Exception {
                        try {
                          return task.call();
                        } finally {
                          removePendingTask(ExecutingFutureImpl.this);
                        }
                      }
                    });
            state.set(new Delegated(backingFuture));
          } catch (RejectedExecutionException e) {
            state.set(new Cancelled());
            removePendingTask(ExecutingFutureImpl.this);
            checkBackingService();
          } finally {
            transitionGuard.latch.countDown();
          }
        }
      }

      @CanIgnoreReturnValue
      private boolean transitionToCancelled() {
        if (state.compareAndSet(this, new Cancelled())) {
          removePendingTask(ExecutingFutureImpl.this);
          return true;
        }
        return false;
      }
    }

    /** Represents the state where the Future was cancelled before execution started. */
    private class Cancelled implements ExecutingFuture<T> {
      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public boolean isCancelled() {
        return true;
      }

      @Override
      public T get(long timeout, TimeUnit unit) {
        throw new CancellationException();
      }

      @Override
      public T get() {
        throw new CancellationException();
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public void backingServiceDied() {}
    }

    /** Execution has started and everything is delegated to the backingFuture. */
    private class Delegated implements ExecutingFuture<T> {
      private final Future<T> backingFuture;

      Delegated(Future<T> backingFuture) {
        this.backingFuture = backingFuture;
      }

      @Override
      public boolean isDone() {
        return backingFuture.isDone();
      }

      @Override
      public boolean isCancelled() {
        return backingFuture.isCancelled();
      }

      @Override
      public T get(long timeout, TimeUnit unit)
          throws ExecutionException, InterruptedException, TimeoutException {
        return backingFuture.get(timeout, unit);
      }

      @Override
      public T get() throws ExecutionException, InterruptedException {
        return backingFuture.get();
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        if (!mayInterruptIfRunning) {
          // Task is logically running already.
          return false;
        }
        return backingFuture.cancel(mayInterruptIfRunning);
      }

      @Override
      public void backingServiceDied() {}
    }

    /**
     * Temporary state protecting a state transition until the object is in a consistent state. Each
     * method here is synchronized and the thread performing the transition should always be first
     * to hold the lock.
     */
    private class InbetweenStates implements ExecutingFuture<T> {
      public final CountDownLatch latch = new CountDownLatch(1);

      @Override
      public boolean isCancelled() {
        return false; // Can only be true in a terminal state. Not there yet.
      }

      @Override
      public boolean isDone() {
        return false; // Can only be true in a terminal state. Not there yet.
      }

      @Override
      public T get(long timeout, TimeUnit unit)
          throws ExecutionException, InterruptedException, TimeoutException {
        long startWait = System.nanoTime();
        if (!latch.await(timeout, unit)) {
          throw new TimeoutException();
        }
        long endWait = System.nanoTime();
        timeout -= unit.convert(endWait - startWait, NANOSECONDS);
        return state.get().get(timeout, unit);
      }

      @Override
      public T get() throws ExecutionException, InterruptedException {
        latch.await();
        return state.get().get();
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false; // Cancellation may fail for any reason. This is one.
      }

      @Override
      public void backingServiceDied() {
        // Only relevant in the initial Created state, and we've left that.
      }
    }
  }
}
