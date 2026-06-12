/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.engine.concurrency;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.maestro.engine.properties.ThreadingModel;
import com.netflix.maestro.engine.properties.ThreadingProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class EngineExecutorsTest {

  private static final int CONCURRENT_TASKS = 16;
  private static final long PER_TASK_SLEEP_MILLIS = 100;

  @Test
  public void virtualThreadsRunIOTasksConcurrently() throws Exception {
    ThreadingProperties props = new ThreadingProperties();
    props.setModel(ThreadingModel.VIRTUAL);

    ExecutorService executor = EngineExecutors.newIoExecutor(props);
    try {
      long elapsed = runConcurrentSleepTasks(executor);
      // With virtual threads, all 16 tasks should run concurrently. The total
      // wall-clock time should be much less than the serialized 16 * 100 ms = 1600 ms
      // lower bound. A generous bound accommodates JIT and scheduling jitter.
      assertThat(elapsed)
          .as("virtual threads should not serialize independent I/O-bound tasks")
          .isLessThan(CONCURRENT_TASKS * PER_TASK_SLEEP_MILLIS / 2L);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void platformThreadPoolCapsConcurrencyAtConfiguredSize() throws Exception {
    ThreadingProperties props = new ThreadingProperties();
    props.setModel(ThreadingModel.PLATFORM);
    props.setPlatformThreadPoolSize(2);

    ExecutorService executor = EngineExecutors.newIoExecutor(props);
    try {
      long elapsed = runConcurrentSleepTasks(executor);
      // With a 2-thread pool and 16 tasks each sleeping 100 ms, the total wall-clock
      // time must be at least 16/2 * 100 = 800 ms. We allow some headroom.
      long lowerBound =
          ((long) Math.ceil((double) CONCURRENT_TASKS / props.getPlatformThreadPoolSize()))
              * PER_TASK_SLEEP_MILLIS;
      assertThat(elapsed)
          .as("bounded platform-thread pool must serialize tasks beyond pool size")
          .isGreaterThanOrEqualTo(lowerBound - 50L);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void virtualThreadExecutorUsesVirtualThreads() throws Exception {
    ThreadingProperties props = new ThreadingProperties();
    props.setModel(ThreadingModel.VIRTUAL);

    ExecutorService executor = EngineExecutors.newIoExecutor(props);
    try {
      Future<Boolean> isVirtual = executor.submit(() -> Thread.currentThread().isVirtual());
      assertThat(isVirtual.get(1, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdownNow();
    }
  }

  private static long runConcurrentSleepTasks(ExecutorService executor)
      throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Long>> futures = new ArrayList<>();
    long startNanos = System.nanoTime();
    for (int i = 0; i < CONCURRENT_TASKS; i++) {
      futures.add(
          executor.submit(
              () -> {
                start.await();
                Thread.sleep(PER_TASK_SLEEP_MILLIS);
                return Thread.currentThread().threadId();
              }));
    }
    start.countDown();
    for (Future<Long> f : futures) {
      f.get(5, TimeUnit.SECONDS);
    }
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }
}
