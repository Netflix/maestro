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

import com.netflix.maestro.engine.properties.ThreadingModel;
import com.netflix.maestro.engine.properties.ThreadingProperties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for engine I/O executors.
 *
 * <p>Selects between Java 21 virtual threads and a bounded platform-thread pool based on {@link
 * ThreadingProperties}. Virtual threads lift the previous hard cap on concurrent in-flight JDBC and
 * signal lookups while preserving a one-line rollback path.
 */
@Slf4j
public final class EngineExecutors {
  private EngineExecutors() {}

  /**
   * Build a fresh I/O-bound executor based on the supplied threading properties.
   *
   * <p>Callers own the returned executor's lifecycle; call {@link ExecutorService#shutdown()} on
   * application stop.
   *
   * @param props the threading properties; must not be null
   * @return a new executor service
   */
  public static ExecutorService newIoExecutor(ThreadingProperties props) {
    ThreadingModel model = props.getModel() == null ? ThreadingModel.VIRTUAL : props.getModel();
    if (model == ThreadingModel.VIRTUAL) {
      String prefix =
          props.getVirtualThreadNamePrefix() == null
              ? "maestro-vt-"
              : props.getVirtualThreadNamePrefix();
      LOG.info("Creating virtual-thread I/O executor (name-prefix={})", prefix);
      return Executors.newThreadPerTaskExecutor(virtualThreadFactory(prefix));
    }
    int size = Math.max(1, props.getPlatformThreadPoolSize());
    LOG.info("Creating platform-thread I/O executor (size={})", size);
    return Executors.newFixedThreadPool(size, daemonThreadFactory("maestro-io-"));
  }

  private static ThreadFactory virtualThreadFactory(String prefix) {
    AtomicLong seq = new AtomicLong();
    return runnable -> {
      Thread t = Thread.ofVirtual().name(prefix + seq.incrementAndGet()).unstarted(runnable);
      return t;
    };
  }

  private static ThreadFactory daemonThreadFactory(String prefix) {
    AtomicLong seq = new AtomicLong();
    return runnable -> {
      Thread t = new Thread(runnable, prefix + seq.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }
}
