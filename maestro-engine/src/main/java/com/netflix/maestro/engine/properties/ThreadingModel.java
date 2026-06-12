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
package com.netflix.maestro.engine.properties;

/**
 * Threading model selection for engine I/O executors.
 *
 * <p>Used to gate a safe, incremental rollout of Java 21 virtual threads. {@link #VIRTUAL} is the
 * recommended default; {@link #PLATFORM} is preserved as a rollback target for environments where
 * virtual threads are not yet desirable.
 */
public enum ThreadingModel {
  /** Java 21 virtual threads via {@link java.util.concurrent.Executors#newThreadPerTaskExecutor}. */
  VIRTUAL,

  /**
   * Legacy bounded platform-thread pool, retained only for safe rollback. Hard-caps the number of
   * concurrent in-flight I/O calls and is therefore a throughput ceiling.
   */
  PLATFORM
}
