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

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.models.error.Details;
import java.util.Optional;

/** Interface for instance_step_concurrency handler. */
public interface InstanceStepConcurrencyHandler {
  /** A noop instance_step_concurrency handler. */
  InstanceStepConcurrencyHandler NOOP_CONCURRENCY_HANDLER = new InstanceStepConcurrencyHandler() {};

  /** Add a step uuid into the uuids for a given concurrencyId. */
  default Optional<Details> registerStep(String concurrencyId, String uuid) {
    return Optional.empty();
  }

  /**
   * Unregister a step uuid from the uuids for a given concurrencyId. If failed, it throws a
   * MaestroRetryableError.
   */
  default void unregisterStep(String concurrencyId, String uuid) {}

  /**
   * Add an instance uuid into the uuids for a given concurrencyId with depth. It will check if
   * either instance uuid set or step uuids reach the limit. If yes, return false; otherwise return
   * true. If there is any exception, also return false.
   */
  default boolean addInstance(RunRequest runRequest) {
    return true;
  }

  /**
   * Remove an instance uuid from the uuids for a given concurrencyId with depth. If failed, it
   * throws a MaestroRetryableError.
   */
  default void removeInstance(String concurrencyId, int depth, String uuid) {}

  /** Delete all created data for a given dag tree with a given concurrencyId. */
  default void cleanUp(String concurrencyId) {}
}
