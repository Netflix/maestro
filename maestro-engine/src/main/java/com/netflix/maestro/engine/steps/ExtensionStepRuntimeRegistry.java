/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.engine.steps;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of the step runtimes behind {@link StepType#EXTENSION} steps, keyed by step sub type.
 *
 * <p>A deployment registers one runtime per sub type at startup, for example {@code
 * register("spark", sparkRuntime)}. {@link ExtensionStepRuntime} then runs every extension step
 * whose sub type, or whose job template ancestor, is registered here. Registration is expected to
 * finish before any step runs; the registry is thread safe but does not order registrations against
 * lookups.
 */
public final class ExtensionStepRuntimeRegistry {
  private final Map<String, StepRuntime> runtimes = new ConcurrentHashMap<>();

  /**
   * Register the runtime for a sub type.
   *
   * @param subType the step sub type, which must not already be registered
   * @param runtime the runtime that executes steps of this sub type
   */
  public void register(String subType, StepRuntime runtime) {
    Checks.notNull(subType, "Extension step runtime sub type cannot be null");
    Checks.notNull(runtime, "Extension step runtime for sub type [%s] cannot be null", subType);
    StepRuntime existing = runtimes.putIfAbsent(subType, runtime);
    Checks.checkTrue(
        existing == null,
        "Extension step runtime for sub type [%s] is already registered",
        subType);
  }

  /**
   * Look up the runtime registered for a sub type.
   *
   * @param subType the step sub type
   * @return the registered runtime, or null if the sub type has none
   */
  @Nullable
  public StepRuntime lookup(@Nullable String subType) {
    return subType == null ? null : runtimes.get(subType);
  }

  /** Returns the registered sub types. */
  public Set<String> subTypes() {
    return Collections.unmodifiableSet(runtimes.keySet());
  }
}
