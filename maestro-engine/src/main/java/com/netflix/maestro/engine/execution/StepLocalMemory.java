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
package com.netflix.maestro.engine.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transient in-memory storage for step runtimes to store step-instance-scoped states. States are
 * NOT persisted and will be lost on JVM reboot.
 */
public final class StepLocalMemory {
  private static final Map<String, Map<String, Object>> MEMORY_MAP = new ConcurrentHashMap<>();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long SIZE_LIMIT_BYTES = 64 * 1024; // 64 KB limit

  private StepLocalMemory() {}

  /**
   * Get or create a transient memory map for the specified step instance. Scoped to the step
   * instance run.
   */
  public static Map<String, Object> getOrCreate(String stepInstanceUuid) {
    if (stepInstanceUuid == null) {
      return new ConcurrentHashMap<>();
    }
    return MEMORY_MAP.computeIfAbsent(
        stepInstanceUuid,
        k ->
            new ConcurrentHashMap<String, Object>() {
              @Override
              public Object put(String key, Object value) {
                Object old = super.put(key, value);
                checkSize(this);
                return old;
              }

              @Override
              public void putAll(Map<? extends String, ?> m) {
                super.putAll(m);
                checkSize(this);
              }

              @Override
              public Object putIfAbsent(String key, Object value) {
                Object old = super.putIfAbsent(key, value);
                checkSize(this);
                return old;
              }
            });
  }

  /** Remove the step instance memory map. */
  public static void remove(String stepInstanceUuid) {
    if (stepInstanceUuid != null) {
      MEMORY_MAP.remove(stepInstanceUuid);
    }
  }

  private static void checkSize(Map<String, Object> map) {
    try {
      byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(map);
      if (bytes.length > SIZE_LIMIT_BYTES) {
        throw new IllegalArgumentException(
            String.format(
                "Step local memory size limit exceeded: %d bytes (limit: %d bytes)",
                bytes.length, SIZE_LIMIT_BYTES));
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to check step local memory size limit", e);
    }
  }
}
