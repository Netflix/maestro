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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import java.util.Set;
import org.junit.Test;

public class ExtensionStepRuntimeRegistryTest extends MaestroBaseTest {
  private final ExtensionStepRuntimeRegistry registry = new ExtensionStepRuntimeRegistry();
  private final StepRuntime runtime = new NoOpStepRuntime();

  @Test
  public void testRegisterAndLookup() {
    registry.register("spark", runtime);
    assertSame(runtime, registry.lookup("spark"));
    assertNull(registry.lookup("flink"));
    assertNull(registry.lookup(null));
    assertEquals(Set.of("spark"), registry.subTypes());
  }

  @Test
  public void testRegisterDuplicateSubType() {
    registry.register("spark", runtime);
    AssertHelper.assertThrows(
        "duplicate registration should throw",
        IllegalArgumentException.class,
        "Extension step runtime for sub type [spark] is already registered",
        () -> registry.register("spark", new SleepStepRuntime()));
    assertSame(runtime, registry.lookup("spark"));
  }

  @Test
  public void testRegisterInvalidArguments() {
    AssertHelper.assertThrows(
        "null sub type should throw",
        NullPointerException.class,
        "Extension step runtime sub type cannot be null",
        () -> registry.register(null, runtime));
    AssertHelper.assertThrows(
        "null runtime should throw",
        NullPointerException.class,
        "Extension step runtime for sub type [spark] cannot be null",
        () -> registry.register("spark", null));
  }
}
