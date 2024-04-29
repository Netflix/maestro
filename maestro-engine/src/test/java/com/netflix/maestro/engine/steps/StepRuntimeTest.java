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
package com.netflix.maestro.engine.steps;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class StepRuntimeTest extends MaestroEngineBaseTest {

  @Test
  public void testStateIsFailed() {
    Assert.assertFalse(StepRuntime.State.CONTINUE.isFailed());
    Assert.assertFalse(StepRuntime.State.DONE.isFailed());
    Assert.assertTrue(StepRuntime.State.USER_ERROR.isFailed());
    Assert.assertTrue(StepRuntime.State.PLATFORM_ERROR.isFailed());
    Assert.assertTrue(StepRuntime.State.FATAL_ERROR.isFailed());
    Assert.assertFalse(StepRuntime.State.STOPPED.isFailed());
    Assert.assertTrue(StepRuntime.State.TIMED_OUT.isFailed());
  }

  @Test
  public void testStateIsTerminal() {
    Assert.assertFalse(StepRuntime.State.CONTINUE.isTerminal());
    Assert.assertTrue(StepRuntime.State.DONE.isTerminal());
    Assert.assertTrue(StepRuntime.State.USER_ERROR.isTerminal());
    Assert.assertTrue(StepRuntime.State.PLATFORM_ERROR.isTerminal());
    Assert.assertTrue(StepRuntime.State.FATAL_ERROR.isTerminal());
    Assert.assertTrue(StepRuntime.State.STOPPED.isTerminal());
    Assert.assertTrue(StepRuntime.State.TIMED_OUT.isTerminal());
  }
}
