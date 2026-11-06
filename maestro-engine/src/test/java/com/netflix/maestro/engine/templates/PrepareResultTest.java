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
package com.netflix.maestro.engine.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import org.junit.Test;

public class PrepareResultTest {
  @Test
  public void testReady() {
    TemplateExpansion expansion = TemplateExpansion.builder().build();
    PrepareResult result = PrepareResult.ready(expansion, null);
    assertTrue(result.isReady());
    assertEquals(expansion, result.expansion());
    assertEquals(StepRuntime.State.DONE, result.state());
    assertFalse(result.artifactChanged());
    assertNull(result.event());
  }

  @Test
  public void testReadyWithEvent() {
    TimelineLogEvent event = TimelineLogEvent.info("prepared");
    PrepareResult result = PrepareResult.ready(TemplateExpansion.builder().build(), event);
    assertTrue(result.isReady());
    assertEquals(event, result.event());
  }

  @Test
  public void testPending() {
    TimelineLogEvent event = TimelineLogEvent.info("waiting");
    PrepareResult result = PrepareResult.pending(event, true);
    assertFalse(result.isReady());
    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifactChanged());
    assertEquals(event, result.event());
  }

  @Test
  public void testFailed() {
    PrepareResult result = PrepareResult.failed(StepRuntime.State.STOPPED, null);
    assertFalse(result.isReady());
    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertFalse(result.artifactChanged());
  }
}
