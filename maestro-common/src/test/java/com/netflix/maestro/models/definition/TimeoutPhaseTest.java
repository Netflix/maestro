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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import org.junit.Test;

public class TimeoutPhaseTest extends MaestroBaseTest {

  @Test
  public void testClockStart() {
    StepRuntimeState state = new StepRuntimeState();
    state.setCreateTime(1L);
    state.setWaitSignalTime(2L);
    state.setWaitPermitTime(3L);
    state.setStartTime(4L);
    assertEquals(1L, TimeoutPhase.STEP.getClockStart(state).longValue());
    assertEquals(2L, TimeoutPhase.WAITING_FOR_SIGNALS.getClockStart(state).longValue());
    assertEquals(3L, TimeoutPhase.WAITING_FOR_PERMITS.getClockStart(state).longValue());
    assertEquals(4L, TimeoutPhase.RUNNING.getClockStart(state).longValue());
    assertNull(TimeoutPhase.RUNNING.getClockStart(new StepRuntimeState()));
  }

  @Test
  public void testAppliesTo() {
    for (StepInstance.Status status : StepInstance.Status.values()) {
      assertTrue(TimeoutPhase.STEP.appliesTo(status));
      assertEquals(
          status == StepInstance.Status.STARTING
              || status == StepInstance.Status.RUNNING
              || status == StepInstance.Status.FINISHING,
          TimeoutPhase.RUNNING.appliesTo(status));
      assertEquals(
          status == StepInstance.Status.WAITING_FOR_SIGNALS,
          TimeoutPhase.WAITING_FOR_SIGNALS.appliesTo(status));
      assertEquals(
          status == StepInstance.Status.WAITING_FOR_PERMITS,
          TimeoutPhase.WAITING_FOR_PERMITS.appliesTo(status));
    }
    assertFalse(TimeoutPhase.WAITING_FOR_SIGNALS.appliesTo(StepInstance.Status.RUNNING));
  }

  @Test
  public void testCreateIsCaseInsensitive() throws Exception {
    assertEquals(TimeoutPhase.RUNNING, TimeoutPhase.create("running"));
    assertEquals(TimeoutPhase.STEP, MAPPER.readValue("\"step\"", TimeoutPhase.class));
    assertEquals(
        "\"WAITING_FOR_SIGNALS\"", MAPPER.writeValueAsString(TimeoutPhase.WAITING_FOR_SIGNALS));
  }
}
