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
import static org.junit.Assert.assertNull;

import com.netflix.maestro.MaestroBaseTest;
import java.util.List;
import org.junit.Test;

public class StepTimeoutsTest extends MaestroBaseTest {
  private static final long WAITING_FOR_PERMITS_SECS = 7200L;

  @Test
  public void testRoundTripSerde() throws Exception {
    StepTimeouts timeouts =
        loadObject("fixtures/step_timeouts/sample-step-timeouts.json", StepTimeouts.class);
    assertEquals(
        timeouts, MAPPER.readValue(MAPPER.writeValueAsString(timeouts), StepTimeouts.class));
    assertEquals("24h", timeouts.getStep().asString());
    assertEquals("4 hours", timeouts.getWaitingForSignals().asString());
    assertEquals(WAITING_FOR_PERMITS_SECS, timeouts.getWaitingForPermits().getLong());
    assertEquals("8h", timeouts.getRunning().asString());
  }

  @Test
  public void testToMapKeepsPhaseOrder() throws Exception {
    StepTimeouts timeouts =
        loadObject("fixtures/step_timeouts/sample-step-timeouts.json", StepTimeouts.class);
    assertEquals(List.of(TimeoutPhase.values()), List.copyOf(timeouts.toMap().keySet()));
    assertEquals(timeouts.getRunning(), timeouts.toMap().get(TimeoutPhase.RUNNING));
  }

  @Test
  public void testParsableValues() throws Exception {
    StepTimeouts timeouts =
        loadObject("fixtures/step_timeouts/sample-parsable-step-timeouts.json", StepTimeouts.class);
    assertEquals("${step_timeout}", timeouts.getStep().asString());
    assertEquals("28800", timeouts.getRunning().asString());
    assertNull(timeouts.getWaitingForSignals());
    assertNull(timeouts.getWaitingForPermits());
    assertEquals(
        List.of(TimeoutPhase.STEP, TimeoutPhase.RUNNING), List.copyOf(timeouts.toMap().keySet()));
  }

  @Test
  public void testEmptyTimeouts() {
    StepTimeouts timeouts = StepTimeouts.builder().build();
    assertNull(timeouts.getStep());
    assertEquals(0, timeouts.toMap().size());
  }
}
