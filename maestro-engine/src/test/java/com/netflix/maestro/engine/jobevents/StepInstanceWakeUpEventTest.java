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
package com.netflix.maestro.engine.jobevents;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class StepInstanceWakeUpEventTest extends MaestroEngineBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    StepInstanceWakeUpEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-step-instance-wake-up-job-event.json",
            StepInstanceWakeUpEvent.class);
    Assert.assertEquals(
        sampleEvent,
        MAPPER.readValue(MAPPER.writeValueAsString(sampleEvent), StepInstanceWakeUpEvent.class));
  }
}
