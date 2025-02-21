/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.timetrigger.models;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class TimeTriggerExecutionTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    TimeTriggerExecution expected =
        loadObject(
            "fixtures/sample-workflow-time-trigger-execution.json", TimeTriggerExecution.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    TimeTriggerExecution actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), TimeTriggerExecution.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }
}
