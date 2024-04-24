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
package com.netflix.maestro.models.trigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimeTriggerTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerdeCron() throws Exception {
    TimeTrigger expected =
        loadObject("fixtures/time_triggers/sample-cron-time-trigger.json", TimeTrigger.class);
    assertEquals(TimeTrigger.Type.CRON, expected.getType());
    String ser1 = MAPPER.writeValueAsString(expected);
    TimeTrigger actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), TimeTrigger.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(ser1, ser2);
    assertEquals("US/Pacific", actual.getTimezone());
  }

  @Test
  public void testRoundTripSerdeInterval() throws Exception {
    TimeTrigger expected =
        loadObject("fixtures/time_triggers/sample-interval-time-trigger.json", TimeTrigger.class);
    assertEquals(TimeTrigger.Type.INTERVAL, expected.getType());
    String ser1 = MAPPER.writeValueAsString(expected);
    TimeTrigger actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), TimeTrigger.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdePredefined() throws Exception {
    TimeTrigger expected =
        loadObject("fixtures/time_triggers/sample-predefined-time-trigger.json", TimeTrigger.class);
    assertEquals(TimeTrigger.Type.PREDEFINED, expected.getType());
    String ser1 = MAPPER.writeValueAsString(expected);
    TimeTrigger actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), TimeTrigger.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(ser1, ser2);
    assertTrue(ser1.contains("@daily"));
  }
}
