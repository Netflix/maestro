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
package com.netflix.maestro.utils;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.trigger.TimeTrigger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import org.junit.Test;

public class TriggerHelperTest extends MaestroBaseTest {

  @Test
  public void testNextExecutionDateForCron() throws Exception {
    TimeTrigger trigger =
        loadObject("fixtures/time_triggers/sample-cron-time-trigger.json", TimeTrigger.class);
    Optional<ZonedDateTime> actual =
        TriggerHelper.nextExecutionDate(trigger, Date.from(Instant.EPOCH), "test-id");
    ZonedDateTime expected =
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(72000), ZoneId.of(trigger.getTimezone()));
    assertEquals(Optional.of(expected), actual);
  }

  @Test
  public void testNextExecutionDateForPredefined() throws Exception {
    TimeTrigger trigger =
        loadObject("fixtures/time_triggers/sample-predefined-time-trigger.json", TimeTrigger.class);
    Optional<ZonedDateTime> actual =
        TriggerHelper.nextExecutionDate(trigger, Date.from(Instant.EPOCH), "test-id");
    ZonedDateTime expected =
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(28800), ZoneId.of(trigger.getTimezone()));
    assertEquals(Optional.of(expected), actual);
  }

  @Test
  public void testNextExecutionDateForInterval() throws Exception {
    TimeTrigger trigger =
        loadObject("fixtures/time_triggers/sample-interval-time-trigger.json", TimeTrigger.class);

    AssertHelper.assertThrows(
        "TimeTrigger nextExecutionDate is not implemented",
        UnsupportedOperationException.class,
        "TimeTrigger nextExecutionDate is not implemented for type: INTERVAL",
        () -> TriggerHelper.nextExecutionDate(trigger, Date.from(Instant.EPOCH), "test-id"));
  }
}
