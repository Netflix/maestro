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

import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.maestro.models.trigger.TriggerUuids;
import java.util.Collections;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class IdHelperTest {

  @Test
  public void testGetOrCreateUuid() {
    UUID expected = UUID.randomUUID();
    Assert.assertEquals(expected.toString(), IdHelper.getOrCreateUuid(expected));
  }

  @Test
  public void testCreateUuid() {
    String expected = "7b92bd31-8478-35db-bb77-dfdb3d7260fc";
    Assert.assertEquals(expected, IdHelper.createUuid("test-uuid1").toString());
    expected = "1e98c90b-0adf-3429-9a53-c8620d70fb5d";
    Assert.assertEquals(expected, IdHelper.createUuid("test-uuid2").toString());
  }

  @Test
  public void testToTriggerUuids() {
    Workflow workflow =
        Workflow.builder()
            .id("test-wf-id")
            .timeTriggers(Collections.singletonList(new CronTimeTrigger()))
            .signalTriggers(Collections.singletonList(new SignalTrigger()))
            .build();
    TriggerUuids triggerUuids = IdHelper.toTriggerUuids(workflow);
    Assert.assertEquals("399e992f-bca3-3cf1-9e1c-f04e7f9ee6f4", triggerUuids.getTimeTriggerUuid());
    Assert.assertEquals(
        Collections.singletonMap("ae3fd022-76e8-3322-b657-0db619b4575f", 0),
        triggerUuids.getSignalTriggerUuids());
  }

  @Test
  public void testHashKey() {
    Assert.assertEquals("29C4", IdHelper.hashKey(1000000L));
    Assert.assertEquals("o9oZ9l1", IdHelper.hashKey(100000000000L));
  }

  @Test
  public void testRangeKey() {
    Assert.assertEquals("44C92", IdHelper.rangeKey(1000000L));
    Assert.assertEquals("71l9Zo9o", IdHelper.rangeKey(100000000000L));
    Assert.assertTrue(IdHelper.rangeKey(1000000L).compareTo(IdHelper.rangeKey(100000000000L)) < 0);
  }

  @Test
  public void testLargeRangeKey() {
    Assert.assertEquals("44C92", IdHelper.rangeKey(1000000L));
    Assert.assertEquals("BAzL8n0Y58m7", IdHelper.rangeKey(Long.MAX_VALUE));
    Assert.assertTrue(IdHelper.rangeKey(1000000L).compareTo(IdHelper.rangeKey(Long.MAX_VALUE)) < 0);
  }
}
