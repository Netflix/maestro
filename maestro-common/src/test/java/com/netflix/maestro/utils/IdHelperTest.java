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
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.signal.SignalParamValue;
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
        Collections.singletonMap("6a180223-3858-3c96-bb20-83682dd19132", 0),
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

  @Test
  public void testDeriveGroupId() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("sample-dag-test-1");
    instance.setWorkflowInstanceId(12);
    instance.setWorkflowRunId(2);
    Assert.assertEquals(0, IdHelper.deriveGroupId(instance));

    instance.setGroupInfo(10);
    Assert.assertEquals(9, IdHelper.deriveGroupId(instance));

    Assert.assertEquals(0, IdHelper.deriveGroupId("test-key", 3));
    Assert.assertTrue("negative-test".hashCode() < 0);
    Assert.assertEquals(2, IdHelper.deriveGroupId("negative-test", 3));
  }

  @Test
  public void testEncodeValue() {
    var v01 = IdHelper.encodeValue(SignalParamValue.of(Long.MAX_VALUE));
    var v02 = IdHelper.encodeValue(SignalParamValue.of(125));
    var v03 = IdHelper.encodeValue(SignalParamValue.of(123));
    var v04 = IdHelper.encodeValue(SignalParamValue.of(12));
    var v05 = IdHelper.encodeValue(SignalParamValue.of(1));
    var v06 = IdHelper.encodeValue(SignalParamValue.of(0));
    var v07 = IdHelper.encodeValue(SignalParamValue.of(-1));
    var v08 = IdHelper.encodeValue(SignalParamValue.of(-12));
    var v09 = IdHelper.encodeValue(SignalParamValue.of(-123));
    var v10 = IdHelper.encodeValue(SignalParamValue.of(-125));
    var v11 = IdHelper.encodeValue(SignalParamValue.of(-Long.MAX_VALUE));
    var v12 = IdHelper.encodeValue(SignalParamValue.of(Long.MIN_VALUE));
    var v13 = IdHelper.encodeValue(SignalParamValue.of("hello"));
    var v14 = IdHelper.encodeValue(SignalParamValue.of("foo"));
    var v15 = IdHelper.encodeValue(SignalParamValue.of("bar"));

    Assert.assertEquals("mAzL8n0Y58m7", v01);
    Assert.assertEquals("d21", v02);
    Assert.assertEquals("d1z", v03);
    Assert.assertEquals("cC", v04);
    Assert.assertEquals("c1", v05);
    Assert.assertEquals("a0", v06);
    Assert.assertEquals("BAzL8n0Y58m6", v07);
    Assert.assertEquals("BAzL8n0Y58lv", v08);
    Assert.assertEquals("BAzL8n0Y58k8", v09);
    Assert.assertEquals("BAzL8n0Y58k6", v10);
    Assert.assertEquals("01", v11);
    Assert.assertEquals("00", v12);
    Assert.assertEquals("#hello", v13);
    Assert.assertEquals("#foo", v14);
    Assert.assertEquals("#bar", v15);

    Assert.assertTrue(v01.compareTo(v02) > 0);
    Assert.assertTrue(v02.compareTo(v03) > 0);
    Assert.assertTrue(v03.compareTo(v04) > 0);
    Assert.assertTrue(v04.compareTo(v05) > 0);
    Assert.assertTrue(v05.compareTo(v06) > 0);
    Assert.assertTrue(v06.compareTo(v07) > 0);
    Assert.assertTrue(v07.compareTo(v08) > 0);
    Assert.assertTrue(v08.compareTo(v09) > 0);
    Assert.assertTrue(v09.compareTo(v10) > 0);
    Assert.assertTrue(v11.compareTo(v12) > 0);
    Assert.assertTrue(v12.compareTo("0") > 0);
    Assert.assertTrue("0".compareTo(v13) > 0);
    Assert.assertTrue(v13.compareTo(v14) > 0);
    Assert.assertTrue(v14.compareTo(v15) > 0);
  }
}
