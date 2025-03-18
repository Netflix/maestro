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
package com.netflix.maestro.timetrigger.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TimeTriggerSubscriptionClientTest extends MaestroBaseTest {

  private TimeTriggerProducer triggerProducer;
  private TimeTriggerSubscriptionClient triggerClient;

  @Before
  public void setup() {
    triggerProducer = Mockito.mock(TimeTriggerProducer.class);
    triggerClient =
        new TimeTriggerSubscriptionClient(
            triggerProducer, new MaestroMetricRepo(new DefaultRegistry()));
  }

  @Test
  public void testUpsertTriggerSubscription() {
    CronTimeTrigger cronTrigger = new CronTimeTrigger();
    cronTrigger.setCron("0 0 0 0 0 0");
    Workflow wf1 =
        Workflow.builder().id("wf1").timeTriggers(Collections.singletonList(cronTrigger)).build();
    triggerClient.upsertTriggerSubscription(
        null, wf1, TriggerUuids.builder().timeTriggerUuid("test-uuid").build(), null);
    var requestCaptor = ArgumentCaptor.forClass(TimeTriggerExecution.class);
    Mockito.verify(triggerProducer, Mockito.times(1)).push(requestCaptor.capture(), anyInt());
    TimeTriggerExecution actual = requestCaptor.getValue();
    Assert.assertEquals("wf1", actual.getWorkflowId());
    Assert.assertEquals("test-uuid", actual.getWorkflowTriggerUuid());
    Assert.assertEquals(1, actual.getTimeTriggersWithWatermarks().size());
    Assert.assertEquals(
        cronTrigger, actual.getTimeTriggersWithWatermarks().getFirst().getTimeTrigger());
  }

  @Test
  public void testUpsertTriggerSubscriptionWithEmptyTimeTrigger() {
    Workflow wf1 = Workflow.builder().id("wf1").timeTriggers(Collections.emptyList()).build();
    triggerClient.upsertTriggerSubscription(
        null, wf1, TriggerUuids.builder().timeTriggerUuid("test-uuid").build(), null);
    Mockito.verify(triggerProducer, Mockito.times(0)).push(any(), anyInt());
  }

  @Test
  public void testUpsertTriggerSubscriptionWithSameTimeTrigger() {
    CronTimeTrigger cronTrigger = new CronTimeTrigger();
    cronTrigger.setCron("0 0 0 0 0 0");
    Workflow wf1 =
        Workflow.builder().id("wf1").timeTriggers(Collections.singletonList(cronTrigger)).build();
    triggerClient.upsertTriggerSubscription(
        null,
        wf1,
        TriggerUuids.builder().timeTriggerUuid("test-uuid").build(),
        TriggerUuids.builder().timeTriggerUuid("test-uuid").build());
    Mockito.verify(triggerProducer, Mockito.times(0)).push(any(), anyInt());
  }
}
