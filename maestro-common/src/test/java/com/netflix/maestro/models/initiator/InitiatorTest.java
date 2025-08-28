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
package com.netflix.maestro.models.initiator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import lombok.Data;
import org.junit.BeforeClass;
import org.junit.Test;

public class InitiatorTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Data
  private static class Initiators {
    Initiator manual1;
    Initiator manual2;
    Initiator subworkflow;
    Initiator foreach;
    Initiator template;
    Initiator cron;
    Initiator signal;
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    Initiators expected = loadObject("fixtures/initiator/sample-initiators.json", Initiators.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    Initiators actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), Initiators.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testInitiatorTimeline() throws Exception {
    Initiators initiators =
        loadObject("fixtures/initiator/sample-initiators.json", Initiators.class);
    assertEquals(
        "Manually run a new workflow instance by a user [unknown]",
        initiators.getManual1().getTimelineEvent().getMessage());
    assertEquals(
        "Manually run a new workflow instance by a user [tester]",
        initiators.getManual2().getTimelineEvent().getMessage());
    assertEquals(
        "SUBWORKFLOW step ([test-parent][1][1][test-step][1]) runs a new workflow instance",
        initiators.getSubworkflow().getTimelineEvent().getMessage());
    assertEquals(
        "FOREACH step ([maestro_foreach-parent][1][1][test-step][1]) runs a new workflow instance",
        initiators.getForeach().getTimelineEvent().getMessage());
    assertEquals(
        "TEMPLATE step ([test-parent][1][1][test-step][1]) runs a new workflow instance",
        initiators.getTemplate().getTimelineEvent().getMessage());
    assertEquals(
        "TIME-based trigger runs a new workflow instance by trigger uuid [foo]",
        initiators.getCron().getTimelineEvent().getMessage());
    assertEquals(
        "SIGNAL-based trigger runs a new workflow instance by trigger uuid [bar]",
        initiators.getSignal().getTimelineEvent().getMessage());
  }

  @Test
  public void testInitiatorAncestors() throws Exception {
    Initiators initiators =
        loadObject("fixtures/initiator/sample-initiators.json", Initiators.class);
    assertEquals(
        "test-parent",
        ((SubworkflowInitiator) initiators.getSubworkflow()).getNonInlineParent().getWorkflowId());
    assertEquals(
        "test-parent",
        ((ForeachInitiator) initiators.getForeach()).getNonInlineParent().getWorkflowId());
    assertEquals(
        "test-parent",
        ((TemplateInitiator) initiators.getTemplate()).getNonInlineParent().getWorkflowId());
  }

  @Test
  public void testInitiatorAncestorSyncFlag() throws Exception {
    Initiators initiators =
        loadObject("fixtures/initiator/sample-initiators.json", Initiators.class);
    assertTrue(initiators.getSubworkflow().getParent().isAsync());
    assertFalse(initiators.getSubworkflow().getParent().getSync());
    assertFalse(((UpstreamInitiator) initiators.getSubworkflow()).getRoot().isAsync());
    assertNull(((UpstreamInitiator) initiators.getSubworkflow()).getRoot().getSync());
    assertFalse(initiators.getForeach().getParent().isAsync());
    assertNull(initiators.getForeach().getParent().getSync());
    assertFalse(initiators.getTemplate().getParent().isAsync());
    assertTrue(initiators.getTemplate().getParent().getSync());
    assertFalse(((UpstreamInitiator) initiators.getTemplate()).getRoot().isAsync());
    assertNull(((UpstreamInitiator) initiators.getTemplate()).getRoot().getSync());
  }
}
