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
package com.netflix.maestro.engine.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowSummaryTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  private void testRoundTripSerdeHelper(String filePath) throws Exception {
    WorkflowSummary expected = loadObject(filePath, WorkflowSummary.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    WorkflowSummary actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), WorkflowSummary.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdeForParams() throws Exception {
    testRoundTripSerdeHelper("fixtures/parameters/sample-wf-summary-params.json");
  }

  @Test
  public void testRoundTripSerdeForRestart() throws Exception {
    testRoundTripSerdeHelper("fixtures/parameters/sample-wf-summary-restart-config.json");
  }

  @Test
  public void testMapParamEvaluation() throws Exception {
    WorkflowSummary summary =
        loadObject("fixtures/parameters/sample-wf-summary-params.json", WorkflowSummary.class);

    Map<String, Map<String, ParamDefinition>> stepRunParams = summary.getStepRunParams();
    assertEquals("bar", stepRunParams.get("step1").get("foo").asStringParamDef().getValue());

    Map<String, Parameter> params = summary.getParams();
    assertEquals("bar", params.get("p1").asMapParam().getEvaluatedParam("l1").asString());
    assertEquals(
        1598399975650L, params.get("p1").asMapParam().getEvaluatedParam("l2").asLong().longValue());
    assertEquals(1.23, params.get("p1").asMapParam().getEvaluatedParam("l3").asDouble(), 0.00001);
    assertEquals(true, params.get("p1").asMapParam().getEvaluatedParam("l4").asBoolean());
    assertEquals(
        Collections.singletonMap("foo", "bar"),
        params.get("p1").asMapParam().getEvaluatedParam("l5").asStringMap());
    assertTrue(
        Objects.deepEquals(
            new String[] {"a", "b", "c"},
            params.get("p1").asMapParam().getEvaluatedParam("l6").asStringArray()));
    assertTrue(
        Objects.deepEquals(
            new long[] {1, 2, 3},
            params.get("p1").asMapParam().getEvaluatedParam("l7").asLongArray()));
    assertTrue(
        Objects.deepEquals(
            new double[] {1.1, 2.2, 3.3},
            params.get("p1").asMapParam().getEvaluatedParam("l8").asDoubleArray()));
    assertTrue(
        Objects.deepEquals(
            new boolean[] {true, false, true},
            params.get("p1").asMapParam().getEvaluatedParam("l9").asBooleanArray()));
    assertEquals("bar", params.get("p1").asMapParam().getEvaluatedParam("e1").asString());
    assertEquals(2L, params.get("p1").asMapParam().getEvaluatedParam("e2").asLong().longValue());
    assertEquals(1.23, params.get("p1").asMapParam().getEvaluatedParam("e3").asDouble(), 0.00001);
    assertEquals(true, params.get("p1").asMapParam().getEvaluatedParam("e4").asBoolean());
    assertEquals(
        Collections.singletonMap("foo", "bar"),
        params.get("p1").asMapParam().getEvaluatedParam("e5").asStringMap());
    assertTrue(
        Objects.deepEquals(
            new String[] {"a", "b", "c"},
            params.get("p1").asMapParam().getEvaluatedParam("e6").asStringArray()));
    assertTrue(
        Objects.deepEquals(
            new long[] {1, 2, 3},
            params.get("p1").asMapParam().getEvaluatedParam("e7").asLongArray()));
    assertTrue(
        Objects.deepEquals(
            new double[] {1.1, 2.2, 3.3},
            params.get("p1").asMapParam().getEvaluatedParam("e8").asDoubleArray()));
    assertTrue(
        Objects.deepEquals(
            new boolean[] {true, false, true},
            params.get("p1").asMapParam().getEvaluatedParam("e9").asBooleanArray()));

    // nested map
    assertEquals(
        "bar",
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("l1")
            .asString());
    assertEquals(
        1598399975650L,
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("l2")
            .asLong()
            .longValue());
    assertEquals(
        1.23,
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("l3")
            .asDouble(),
        0.00001);
    assertEquals(
        true,
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("l4")
            .asBoolean());
    assertEquals(
        Collections.singletonMap("foo", "bar"),
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("l5")
            .asStringMap());
    assertTrue(
        Objects.deepEquals(
            new String[] {"a", "b", "c"},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("l6")
                .asStringArray()));
    assertTrue(
        Objects.deepEquals(
            new long[] {1, 2, 3},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("l7")
                .asLongArray()));
    assertTrue(
        Objects.deepEquals(
            new double[] {1.1, 2.2, 3.3},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("l8")
                .asDoubleArray()));
    assertTrue(
        Objects.deepEquals(
            new boolean[] {true, false, true},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("l9")
                .asBooleanArray()));
    assertEquals(
        "bar",
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("e1")
            .asString());
    assertEquals(
        123L,
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("e2")
            .asLong()
            .longValue());
    assertEquals(
        1.23,
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("e3")
            .asDouble(),
        0.00001);
    assertEquals(
        true,
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("e4")
            .asBoolean());
    assertEquals(
        Collections.singletonMap("foo", "bar"),
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedParam("e5")
            .asStringMap());
    assertTrue(
        Objects.deepEquals(
            new String[] {"a", "b", "c"},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("e6")
                .asStringArray()));
    assertTrue(
        Objects.deepEquals(
            new long[] {1, 2, 3},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("e7")
                .asLongArray()));
    assertTrue(
        Objects.deepEquals(
            new double[] {1.1, 2.2, 3.3},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("e8")
                .asDoubleArray()));
    assertTrue(
        Objects.deepEquals(
            new boolean[] {true, false, true},
            params
                .get("p1")
                .asMapParam()
                .getEvaluatedParam("l10")
                .asMapParam()
                .getEvaluatedParam("e9")
                .asBooleanArray()));

    // doubly nested empty map
    assertEquals(
        Collections.<String, Object>emptyMap(),
        params
            .get("p1")
            .asMapParam()
            .getEvaluatedParam("l10")
            .asMapParam()
            .getEvaluatedResultForParam("l10"));
    assertEquals(
        "{\"bar\":123,\"bat\":{\"baz\":[1,2,3]},\"foo\":\"bar\"}",
        MAPPER.writeValueAsString(params.get("e1").getEvaluatedResult()));
  }

  @Test
  public void testDeriveInstanceStepConcurrency() throws Exception {
    WorkflowSummary summary =
        loadObject("fixtures/parameters/sample-wf-summary-params.json", WorkflowSummary.class);

    // use default if no concurrences are set
    assertEquals(Defaults.DEFAULT_STEP_CONCURRENCY, summary.deriveInstanceStepConcurrency());

    // use properties' step concurrency
    summary.getRunProperties().setStepConcurrency(10L);
    assertEquals(10L, summary.deriveInstanceStepConcurrency());

    // use workflow instance step concurrency
    summary.getRunProperties().setStepConcurrency(null);
    summary.setInstanceStepConcurrency(20L);
    assertEquals(20L, summary.deriveInstanceStepConcurrency());

    // use the min of both concurrences
    summary.getRunProperties().setStepConcurrency(10L);
    summary.setInstanceStepConcurrency(20L);
    assertEquals(10L, summary.deriveInstanceStepConcurrency());
  }

  @Test
  public void testDeriveRuntimeTagPermits() throws Exception {
    WorkflowSummary summary =
        loadObject("fixtures/parameters/sample-wf-summary-params.json", WorkflowSummary.class);
    summary.setCorrelationId("correlation_id");

    Tag t1 = new Tag();
    t1.setName(Constants.MAESTRO_PREFIX + summary.getWorkflowId());
    t1.setPermit((int) Defaults.DEFAULT_STEP_CONCURRENCY);
    Tag t2 = new Tag();
    t2.setName(Constants.MAESTRO_PREFIX + summary.getWorkflowId() + ":stepid");
    t2.setPermit((int) Defaults.DEFAULT_STEP_CONCURRENCY);
    Tag t3 = new Tag();
    t3.setName(Constants.MAESTRO_PREFIX + summary.getCorrelationId());
    t3.setPermit((int) Defaults.DEFAULT_INSTANCE_STEP_CONCURRENCY);

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder().stepId("stepid").type(StepType.NOOP).build();

    // set default (workflow level and step level) permits if no concurrences are set
    assertEquals(Arrays.asList(t1, t2), summary.deriveRuntimeTagPermits(runtimeSummary));

    // set permit for workflow level and step level step_concurrency
    summary.getRunProperties().setStepConcurrency(10L);
    t1.setPermit(10);
    assertEquals(Arrays.asList(t1, t2), summary.deriveRuntimeTagPermits(runtimeSummary));

    // set permit for instance_step_concurrency only when workflow level and step level
    // step_concurrency are not set
    summary.getRunProperties().setStepConcurrency(null);
    summary.setInstanceStepConcurrency(15L);
    t3.setPermit(15);
    assertEquals(Collections.singletonList(t3), summary.deriveRuntimeTagPermits(runtimeSummary));

    // set tag permits for both step_concurrency and instance_step_concurrency
    summary.getRunProperties().setStepConcurrency(10L);
    summary.setInstanceStepConcurrency(15L);
    t3.setPermit(15);
    assertEquals(Arrays.asList(t1, t3), summary.deriveRuntimeTagPermits(runtimeSummary));

    // don't set tag permits for instance_step_concurrency if non-leaf steps
    runtimeSummary = StepRuntimeSummary.builder().stepId("stepid").type(StepType.FOREACH).build();
    summary.getRunProperties().setStepConcurrency(10L);
    summary.setInstanceStepConcurrency(15L);
    assertEquals(Collections.singletonList(t1), summary.deriveRuntimeTagPermits(runtimeSummary));

    // don't set any tag permit if no step_concurrency set for non-leaf steps
    summary.getRunProperties().setStepConcurrency(null);
    summary.setInstanceStepConcurrency(15L);
    assertEquals(Collections.emptyList(), summary.deriveRuntimeTagPermits(runtimeSummary));
  }
}
