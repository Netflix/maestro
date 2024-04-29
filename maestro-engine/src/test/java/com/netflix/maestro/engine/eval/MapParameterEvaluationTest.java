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
package com.netflix.maestro.engine.eval;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.parameter.BooleanArrayParameter;
import com.netflix.maestro.models.parameter.DoubleArrayParameter;
import com.netflix.maestro.models.parameter.LongArrayParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.junit.BeforeClass;
import org.junit.Test;

public class MapParameterEvaluationTest extends MaestroEngineBaseTest {

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Test
  public void testMapParamEvaluation() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-map-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> params = toParameters(workflow.getParams());
    paramEvaluator.evaluateWorkflowParameters(params, workflow.getId());
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

    // empty array
    assertArrayEquals(new long[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l11"));
    assertArrayEquals(
        new double[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l12"), 0.000001);
    assertArrayEquals(
        new boolean[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l13"));
    assertArrayEquals(
        new String[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l14"));
    assertEquals(
        Collections.emptyMap(), params.get("p1").asMapParam().getEvaluatedResultForParam("l15"));
  }

  @Test
  public void testMapParamEmptyArrayEvaluation() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-map-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();

    Map<String, Parameter> origParams = toParameters(workflow.getParams());
    paramEvaluator.evaluateWorkflowParameters(origParams, workflow.getId());
    Map<String, Parameter> params =
        MAPPER.readValue(
            MAPPER.writeValueAsString(origParams), new TypeReference<Map<String, Parameter>>() {});

    // empty array
    assertArrayEquals(new long[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l11"));
    assertArrayEquals(
        new double[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l12"), 0.000001);
    assertArrayEquals(
        new boolean[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l13"));
    assertArrayEquals(
        new String[0], params.get("p1").asMapParam().getEvaluatedResultForParam("l14"));
    assertEquals(
        Collections.emptyMap(), params.get("p1").asMapParam().getEvaluatedResultForParam("l15"));

    // evaluate expressions referencing empty array
    params.put("eval-param1", LongArrayParameter.builder().expression("p1.get('l11')").build());
    params.put("eval-param2", DoubleArrayParameter.builder().expression("p1.get('l12')").build());
    params.put("eval-param3", BooleanArrayParameter.builder().expression("p1.get('l13')").build());
    params.put("eval-param4", StringArrayParameter.builder().expression("p1.get('l14')").build());
    paramEvaluator.evaluateWorkflowParameters(params, workflow.getId());

    assertArrayEquals(new long[0], params.get("eval-param1").getEvaluatedResult());
    assertArrayEquals(new double[0], params.get("eval-param2").getEvaluatedResult(), 0.000001);
    assertArrayEquals(new boolean[0], params.get("eval-param3").getEvaluatedResult());
    assertArrayEquals(new String[0], params.get("eval-param4").getEvaluatedResult());
  }

  @Test
  public void testInvalidMapParamAsType() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-map-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> params = toParameters(workflow.getParams());
    paramEvaluator.evaluateWorkflowParameters(params, workflow.getId());
    AssertHelper.assertThrows(
        "cannot cast a MAP param to STRING_MAP",
        MaestroInternalError.class,
        "Param [p1] is a [MAP] type and cannot be used as STRING_MAP",
        () -> params.get("p1").asStringMapParam());

    AssertHelper.assertThrows(
        "cannot cast a nested MAP param to DOUBLE_ARRAY",
        MaestroInternalError.class,
        "Param [l10] is a [MAP] type and cannot be used as DOUBLE_ARRAY",
        () -> params.get("p1").asMapParam().getEvaluatedParam("l10").asDoubleArrayParam());
  }
}
