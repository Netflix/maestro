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

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringMapParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParameterEvaluationTest extends MaestroEngineBaseTest {

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Test
  public void testParamEvaluation() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> params = toParameters(workflow.getParams());
    paramEvaluator.evaluateWorkflowParameters(params, workflow.getId());
    assertEquals("bar", params.get("l1").getEvaluatedResult());
    assertEquals(Long.valueOf(1598399975650L), params.get("l2").getEvaluatedResult());
    assertEquals(Double.valueOf(1.23), params.get("l3").getEvaluatedResult());
    assertEquals(true, params.get("l4").getEvaluatedResult());
    assertEquals(Collections.singletonMap("foo", "bar"), params.get("l5").getEvaluatedResult());
    assertTrue(
        Objects.deepEquals(new String[] {"a", "b", "c"}, params.get("l6").getEvaluatedResult()));
    assertTrue(Objects.deepEquals(new long[] {1, 2, 3}, params.get("l7").getEvaluatedResult()));
    assertTrue(
        Objects.deepEquals(new double[] {1.1, 2.2, 3.3}, params.get("l8").getEvaluatedResult()));
    assertTrue(
        Objects.deepEquals(
            new boolean[] {true, false, true}, params.get("l9").getEvaluatedResult()));
    assertEquals("bar", params.get("e1").getEvaluatedResult());
    assertEquals(Long.valueOf(2L), params.get("e2").getEvaluatedResult());
    assertEquals(Double.valueOf(1.23), params.get("e3").getEvaluatedResult());
    assertEquals(true, params.get("e4").getEvaluatedResult());
    assertEquals(Collections.singletonMap("foo", "bar"), params.get("e5").getEvaluatedResult());
    assertTrue(
        Objects.deepEquals(new String[] {"a", "b", "c"}, params.get("e6").getEvaluatedResult()));
    assertTrue(Objects.deepEquals(new long[] {1, 2, 3}, params.get("e7").getEvaluatedResult()));
    assertTrue(
        Objects.deepEquals(new double[] {1.1, 2.2, 3.3}, params.get("e8").getEvaluatedResult()));
    assertTrue(
        Objects.deepEquals(
            new boolean[] {true, false, true}, params.get("e9").getEvaluatedResult()));
    assertEquals(
        Collections.singletonMap("application", "{\"application_name\":\"bdp_scheduler_api\"}"),
        params.get("AUTH_ACTING_ON_BEHALF_OF_BLOB").getEvaluatedResult());

    // use the asType methods.
    assertEquals("bar", params.get("l1").asString());
    assertEquals(1598399975650L, params.get("l2").asLong().longValue());
    assertEquals(1.23, params.get("l3").asDouble(), 0.00001);
    assertEquals(true, params.get("l4").asBoolean());
    assertEquals(Collections.singletonMap("foo", "bar"), params.get("l5").asStringMap());
    assertTrue(Objects.deepEquals(new String[] {"a", "b", "c"}, params.get("l6").asStringArray()));
    assertTrue(Objects.deepEquals(new long[] {1, 2, 3}, params.get("l7").asLongArray()));
    assertTrue(Objects.deepEquals(new double[] {1.1, 2.2, 3.3}, params.get("l8").asDoubleArray()));
    assertTrue(
        Objects.deepEquals(new boolean[] {true, false, true}, params.get("l9").asBooleanArray()));
    assertEquals("bar", params.get("e1").asString());
    assertEquals(2L, params.get("e2").asLong().longValue());
    assertEquals(1.23, params.get("e3").asDouble(), 0.00001);
    assertEquals(true, params.get("e4").asBoolean());
    assertEquals(Collections.singletonMap("foo", "bar"), params.get("e5").asStringMap());
    assertTrue(Objects.deepEquals(new String[] {"a", "b", "c"}, params.get("e6").asStringArray()));
    assertTrue(Objects.deepEquals(new long[] {1, 2, 3}, params.get("e7").asLongArray()));
    assertTrue(Objects.deepEquals(new double[] {1.1, 2.2, 3.3}, params.get("e8").asDoubleArray()));
    assertTrue(
        Objects.deepEquals(new boolean[] {true, false, true}, params.get("e9").asBooleanArray()));
    assertEquals(
        Collections.singletonMap("application", "{\"application_name\":\"bdp_scheduler_api\"}"),
        params.get("AUTH_ACTING_ON_BEHALF_OF_BLOB").asStringMap());
    assertEquals(
        Collections.singletonMap("application", "{\"application_name\":\"bdp_scheduler_api\"}"),
        params.get("AUTH_TRIGGERING_ON_BEHALF_OF_BLOB").asStringMap());
  }

  @Test
  public void testInvalidParamAsType() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> params = toParameters(workflow.getParams());
    paramEvaluator.evaluateWorkflowParameters(params, workflow.getId());
    AssertHelper.assertThrows(
        "cannot cast a STRING param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l1] is a [STRING] type and cannot be used as BOOLEAN",
        () -> params.get("l1").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a LONG param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l2] is a [LONG] type and cannot be used as BOOLEAN",
        () -> params.get("l2").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a DOUBLE param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l3] is a [DOUBLE] type and cannot be used as BOOLEAN",
        () -> params.get("l3").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a BOOLEAN param to STRING",
        MaestroInternalError.class,
        "Param [l4] is a [BOOLEAN] type and cannot be used as STRING",
        () -> params.get("l4").asString());
    AssertHelper.assertThrows(
        "cannot cast a STRING_MAP param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l5] is a [STRING_MAP] type and cannot be used as BOOLEAN",
        () -> params.get("l5").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a STRING_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l6] is a [STRING_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("l6").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a LONG_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l7] is a [LONG_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("l7").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a DOUBLE_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l8] is a [DOUBLE_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("l8").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a BOOLEAN_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [l9] is a [BOOLEAN_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("l9").asBoolean());

    AssertHelper.assertThrows(
        "cannot cast a STRING param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e1] is a [STRING] type and cannot be used as BOOLEAN",
        () -> params.get("e1").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a LONG param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e2] is a [LONG] type and cannot be used as BOOLEAN",
        () -> params.get("e2").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a DOUBLE param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e3] is a [DOUBLE] type and cannot be used as BOOLEAN",
        () -> params.get("e3").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a BOOLEAN param to STRING",
        MaestroInternalError.class,
        "Param [e4] is a [BOOLEAN] type and cannot be used as STRING",
        () -> params.get("e4").asString());
    AssertHelper.assertThrows(
        "cannot cast a STRING_MAP param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e5] is a [STRING_MAP] type and cannot be used as BOOLEAN",
        () -> params.get("e5").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a STRING_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e6] is a [STRING_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("e6").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a LONG_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e7] is a [LONG_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("e7").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a DOUBLE_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e8] is a [DOUBLE_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("e8").asBoolean());
    AssertHelper.assertThrows(
        "cannot cast a BOOLEAN_ARRAY param to BOOLEAN",
        MaestroInternalError.class,
        "Param [e9] is a [BOOLEAN_ARRAY] type and cannot be used as BOOLEAN",
        () -> params.get("e9").asBoolean());
  }

  @Test
  public void testStepParamDoubleEvaluation() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> stepParams = toParameters(workflow.getSteps().get(0).getParams());
    paramEvaluator.evaluateStepParameters(
        Collections.emptyMap(), Collections.emptyMap(), stepParams, "job1");
    assertEquals(
        15L, stepParams.get("sleep_seconds").asLongParam().getEvaluatedResult().longValue());
    assertEquals("15", stepParams.get("sleep_seconds").getEvaluatedResultString());
    assertEquals((Double) 2.0, stepParams.get("cpu").getEvaluatedResult());
    assertEquals("2.0", stepParams.get("cpu").getEvaluatedResultString());
    assertEquals((Double) 10.229999999999, stepParams.get("memory").getEvaluatedResult());
    assertEquals("10.229999999999", stepParams.get("memory").getEvaluatedResultString());
    assertEquals((Double) 1.00000000000001, stepParams.get("monitor").getEvaluatedResult());
    assertEquals("1.00000000000001", stepParams.get("monitor").getEvaluatedResultString());
  }

  @Test
  public void testStepParamDoubleArrayEvaluation() throws Exception {
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-params.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> stepParams = toParameters(workflow.getSteps().get(1).getParams());
    paramEvaluator.evaluateStepParameters(
        Collections.emptyMap(), Collections.emptyMap(), stepParams, "job2");
    assertArrayEquals(
        new double[] {15, 16}, stepParams.get("param1").asDoubleArray(), 0.00000000000000);
    assertArrayEquals(
        new double[] {12.3, 45.0}, stepParams.get("param2").asDoubleArray(), 0.00000000000000);
    assertArrayEquals(
        new double[] {10.229999999999, 1.00000000000001},
        stepParams.get("param3").asDoubleArray(),
        0.00000000000000);
    assertArrayEquals(
        new double[] {1.2, 3.45}, stepParams.get("param4").asDoubleArray(), 0.00000000000000);
    assertArrayEquals(
        new double[] {1.229999999999, 3.00000000000001},
        stepParams.get("param5").asDoubleArray(),
        0.00000000000000);
  }

  @Test
  public void testParseWorkflowParameterWithSignalParamUsingDoubleUnderscore() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "signal_a",
            StringMapParameter.builder().evaluatedResult(singletonMap("param1", "value1")).build(),
            "signal_b",
            MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));
    paramExtensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder().initiator(initiator).build());

    StringParameter bar =
        StringParameter.builder().name("bar").expression("signal_a__param1 + '-1';").build();
    paramEvaluator.parseWorkflowParameter(Collections.emptyMap(), bar, "test-workflow");
    assertEquals("value1-1", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").expression("signal_b__param2 - 1;").build();
    paramEvaluator.parseWorkflowParameter(Collections.emptyMap(), bar, "test-workflow");
    assertEquals("122", bar.getEvaluatedResult());
    paramExtensionRepo.clear();
  }

  @Test
  public void testParseStepParameterWithSignalParamUsingDoubleUnderscore() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "signal_a",
            StringMapParameter.builder().evaluatedResult(singletonMap("param1", "value1")).build(),
            "signal_b",
            MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));
    paramExtensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder().initiator(initiator).build());

    StringParameter bar =
        StringParameter.builder().name("bar").expression("signal_b__param2 + '-1';").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), bar, "step1");
    assertEquals("123-1", bar.getEvaluatedResult());
    paramExtensionRepo.clear();
  }

  @Test
  public void testParseLiteralWorkflowParameterWithSignalParamUsingDoubleUnderscore() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "signal-a",
            StringMapParameter.builder().evaluatedResult(singletonMap("param1", "value1")).build(),
            "signal-b",
            MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));
    paramExtensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder().initiator(initiator).build());

    StringParameter bar =
        StringParameter.builder().name("bar").value("test ${signal-a__param1}-1").build();
    paramEvaluator.parseWorkflowParameter(Collections.emptyMap(), bar, "test-workflow");
    assertEquals("test value1-1", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").value("test ${signal-b__param2}-1").build();
    paramEvaluator.parseWorkflowParameter(Collections.emptyMap(), bar, "test-workflow");
    assertEquals("test 123-1", bar.getEvaluatedResult());
    paramExtensionRepo.clear();
  }

  @Test
  public void testParseLiteralStepParameterWithSignalParamUsingDoubleUnderscore() {
    SignalInitiator initiator = new SignalInitiator();
    initiator.setParams(
        twoItemMap(
            "signal-a",
            StringMapParameter.builder().evaluatedResult(singletonMap("param1", "value1")).build(),
            "signal-b",
            MapParameter.builder().evaluatedResult(singletonMap("param2", 123L)).build()));
    paramExtensionRepo.reset(
        Collections.emptyMap(),
        Collections.emptyMap(),
        InstanceWrapper.builder().initiator(initiator).build());

    StringParameter bar =
        StringParameter.builder().name("bar").value("test ${signal-a__param1}").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), bar, "step1");
    assertEquals("test value1", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").value("test ${signal-b__param2}").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), bar, "step1");
    assertEquals("test 123", bar.getEvaluatedResult());
    paramExtensionRepo.clear();
  }
}
