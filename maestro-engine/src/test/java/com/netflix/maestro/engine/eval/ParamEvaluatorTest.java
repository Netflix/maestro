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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroInvalidExpressionException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.parameter.BooleanParameter;
import com.netflix.maestro.models.parameter.LongArrayParameter;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.parameter.StringParameter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class ParamEvaluatorTest extends MaestroEngineBaseTest {

  @Test
  public void testParseWorkflowParameter() {
    StringParameter bar = StringParameter.builder().name("bar").expression("foo + '-1';").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap("foo", LongParameter.builder().expression("1+2+3;").build()),
        bar,
        "test-workflow");
    assertEquals("6-1", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").expression("foo + '-1';").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo", LongParameter.builder().evaluatedResult(6L).evaluatedTime(123L).build()),
        bar,
        "test-workflow");
    assertEquals("6-1", bar.getEvaluatedResult());
  }

  @Test
  public void testParseWorkflowParameterWithImplicitToString() {
    StringParameter bar = StringParameter.builder().name("bar").expression("foo - 1;").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap("foo", LongParameter.builder().expression("1+2+3;").build()),
        bar,
        "test-workflow");
    assertEquals("5", bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a string for type class java.lang.Long",
        bar.getMeta().get("info"));

    bar = StringParameter.builder().name("bar").expression("foo - 1;").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo", LongParameter.builder().evaluatedResult(6L).evaluatedTime(123L).build()),
        bar,
        "test-workflow");
    assertEquals("5", bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a string for type class java.lang.Long",
        bar.getMeta().get("info"));
  }

  @Test
  public void testParseWorkflowParameterWithImplicitToLong() {
    LongParameter bar = LongParameter.builder().name("bar").expression("foo + 1;").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap("foo", StringParameter.builder().expression("1+2+3;").build()),
        bar,
        "test-workflow");
    assertEquals(61, (long) bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a long for type String",
        bar.getMeta().get("warn"));

    bar = LongParameter.builder().name("bar").expression("foo + 1;").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo", StringParameter.builder().evaluatedResult("100").evaluatedTime(123L).build()),
        bar,
        "test-workflow");
    assertEquals(1001, (long) bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a long for type String",
        bar.getMeta().get("warn"));

    AssertHelper.assertThrows(
        "Can only cast string to long",
        IllegalArgumentException.class,
        "Param [bar] is expected to be a Long compatible type but is [class java.lang.Double]",
        () ->
            paramEvaluator.parseWorkflowParameter(
                Collections.emptyMap(),
                LongParameter.builder().name("bar").expression("0.01;").build(),
                "test-workflow"));

    AssertHelper.assertThrows(
        "Can only cast valid string to long",
        NumberFormatException.class,
        "For input string: \"foo\"",
        () ->
            paramEvaluator.parseWorkflowParameter(
                Collections.emptyMap(),
                LongParameter.builder().name("bar").expression("'foo';").build(),
                "test-workflow"));
  }

  @Test
  public void testParseWorkflowParameterWithImplicitToBoolean() {
    BooleanParameter bar = BooleanParameter.builder().name("bar").expression("foo + 'e';").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap("foo", StringParameter.builder().expression("'trU'").build()),
        bar,
        "test-workflow");
    assertTrue(bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a boolean for type String",
        bar.getMeta().get("warn"));

    bar = BooleanParameter.builder().name("bar").expression("foo + 'e';").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo", StringParameter.builder().evaluatedResult("FAls").evaluatedTime(123L).build()),
        bar,
        "test-workflow");
    assertFalse(bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a boolean for type String",
        bar.getMeta().get("warn"));

    AssertHelper.assertThrows(
        "Can only cast string to boolean",
        IllegalArgumentException.class,
        "Param [bar] is expected to be a Boolean compatible type but is [class java.lang.Double]",
        () ->
            paramEvaluator.parseWorkflowParameter(
                Collections.emptyMap(),
                BooleanParameter.builder().name("bar").expression("0.01;").build(),
                "test-workflow"));

    AssertHelper.assertThrows(
        "Can only cast valid string to boolean",
        IllegalArgumentException.class,
        "Param [bar] is expected to have a Boolean compatible result but is [foo]",
        () ->
            paramEvaluator.parseWorkflowParameter(
                Collections.emptyMap(),
                BooleanParameter.builder().name("bar").expression("'foo';").build(),
                "test-workflow"));
  }

  @Test
  public void testParseWorkflowParameterWithImplicitToStringArray() {
    StringArrayParameter bar =
        StringArrayParameter.builder().name("bar").expression("foo;").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo", LongArrayParameter.builder().expression("return new long[]{1, 2,3};").build()),
        bar,
        "test-workflow");
    assertArrayEquals(new String[] {"1", "2", "3"}, bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a string array for type class [J",
        bar.getMeta().get("info"));

    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo",
            LongArrayParameter.builder()
                .evaluatedResult(new long[] {1, 2, 3})
                .evaluatedTime(123L)
                .build()),
        bar,
        "test-workflow");
    assertArrayEquals(new String[] {"1", "2", "3"}, bar.getEvaluatedResult());
    assertEquals(
        "Implicitly converted the evaluated result to a string array for type class [J",
        bar.getMeta().get("info"));
  }

  @Test
  public void testParseLiteralWorkflowParameter() {
    StringParameter bar = StringParameter.builder().name("bar").value("test $foo-1").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap("foo", LongParameter.builder().expression("1+2+3;").build()),
        bar,
        "test-workflow");
    assertEquals("test 6-1", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").value("test $foo-1").build();
    paramEvaluator.parseWorkflowParameter(
        Collections.singletonMap(
            "foo", LongParameter.builder().evaluatedResult(6L).evaluatedTime(123L).build()),
        bar,
        "test-workflow");
    assertEquals("test 6-1", bar.getEvaluatedResult());
  }

  @Test
  public void testParseStepParameter() {
    StringParameter bar =
        StringParameter.builder().name("bar").expression("step1__foo + '-1';").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap(
            "foo", StringParameter.builder().evaluatedResult("123").evaluatedTime(123L).build()),
        bar,
        "step1");
    assertEquals("123-1", bar.getEvaluatedResult());
  }

  @Test
  public void testParseStepParameterWithSameName() {
    LongParameter bar = LongParameter.builder().name("sample").expression("sample;").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.singletonMap(
            "sample", LongParameter.builder().evaluatedResult(5L).evaluatedTime(123L).build()),
        Collections.singletonMap("sample", LongParameter.builder().expression("sample;").build()),
        bar,
        "step1");
    assertEquals(5L, bar.getEvaluatedResult().longValue());

    bar = LongParameter.builder().name("sample").expression("sample;").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.singletonMap(
            "sample", LongParameter.builder().evaluatedResult(5L).evaluatedTime(123L).build()),
        Collections.singletonMap(
            "sample", LongParameter.builder().evaluatedResult(123L).evaluatedTime(123L).build()),
        bar,
        "step1");
    assertEquals(123L, bar.getEvaluatedResult().longValue());
  }

  @Test
  public void testParseStepParameterWithOrderEffect() {
    Map<String, Parameter> workflowParams = new LinkedHashMap<>();
    workflowParams.put(
        "foo", LongParameter.builder().evaluatedResult(1L).evaluatedTime(123L).build());
    workflowParams.put(
        "bar", LongParameter.builder().evaluatedResult(6L).evaluatedTime(123L).build());

    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put("foo", LongParameter.builder().name("foo").expression("bar + 1").build());
    params.put("bar", LongParameter.builder().name("bar").expression("foo + 1").build());

    paramEvaluator.evaluateStepParameters(Collections.emptyMap(), workflowParams, params, "step1");
    assertEquals(7L, params.get("foo").asLongParam().getEvaluatedResult().longValue());
    assertEquals(8L, params.get("bar").asLongParam().getEvaluatedResult().longValue());

    params = new LinkedHashMap<>();
    params.put("bar", LongParameter.builder().name("bar").expression("foo + 1").build());
    params.put("foo", LongParameter.builder().name("foo").expression("bar + 1").build());
    paramEvaluator.evaluateStepParameters(Collections.emptyMap(), workflowParams, params, "step1");
    assertEquals(3L, params.get("foo").asLongParam().getEvaluatedResult().longValue());
    assertEquals(2L, params.get("bar").asLongParam().getEvaluatedResult().longValue());
  }

  @Test
  public void testParseStepParameterWith3Underscore() {
    StringParameter bar =
        StringParameter.builder().name("bar").expression("_step1___foo + '-1';").build();
    paramEvaluator.parseStepParameter(
        Collections.singletonMap("_step1", Collections.emptyMap()),
        Collections.emptyMap(),
        Collections.singletonMap("_foo", StringParameter.builder().value("123").build()),
        bar,
        "_step1");
    assertEquals("123-1", bar.getEvaluatedResult());
  }

  @Test
  public void testParseStepParameterWith4Underscore() {
    StringParameter bar =
        StringParameter.builder().name("bar").expression("_step1____foo + '-1';").build();
    paramEvaluator.parseStepParameter(
        Collections.singletonMap("_step1_", Collections.emptyMap()),
        Collections.emptyMap(),
        Collections.singletonMap("_foo", StringParameter.builder().value("123").build()),
        bar,
        "_step1_");
    assertEquals("123-1", bar.getEvaluatedResult());
  }

  @Test
  public void testParseStepParameterUsingParamsToGetWorkflowParams() {
    StringParameter bar =
        StringParameter.builder().name("bar").expression("params.get('foo') + '-1'").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().evaluatedResult("123").build()),
        Collections.emptyMap(),
        bar,
        "step1");
    assertEquals("123-1", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").expression("params['foo'] + '-1'").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().evaluatedResult("123").build()),
        Collections.emptyMap(),
        bar,
        "step1");
    assertEquals("123-1", bar.getEvaluatedResult());

    BooleanParameter bat =
        BooleanParameter.builder().name("bar").expression("params.get('bar') == null").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().evaluatedResult("123").build()),
        Collections.emptyMap(),
        bat,
        "step1");
    assertTrue(bat.getEvaluatedResult());

    bat = BooleanParameter.builder().name("bar").expression("params['bar'] != null").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().evaluatedResult("123").build()),
        Collections.emptyMap(),
        bat,
        "step1");
    assertFalse(bat.getEvaluatedResult());
  }

  @Test
  public void testParseStepParameterWithInvalidReference() {
    AssertHelper.assertThrows(
        "cannot find a step id or signal instance",
        MaestroInvalidExpressionException.class,
        "Failed to evaluate the param with a definition: [step2__foo]",
        () ->
            paramEvaluator.parseStepParameter(
                Collections.singletonMap("step1", Collections.emptyMap()),
                Collections.emptyMap(),
                Collections.singletonMap("foo", StringParameter.builder().value("123").build()),
                StringParameter.builder().name("bar").expression("step2__foo + '-1';").build(),
                "step1"));

    AssertHelper.assertThrows(
        "step id is ambiguous",
        MaestroInternalError.class,
        "reference [step2___foo] cannot be parsed due to ambiguity (both step ids [step2] and [step2_] exist",
        () ->
            paramEvaluator.parseStepParameter(
                twoItemMap("step2", Collections.emptyMap(), "step2_", Collections.emptyMap()),
                Collections.emptyMap(),
                Collections.emptyMap(),
                StringParameter.builder().name("bar").expression("step2___foo + '-1';").build(),
                "step1"));

    AssertHelper.assertThrows(
        "cannot find a step id",
        MaestroInternalError.class,
        "reference [step3___foo] cannot be parsed as cannot find either step id [step3] or [step3_]",
        () ->
            paramEvaluator.parseStepParameter(
                Collections.singletonMap("step2", Collections.emptyMap()),
                Collections.emptyMap(),
                Collections.emptyMap(),
                StringParameter.builder().name("bar").expression("step3___foo + '-1';").build(),
                "step1"));

    AssertHelper.assertThrows(
        "cannot find a step id",
        MaestroInternalError.class,
        "reference [step3____foo] cannot be parsed as cannot find either step id [step3] or [step3_]",
        () ->
            paramEvaluator.parseStepParameter(
                Collections.singletonMap("step2", Collections.emptyMap()),
                Collections.emptyMap(),
                Collections.emptyMap(),
                StringParameter.builder().name("bar").expression("step3____foo + '-1';").build(),
                "step1"));
  }

  @Test
  public void testParseLiteralStepParameter() {
    StringParameter bar = StringParameter.builder().name("bar").value("test ${foo}").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().value("123").build()),
        bar,
        "step1");
    assertEquals("test 123", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").value("test ${foo}").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap(
            "foo", StringParameter.builder().evaluatedResult("123").evaluatedTime(123L).build()),
        bar,
        "step1");
    assertEquals("test 123", bar.getEvaluatedResult());
  }

  @Test
  public void testParseInjectedStepIdStepParameter() {
    StringParameter bar = StringParameter.builder().name("id").value("test ${step_id}").build();

    // create all the mock instances.
    InstanceWrapper mockInstanceWrapper = mock(InstanceWrapper.class);
    StepInstanceAttributes mockStepAttributes = mock(StepInstanceAttributes.class);
    when(mockStepAttributes.getStepId()).thenReturn("step1");
    when(mockInstanceWrapper.isWorkflowParam()).thenReturn(false);
    when(mockInstanceWrapper.getStepInstanceAttributes()).thenReturn(mockStepAttributes);

    paramExtensionRepo.reset(Collections.emptyMap(), null, mockInstanceWrapper);
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap(
            "step_id",
            StringParameter.builder()
                .mode(ParamMode.CONSTANT)
                .name("step_id")
                .expression("return params.getFromStep(\"step_id\");")
                .build()),
        bar,
        "step1");
    assertEquals("test step1", bar.getEvaluatedResult());
  }

  @Test
  public void testParseLiteralStepParameterWithStepId() {
    StringParameter bar = StringParameter.builder().name("bar").value("test ${step1__foo}").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().value("123").build()),
        bar,
        "step1");
    assertEquals("test 123", bar.getEvaluatedResult());

    bar = StringParameter.builder().name("bar").value("test ${step1__foo}").build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap(
            "foo", StringParameter.builder().evaluatedResult("123").evaluatedTime(123L).build()),
        bar,
        "step1");
    assertEquals("test 123", bar.getEvaluatedResult());
  }

  @Test
  public void testCyclicWorkflowParamReference() {
    AssertHelper.assertThrows(
        "cannot have cyclic param referencing",
        IllegalArgumentException.class,
        "In workflow [test-workflow], param [bar] definition contains a cyclic reference chain",
        () ->
            paramEvaluator.parseWorkflowParameter(
                Collections.singletonMap(
                    "bar", StringParameter.builder().name("bar").expression("bar + '-1';").build()),
                StringParameter.builder().name("bar").expression("bar + '-1';").build(),
                "test-workflow"));

    AssertHelper.assertThrows(
        "cannot have cyclic param referencing",
        IllegalArgumentException.class,
        "In step [step1], param [bar] definition contains a cyclic reference chain",
        () ->
            paramEvaluator.parseStepParameter(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(
                    "bar", StringParameter.builder().name("bar").expression("bar + '-1';").build()),
                StringParameter.builder().name("bar").expression("bar + '-1';").build(),
                "step1"));

    AssertHelper.assertThrows(
        "cannot have cyclic param referencing",
        IllegalArgumentException.class,
        "In step [step1], param [bar] definition contains a cyclic reference chain",
        () ->
            paramEvaluator.parseStepParameter(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(
                    "bar", StringParameter.builder().name("bar").expression("bar + '-1';").build()),
                StringParameter.builder().name("bar").expression("step1__bar + '-1';").build(),
                "step1"));

    AssertHelper.assertThrows(
        "cannot have cyclic param referencing",
        IllegalArgumentException.class,
        "In step [step1], param [bar] definition contains a cyclic reference chain",
        () -> {
          Map<String, Parameter> params = new LinkedHashMap<>();
          params.put("bar", LongParameter.builder().name("bar").expression("foo + 1").build());
          params.put("foo", LongParameter.builder().name("foo").expression("bar + 1").build());
          paramEvaluator.parseStepParameter(
              Collections.emptyMap(),
              Collections.emptyMap(),
              params,
              StringParameter.builder().name("bar").expression("foo + 1").build(),
              "step1");
        });
  }

  @Test
  public void testParseAttribute() {
    Parameter param =
        paramEvaluator.parseAttribute(
            ParamDefinition.buildParamDefinition("bar", "foo"),
            Collections.singletonMap("foo", LongParameter.builder().expression("1+2+3;").build()),
            "test-workflow",
            true);
    assertEquals("foo", param.asString());

    param =
        paramEvaluator.parseAttribute(
            ParamDefinition.buildParamDefinition("bar", "${foo}"),
            Collections.singletonMap("foo", LongParameter.builder().expression("1+2+3;").build()),
            "test-workflow",
            true);
    assertEquals("6", param.asString());

    assertNull(
        paramEvaluator.parseAttribute(
            ParamDefinition.buildParamDefinition("bar", "${foo}"),
            Collections.emptyMap(),
            "test-workflow",
            true));
  }

  @Test
  public void testParseInvalidAttributeValue() {
    AssertHelper.assertThrows(
        "unable to parse attribute",
        MaestroUnprocessableEntityException.class,
        "Failed to parse attribute [bar] for workflow [test-workflow] due to ",
        () ->
            paramEvaluator.parseAttribute(
                ParamDefinition.buildParamDefinition("bar", "${foo}"),
                Collections.singletonMap(
                    "bat", LongParameter.builder().expression("1+2+3;").build()),
                "test-workflow",
                false));
  }

  @Test
  public void testEvaluateSignalParameters() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").value("signal ${step1__foo}").build());
    paramDefMap.put(
        "bar", StringParamDefinition.builder().name("bar").value("test ${step1__foo}").build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    MapParameter mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test 123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("signal 123", mapParameter.getEvaluatedParam("name").getEvaluatedResultString());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").value("signal ${foo}").build());
    paramDefMap.put(
        "bar", StringParamDefinition.builder().name("bar").value("test ${foo}").build());

    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test 123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").value("signal123").build());
    paramDefMap.put("bar", StringParamDefinition.builder().name("bar").value("test12").build());
    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test12", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("signal123", mapParameter.getEvaluatedParam("name").getEvaluatedResult());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").expression("'signal' + foo").build());
    paramDefMap.put(
        "bar", StringParamDefinition.builder().name("bar").expression("'test' + foo").build());
    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("signal123", mapParameter.getEvaluatedParam("name").getEvaluatedResult());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").expression("foo").build());
    paramDefMap.put("bar", StringParamDefinition.builder().name("bar").expression("foo").build());
    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("123", mapParameter.getEvaluatedParam("name").getEvaluatedResult());
  }

  @Test
  public void testEvaluateStepOutputsParameter() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").value("signal ${step1__foo}").build());
    paramDefMap.put(
        "bar", StringParamDefinition.builder().name("bar").value("test ${step1__foo}").build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    MapParameter mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test 123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("signal 123", mapParameter.getEvaluatedParam("name").getEvaluatedResultString());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").value("signal ${foo}").build());
    paramDefMap.put(
        "bar", StringParamDefinition.builder().name("bar").value("test ${foo}").build());
    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test 123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").value("signal123").build());
    paramDefMap.put("bar", StringParamDefinition.builder().name("bar").value("test12").build());
    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test12", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("signal123", mapParameter.getEvaluatedParam("name").getEvaluatedResult());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "name", StringParamDefinition.builder().name("name").expression("'signal' + foo").build());
    paramDefMap.put(
        "bar", StringParamDefinition.builder().name("bar").expression("'test' + foo").build());

    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("test123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("signal123", mapParameter.getEvaluatedParam("name").getEvaluatedResult());

    paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").expression("foo").build());
    paramDefMap.put("bar", StringParamDefinition.builder().name("bar").expression("foo").build());
    mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();
    mapParameter = (MapParameter) mapParamDefinition.toParameter();

    paramEvaluator.evaluateSignalDependenciesOrOutputsParameters(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap("foo", StringParameter.builder().name("foo").value("123").build()),
        Collections.singletonList(mapParameter),
        "step1");
    assertEquals("123", mapParameter.getEvaluatedParam("bar").getEvaluatedResult());
    assertEquals("123", mapParameter.getEvaluatedParam("name").getEvaluatedResult());
  }

  @Test
  public void testParamsSizeOverLimit() throws Exception {
    ObjectMapper mockMapper = mock(ObjectMapper.class);
    ParamEvaluator testEvaluator = new ParamEvaluator(null, mockMapper);

    when(mockMapper.writeValueAsString(any()))
        .thenReturn(new String(new char[Constants.JSONIFIED_PARAMS_STRING_SIZE_LIMIT - 1]));
    testEvaluator.evaluateWorkflowParameters(Collections.emptyMap(), "foo");
    testEvaluator.evaluateStepParameters(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), "bar");

    when(mockMapper.writeValueAsString(any()))
        .thenReturn(new String(new char[Constants.JSONIFIED_PARAMS_STRING_SIZE_LIMIT + 1]));
    AssertHelper.assertThrows(
        "Parameter size is over limit",
        IllegalArgumentException.class,
        "Parameters' total size [750001] is larger than system limit [750000]",
        () -> testEvaluator.evaluateWorkflowParameters(Collections.emptyMap(), "foo"));

    AssertHelper.assertThrows(
        "Parameter size is over limit",
        IllegalArgumentException.class,
        "Parameters' total size [750001] is larger than system limit [750000]",
        () ->
            testEvaluator.evaluateStepParameters(
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), "bar"));
  }
}
