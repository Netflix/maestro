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
package com.netflix.maestro.models.parameter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.utils.ParamHelper;
import java.math.BigDecimal;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParameterTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowDefinition expected =
        loadObject("fixtures/parameters/sample-wf-params.json", WorkflowDefinition.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    WorkflowDefinition actual = MAPPER.readValue(ser1, WorkflowDefinition.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testLongParameter() {
    LongParameter param =
        LongParameter.builder()
            .name("longParam")
            .expression("return 123;")
            .evaluatedResult(123L)
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asLongParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testStringParameter() {
    StringParameter param =
        StringParameter.builder()
            .name("stringParam")
            .expression("return 'abc';")
            .evaluatedResult("abc")
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asStringParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testDoubleParameter() {
    DoubleParameter param =
        DoubleParameter.builder()
            .name("doubleParam")
            .expression("return 12.3;")
            .evaluatedResult(12.3)
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asDoubleParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(
        new BigDecimal(param.getEvaluatedResult().toString()), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testBooleanParameter() {
    BooleanParameter param =
        BooleanParameter.builder()
            .name("booleanParam")
            .expression("return true;")
            .evaluatedResult(true)
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asBooleanParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testLongArrayParameter() {
    LongArrayParameter param =
        LongArrayParameter.builder()
            .name("longArrayParam")
            .expression("return [1, 2, 3];")
            .evaluatedResult(new long[] {123L})
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asLongArrayParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testStringArrayParameter() {
    StringArrayParameter param =
        StringArrayParameter.builder()
            .name("stringArrayParam")
            .expression("return ['abc'];")
            .evaluatedResult(new String[] {"abc"})
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asStringArrayParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertArrayEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testDoubleArrayParameter() {
    DoubleArrayParameter param =
        DoubleArrayParameter.builder()
            .name("doubleArrayParam")
            .expression("return [12.3];")
            .evaluatedResult(new double[] {12.3})
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asDoubleArrayParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertArrayEquals(
        ParamHelper.toDecimalArray("doubleArrayParam", param.getEvaluatedResult()),
        param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testBooleanArrayParameter() {
    BooleanArrayParameter param =
        BooleanArrayParameter.builder()
            .name("booleanArrayParam")
            .expression("return [true];")
            .evaluatedResult(new boolean[] {true})
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asBooleanArrayParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testStringMapParameter() {
    StringMapParameter param =
        StringMapParameter.builder()
            .name("stringMapParam")
            .expression("m = new Map(); m.put('abc','def'); return m;")
            .evaluatedResult(Collections.singletonMap("abc", "def"))
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asStringMapParamDef().getMeta());

    param = param.toBuilder().mode(ParamMode.CONSTANT).build();
    assertEquals(param.getEvaluatedResult(), param.toDefinition().getValue());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
    assertEquals(ParamMode.CONSTANT, param.toDefinition().getMode());
  }

  @Test
  public void testMapParameter() {
    MapParameter param =
        MapParameter.builder()
            .name("mapParam")
            .expression("m = new Map(); m.put('foo', 123); return m;")
            .evaluatedResult(Collections.singletonMap("foo", 123L))
            .evaluatedTime(12345L)
            .mode(ParamMode.IMMUTABLE)
            .build();

    assertNull(param.toDefinition().getValue());
    assertEquals(param.getExpression(), param.toDefinition().getExpression());
    assertEquals(ParamMode.IMMUTABLE, param.toDefinition().getMode());
    assertNull(param.toDefinition().asMapParamDef().getMeta());

    param = param.toBuilder().mode(null).build();
    assertEquals(
        Collections.singletonMap("foo", buildParam("foo", 123L).toDefinition()),
        param.toDefinition().getValue());
    assertNull(param.toDefinition().getMode());
    assertNull(param.toBuilder().mode(ParamMode.CONSTANT).build().toDefinition().getExpression());
  }

  @Test
  public void testParameterDefinitionToString() {
    StringParamDefinition stringParamDef =
        StringParamDefinition.builder()
            .name("stringParam")
            .value("bar")
            .mode(ParamMode.MUTABLE)
            .build();
    assertEquals(
        "StringParamDefinition(super=AbstractParamDefinition(name=stringParam, expression=null, validator=null, tags=null, mode=MUTABLE, meta=null), value=bar)",
        stringParamDef.toString());
    StringMapParamDefinition stringMapParamDef =
        StringMapParamDefinition.builder()
            .name("stringMapParam")
            .value(Collections.singletonMap("foo", "bar"))
            .mode(ParamMode.CONSTANT)
            .build();
    assertEquals(
        "StringMapParamDefinition(super=AbstractParamDefinition(name=stringMapParam, expression=null, validator=null, tags=null, mode=CONSTANT, meta=null), value={foo=bar})",
        stringMapParamDef.toString());
    StringArrayParamDefinition stringArrayParamDef =
        StringArrayParamDefinition.builder()
            .name("stringArrayParam")
            .expression("true")
            .value(new String[] {"val1, val2"})
            .build();
    assertEquals(
        "StringArrayParamDefinition(super=AbstractParamDefinition(name=stringArrayParam, expression=true, validator=null, tags=null, mode=null, meta=null), value=[val1, val2])",
        stringArrayParamDef.toString());
    MapParamDefinition mapParamDef =
        MapParamDefinition.builder()
            .name("mapParam")
            .value(Collections.singletonMap("foo", stringParamDef))
            .build();
    assertEquals(
        "MapParamDefinition(super=AbstractParamDefinition(name=mapParam, expression=null, validator=null, tags=null, mode=null, meta=null), value={foo=StringParamDefinition(super=AbstractParamDefinition(name=foo, expression=null, validator=null, tags=null, mode=MUTABLE, meta=null), value=bar)})",
        mapParamDef.toString());
    LongParamDefinition longParamDef =
        LongParamDefinition.builder()
            .name("longParam")
            .value(1024L)
            .mode(ParamMode.MUTABLE)
            .build();
    assertEquals(
        "LongParamDefinition(super=AbstractParamDefinition(name=longParam, expression=null, validator=null, tags=null, mode=MUTABLE, meta=null), value=1024)",
        longParamDef.toString());
    LongArrayParamDefinition longArrayParamDef =
        LongArrayParamDefinition.builder()
            .name("longArrayParam")
            .value(new long[] {256, 512})
            .meta(Collections.singletonMap("key", "value"))
            .build();
    assertEquals(
        "LongArrayParamDefinition(super=AbstractParamDefinition(name=longArrayParam, expression=null, validator=null, tags=null, mode=null, meta={key=value}), value=[256, 512])",
        longArrayParamDef.toString());
    DoubleParamDefinition doubleParamDef =
        DoubleParamDefinition.builder()
            .name("doubleParam")
            .value(BigDecimal.valueOf(10))
            .mode(ParamMode.IMMUTABLE)
            .build();
    assertEquals(
        "DoubleParamDefinition(super=AbstractParamDefinition(name=doubleParam, expression=null, validator=null, tags=null, mode=IMMUTABLE, meta=null), value=10)",
        doubleParamDef.toString());
    DoubleArrayParamDefinition doubleArrayParamDef =
        DoubleArrayParamDefinition.builder()
            .name("doubleArrayParam")
            .value(new BigDecimal[] {BigDecimal.valueOf(20)})
            .build();
    assertEquals(
        "DoubleArrayParamDefinition(super=AbstractParamDefinition(name=doubleArrayParam, expression=null, validator=null, tags=null, mode=null, meta=null), value=[20])",
        doubleArrayParamDef.toString());
    BooleanParamDefinition booleanParamDef =
        BooleanParamDefinition.builder()
            .name("booleanParam")
            .value(true)
            .expression("true")
            .build();
    assertEquals(
        "BooleanParamDefinition(super=AbstractParamDefinition(name=booleanParam, expression=true, validator=null, tags=null, mode=null, meta=null), value=true)",
        booleanParamDef.toString());
    BooleanArrayParamDefinition booleanArrayParamDef =
        BooleanArrayParamDefinition.builder()
            .name("booleanArrayParam")
            .value(new boolean[] {false, true, false})
            .mode(ParamMode.MUTABLE)
            .build();
    assertEquals(
        "BooleanArrayParamDefinition(super=AbstractParamDefinition(name=booleanArrayParam, expression=null, validator=null, tags=null, mode=MUTABLE, meta=null), value=[false, true, false])",
        booleanArrayParamDef.toString());
  }
}
