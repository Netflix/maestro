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

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.parameter.BooleanArrayParameter;
import com.netflix.maestro.models.parameter.BooleanParameter;
import com.netflix.maestro.models.parameter.DoubleArrayParameter;
import com.netflix.maestro.models.parameter.DoubleParameter;
import com.netflix.maestro.models.parameter.LongArrayParameter;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import com.netflix.maestro.models.parameter.StringMapParameter;
import com.netflix.maestro.models.parameter.StringParameter;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LiteralEvaluatorTest extends MaestroEngineBaseTest {

  private String crazyName;
  private Map<String, Parameter> params;

  @Before
  public void setUp() {
    crazyName = "maybe.its.a/data/1/artifact.or-something.3.14159";
    params = new LinkedHashMap<>();
    params.put("var", LongParameter.builder().evaluatedResult(0L).evaluatedTime(123L).build());
    params.put(
        "var1", BooleanParameter.builder().evaluatedResult(true).evaluatedTime(123L).build());
    params.put("var2", StringParameter.builder().evaluatedResult("2").evaluatedTime(123L).build());
    params.put(
        crazyName, DoubleParameter.builder().evaluatedResult(3.0).evaluatedTime(123L).build());
    params.put(
        "foo", StringParameter.builder().evaluatedResult("$bar").evaluatedTime(123L).build());
    params.put(
        "bar", StringParameter.builder().evaluatedResult("$baz").evaluatedTime(123L).build());
    params.put(
        "baz", StringParameter.builder().evaluatedResult("$foo").evaluatedTime(123L).build());
    params.put(
        "strArray",
        StringArrayParameter.builder()
            .evaluatedResult(new String[] {"one", "two", "three"})
            .evaluatedTime(123L)
            .build());
    params.put(
        "intArray",
        LongArrayParameter.builder()
            .evaluatedResult(new long[] {1L, 2L, 3L})
            .evaluatedTime(123L)
            .build());
    params.put(
        "boolArray",
        BooleanArrayParameter.builder()
            .evaluatedResult(new boolean[] {true, false, true})
            .evaluatedTime(123L)
            .build());
    params.put(
        "doubleArray",
        DoubleArrayParameter.builder()
            .evaluatedResult(new double[] {1.1, 2.2, 3.3})
            .evaluatedTime(123L)
            .build());
    params.put(
        "strMap",
        StringMapParameter.builder()
            .evaluatedResult(Collections.singletonMap("key", "$value"))
            .evaluatedTime(123L)
            .build());
    params.put(
        "mapParam",
        MapParameter.builder()
            .evaluatedResult(Collections.singletonMap("key", "$value"))
            .evaluatedTime(123L)
            .build());
    params.put("notEvaluated", StringParameter.builder().build());
  }

  @Test
  public void testStringInterpolationInStringParam() {
    Parameter param =
        StringParameter.builder()
            .name("test")
            .value("test $var $var $var1 $var2 ${" + crazyName + "}")
            .build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(
        new LinkedHashSet<>(
            Arrays.asList(
                "var", "var1", "var2", "maybe.its.a/data/1/artifact.or-something.3.14159")),
        paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("test 0 0 true 2 3.0", result);
  }

  @Test
  public void testStringInterpolationInStringArrayParam() {
    Parameter param =
        StringArrayParameter.builder()
            .name("test")
            .value(new String[] {"test $var", "test $var1", "test $var $var2"})
            .build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("var", "var1", "var2")), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertArrayEquals(new String[] {"test 0", "test true", "test 0 2"}, (String[]) result);
  }

  @Test
  public void testStringInterpolationInStringMapParam() {
    Map<String, String> value = new HashMap<>();
    value.put("foo", "$var");
    value.put("bar", "bat");
    value.put("baz", "${var2}");
    Parameter param = StringMapParameter.builder().name("test").value(value).build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("var", "var2")), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    value.put("foo", "0");
    value.put("bar", "bat");
    value.put("baz", "2");
    Assert.assertEquals(value, result);
  }

  @Test
  public void testStringInterpolationWithKeyValueParamInStringMap() {
    Parameter param =
        StringMapParameter.builder().value(Collections.singletonMap("$var", "$var2")).build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("var", "var2")), paramNames);

    // check string interpolation in both the key and the value
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals(Collections.singletonMap("0", "2"), result);
  }

  @Test
  public void testStringInterpolationWithEscapedNames() {
    Parameter param =
        StringParameter.builder()
            .name("test")
            .value("$$var$var $var1$$var1 $$$var2 $$$$var2")
            .build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("var", "var1", "var2")), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("$var0 true$var1 $2 $$var2", result);
  }

  @Test
  public void testStringInterpolationWithBracketAndEscapedNames() {
    Parameter param =
        StringParameter.builder()
            .name("test")
            .value("${var}$${var} $${" + crazyName + "}${" + crazyName + "} $$${var1} $$$${var1}")
            .build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("var", "var1", crazyName)), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("0${var} ${" + crazyName + "}3.0 $true $${var1}", result);
  }

  @Test
  public void testStringInterpolationWithoutDoubleInterpolation() {
    Parameter param = StringParameter.builder().name("test").value("$foo$bar$baz").build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Arrays.asList("foo", "bar", "baz")), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("$bar$baz$foo", result);
  }

  @Test
  public void testReplaceDoubleDollarSign() {
    Parameter param =
        StringParameter.builder().name("test").value("no variables$$ $$$$ in this string").build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertTrue(paramNames.isEmpty());

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("no variables$ $$ in this string", result);
  }

  @Test
  public void testStringInterpolationWithArrayParam() {
    Parameter param =
        StringParameter.builder()
            .name("test")
            .value("test $strArray $intArray $boolArray $doubleArray")
            .build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(
        new LinkedHashSet<>(Arrays.asList("strArray", "intArray", "boolArray", "doubleArray")),
        paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals(
        "test [\"one\",\"two\",\"three\"] [1,2,3] [true,false,true] [1.1,2.2,3.3]", result);
  }

  @Test
  public void testStringInterpolationJsonError() {
    Parameter param = StringParameter.builder().name("test").value("test $invalidMap").build();
    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put(
        "invalidMap",
        MapParameter.builder()
            .evaluatedResult(Collections.singletonMap("key", new Object()))
            .evaluatedTime(123L)
            .build());

    AssertHelper.assertThrows(
        "Throw an error if param cannot be json serialized",
        MaestroInternalError.class,
        "INTERNAL_ERROR - Cannot evaluate [invalidMap] as string due to",
        () -> LiteralEvaluator.eval(param, params));
  }

  @Test
  public void testStringInterpolationWithStringMapParam() {
    Parameter param = StringParameter.builder().name("test").value("test $strMap").build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Collections.singletonList("strMap")), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("test {\"key\":\"$value\"}", result);
  }

  @Test
  public void testLiteralEvalMapParam() {
    Parameter param = StringParameter.builder().name("test").value("test $mapParam").build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Collections.singletonList("mapParam")), paramNames);

    // check string interpolation
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals("test {\"key\":\"$value\"}", result);
  }

  @Test
  public void testNonExistingParam() {
    Parameter param = StringParameter.builder().name("test").value("test $notExisting").build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Collections.singletonList("notExisting")), paramNames);

    AssertHelper.assertThrows(
        "Throw an error if there is a non existing parameter",
        MaestroInternalError.class,
        "Cannot interpolate [test $notExisting] as param [notExisting] is not found",
        () -> LiteralEvaluator.eval(param, params));
  }

  @Test
  public void testNotEvaluatedParam() {
    Parameter param = StringParameter.builder().name("test").value("test $notEvaluated").build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertEquals(new LinkedHashSet<>(Collections.singletonList("notEvaluated")), paramNames);

    AssertHelper.assertThrows(
        "Throw an error if there is an unevaluated parameter",
        MaestroInternalError.class,
        "Cannot interpolate [test $notEvaluated] as param [notEvaluated] is not evaluated yet",
        () -> LiteralEvaluator.eval(param, params));
  }

  @Test
  public void testOtherParamType() {
    Parameter param = LongParameter.builder().name("test").value(1L).build();

    // check get referenced param names
    Set<String> paramNames = LiteralEvaluator.getReferencedParamNames(param);
    Assert.assertTrue(paramNames.isEmpty());

    // string interpolation has no effect
    Object result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals(1L, result);

    param = DoubleParameter.builder().name("test").value(new BigDecimal("1.0")).build();
    result = LiteralEvaluator.eval(param, params);
    Assert.assertEquals(1.0, (Double) result, 0.00000000000000);

    param =
        DoubleArrayParameter.builder()
            .name("test")
            .value(new BigDecimal[] {new BigDecimal("1.0")})
            .build();
    result = LiteralEvaluator.eval(param, params);
    Assert.assertArrayEquals(new double[] {1.0}, (double[]) result, 0.00000000000000);
  }
}
