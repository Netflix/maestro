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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class ParamHelperTest {

  @Test
  public void testDoubleArrayToDecimalArray() {
    Object val = new double[] {1.2, 3.4, 5.6};
    BigDecimal[] actual = ParamHelper.toDecimalArray("foo", val);
    assertEquals(1.2, actual[0].doubleValue(), 0.00000000);
    assertEquals(3.4, actual[1].doubleValue(), 0.00000000);
    assertEquals(5.6, actual[2].doubleValue(), 0.00000000);
  }

  @Test
  public void testDecimalArrayToDecimalArray() {
    BigDecimal[] val =
        new BigDecimal[] {new BigDecimal("1.2"), new BigDecimal("3.4"), new BigDecimal("5.6")};
    BigDecimal[] actual = ParamHelper.toDecimalArray("foo", val);
    assertArrayEquals(val, actual);
  }

  @Test
  public void testListToDecimalArray() {
    Object val = Arrays.asList(new BigDecimal("1.2"), "3.4", 5.6);
    BigDecimal[] actual = ParamHelper.toDecimalArray("foo", val);
    assertEquals(1.2, actual[0].doubleValue(), 0.00000000);
    assertEquals(3.4, actual[1].doubleValue(), 0.00000000);
    assertEquals(5.6, actual[2].doubleValue(), 0.00000000);
  }

  @Test
  public void testInvalidToDecimalArray() {
    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Invalid number format for value: [true, 5.6]",
        () -> ParamHelper.toDecimalArray("foo", Arrays.asList(true, 5.6)));

    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Invalid number format for value: [3.4abc, 5.6]",
        () -> ParamHelper.toDecimalArray("foo", Arrays.asList("3.4abc", 5.6)));

    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Invalid number format for value: [null, 5.6]",
        () -> ParamHelper.toDecimalArray("foo", Arrays.asList(null, 5.6)));

    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Cannot cast value [null] into a BigDecimal array",
        () -> ParamHelper.toDecimalArray("foo", null));
  }

  @Test
  public void testDoubleArrayToDoubleArray() {
    double[] val = new double[] {1.2, 3.4, 5.6};
    double[] actual = ParamHelper.toDoubleArray("foo", val);
    assertArrayEquals(val, actual, 0.00001);
  }

  @Test
  public void testDecimalArrayToDoubleArray() {
    Object val =
        new BigDecimal[] {new BigDecimal("1.2"), new BigDecimal("3.4"), new BigDecimal("5.6")};
    double[] actual = ParamHelper.toDoubleArray("foo", val);
    assertEquals(1.2, actual[0], 0.00000000);
    assertEquals(3.4, actual[1], 0.00000000);
    assertEquals(5.6, actual[2], 0.00000000);
  }

  @Test
  public void testListToDoubleArray() {
    Object val = Arrays.asList(new BigDecimal("1.2"), "3.4", 5.6);
    double[] actual = ParamHelper.toDoubleArray("foo", val);
    assertEquals(1.2, actual[0], 0.00000000);
    assertEquals(3.4, actual[1], 0.00000000);
    assertEquals(5.6, actual[2], 0.00000000);
  }

  @Test
  public void testInvalidToDoubleArray() {
    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Invalid number format for evaluated result: [true, 5.6]",
        () -> ParamHelper.toDoubleArray("foo", Arrays.asList(true, 5.6)));

    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Invalid number format for evaluated result: [3.4abc, 5.6]",
        () -> ParamHelper.toDoubleArray("foo", Arrays.asList("3.4abc", 5.6)));

    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Invalid number format for evaluated result: [null, 5.6]",
        () -> ParamHelper.toDoubleArray("foo", Arrays.asList(null, 5.6)));

    AssertHelper.assertThrows(
        "Invalid number format",
        MaestroInternalError.class,
        "Param [foo] has an invalid evaluated result [null]",
        () -> ParamHelper.toDoubleArray("foo", null));
  }

  @Test
  public void testDeriveTypedParameter() {
    Parameter param = ParamHelper.deriveTypedParameter("foo", "test-expr", 123L, null, null, null);
    assertEquals(ParamType.LONG, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", Boolean.TRUE, null, null, null);
    assertEquals(ParamType.BOOLEAN, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", 12.3, null, null, null);
    assertEquals(ParamType.DOUBLE, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", "bar", null, null, null);
    assertEquals(ParamType.STRING, param.getType());
    param =
        ParamHelper.deriveTypedParameter(
            "foo", "test-expr", Collections.emptyMap(), null, null, null);
    assertEquals(ParamType.MAP, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", new long[0], null, null, null);
    assertEquals(ParamType.LONG_ARRAY, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", new boolean[0], null, null, null);
    assertEquals(ParamType.BOOLEAN_ARRAY, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", new double[0], null, null, null);
    assertEquals(ParamType.DOUBLE_ARRAY, param.getType());
    param = ParamHelper.deriveTypedParameter("foo", "test-expr", new String[0], null, null, null);
    assertEquals(ParamType.STRING_ARRAY, param.getType());
    param =
        ParamHelper.deriveTypedParameter(
            "foo", "test-expr", Collections.singletonList(123L), null, null, null);
    assertEquals(ParamType.LONG_ARRAY, param.getType());
    param =
        ParamHelper.deriveTypedParameter(
            "foo", "test-expr", Collections.singletonList(true), null, null, null);
    assertEquals(ParamType.BOOLEAN_ARRAY, param.getType());
    param =
        ParamHelper.deriveTypedParameter(
            "foo", "test-expr", Collections.singletonList(12.3), null, null, null);
    assertEquals(ParamType.DOUBLE_ARRAY, param.getType());
    param =
        ParamHelper.deriveTypedParameter(
            "foo",
            "test-expr",
            Collections.singletonList(new BigDecimal("12.3")),
            null,
            null,
            null);
    assertEquals(ParamType.DOUBLE_ARRAY, param.getType());
    param =
        ParamHelper.deriveTypedParameter(
            "foo", "test-expr", Collections.singletonList("bar"), null, null, null);
    assertEquals(ParamType.STRING_ARRAY, param.getType());
  }
}
