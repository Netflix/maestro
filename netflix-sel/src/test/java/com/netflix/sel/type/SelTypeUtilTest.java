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
package com.netflix.sel.type;

import static org.junit.Assert.*;

import com.netflix.sel.MockType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelTypeUtilTest {

  private MethodHandle m1;
  private MethodHandle m2;

  @Before
  public void setUp() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(12345L);
  }

  @After
  public void tearDown() throws Exception {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testTypeMatch() {
    SelTypeUtil.checkTypeMatch(SelTypes.NULL, SelTypes.NULL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeMisMatch() {
    SelTypeUtil.checkTypeMatch(SelTypes.NULL, SelTypes.VOID);
  }

  @Test
  public void testCallJavaMethodWithoutArg() throws Throwable {
    m1 =
        MethodHandles.lookup()
            .findStatic(MockType.class, "staticNoArg", MethodType.methodType(void.class));
    m2 =
        MethodHandles.lookup()
            .findVirtual(MockType.class, "noArg", MethodType.methodType(String.class));
    SelType res = SelTypeUtil.callJavaMethod(null, new SelType[0], m1, "staticNoArg");
    assertEquals(SelType.NULL, res);
    res = SelTypeUtil.callJavaMethod(new MockType(), new SelType[0], m2, "noArg");
    assertEquals(SelTypes.STRING, res.type());
    assertEquals("noArg", res.toString());
  }

  @Test
  public void testCallJavaMethodWithOneArg() throws Throwable {
    m1 =
        MethodHandles.lookup()
            .findStatic(
                MockType.class, "staticOneArg", MethodType.methodType(int.class, int.class));
    m2 =
        MethodHandles.lookup()
            .findVirtual(
                MockType.class, "oneArg", MethodType.methodType(String.class, String.class));
    SelType res =
        SelTypeUtil.callJavaMethod(null, new SelType[] {SelLong.of(123)}, m1, "staticOneArg");
    assertEquals(SelTypes.LONG, res.type());
    assertEquals(123L, ((SelLong) res).longVal());
    res =
        SelTypeUtil.callJavaMethod(
            new MockType(), new SelType[] {SelString.of("foo")}, m2, "oneArg");
    assertEquals(SelTypes.STRING, res.type());
    assertEquals("foo", res.toString());
  }

  @Test
  public void testCallJavaMethodWithTwoArgs() throws Throwable {
    m1 =
        MethodHandles.lookup()
            .findStatic(
                MockType.class,
                "staticTwoArgs",
                MethodType.methodType(double.class, double.class, boolean.class));
    m2 =
        MethodHandles.lookup()
            .findVirtual(
                MockType.class,
                "twoArgs",
                MethodType.methodType(String.class, String.class, DateTime.class));
    SelType res =
        SelTypeUtil.callJavaMethod(
            null, new SelType[] {SelDouble.of(1.2), SelBoolean.of(true)}, m1, "staticTwoArgs");
    assertEquals(SelTypes.DOUBLE, res.type());
    assertEquals(2.2, ((SelDouble) res).doubleVal(), 0.01);
    res =
        SelTypeUtil.callJavaMethod(
            new MockType(),
            new SelType[] {SelString.of("foo"), SelJodaDateTime.of(new DateTime(DateTimeZone.UTC))},
            m2,
            "twoArgs");
    assertEquals(SelTypes.STRING, res.type());
    assertEquals("foo1970-01-01T00:00:12.345Z", res.toString());
  }

  @Test
  public void testBox() {
    Object[] testObjects =
        new Object[] {
          null,
          "abc",
          1,
          1L,
          true,
          new String[] {"foo", "bar"},
          new Long[] {1L, 2L},
          new long[] {1L, 2L},
          new Integer[] {3, 4},
          new int[] {5, 6, 7},
          new Boolean[] {true, false},
          new boolean[] {},
          new HashMap(),
          new DateTime(DateTimeZone.UTC),
          new DateTime(DateTimeZone.UTC).dayOfWeek()
        };
    String[] expectedResults =
        new String[] {
          "NULL: NULL",
          "STRING: abc",
          "LONG: 1",
          "LONG: 1",
          "BOOLEAN: true",
          "STRING_ARRAY: [foo, bar]",
          "LONG_ARRAY: [1, 2]",
          "LONG_ARRAY: [1, 2]",
          "LONG_ARRAY: [3, 4]",
          "LONG_ARRAY: [5, 6, 7]",
          "BOOLEAN_ARRAY: [true, false]",
          "BOOLEAN_ARRAY: []",
          "MAP: {}",
          "DATETIME: 1970-01-01T00:00:12.345Z",
          "DATETIME_PROPERTY: Property[dayOfWeek]"
        };

    for (int i = 0; i < testObjects.length; ++i) {
      SelType res = SelTypeUtil.box(testObjects[i]);
      assertEquals(expectedResults[i], res.type() + ": " + res.toString());
    }
  }

  @Test
  public void testBoxDoubles() {
    SelType res = SelTypeUtil.box(0.1f);
    assertEquals(SelTypes.DOUBLE, res.type());
    assertEquals(0.1, ((SelDouble) res).doubleVal(), 0.01);
    res = SelTypeUtil.box(12.3);
    assertEquals(SelTypes.DOUBLE, res.type());
    assertEquals(12.3, ((SelDouble) res).doubleVal(), 0.01);

    Object[] testObjects =
        new Object[] {
          new Double[] {1.1}, new double[] {3.3}, new Float[] {1.2f}, new float[] {3.0f},
        };
    double[] expectedResults = new double[] {1.1, 3.3, 1.2, 3.0};
    for (int i = 0; i < testObjects.length; ++i) {
      res = SelTypeUtil.box(testObjects[i]);
      assertEquals(SelTypes.DOUBLE_ARRAY, res.type());
      assertEquals(expectedResults[i], ((double[]) res.unbox())[0], 0.01);
    }
  }

  @Test
  public void testBoxUnsupported() {
    Object[] testObjects =
        new Object[] {
          DateTimeZone.forID("UTC"),
          DateTimeFormat.forPattern("yyyy"),
          Days.days(1),
          new SimpleDateFormat("yyyyMMdd"),
          new Date(12345),
          new ArrayList()
        };
    for (int i = 0; i < testObjects.length; ++i) {
      try {
        SelTypeUtil.box(testObjects[i]);
      } catch (UnsupportedOperationException e) {
        // expected
      }
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFromStringToSelType() {
    assertEquals(SelTypes.LONG, SelTypeUtil.fromStringToSelType("int"));
    SelTypeUtil.fromStringToSelType("List");
  }

  @Test
  public void preprocess() {
    String str = "\"\\\\\\\'\\\"\\n\\\'/abc;\\t\\b\\n\\f\\r\\\\\"";
    assertEquals("\\\'\"\n\'/abc;\t\b\n\f\r\\", SelTypeUtil.preprocess(str));
  }

  @Test(expected = IllegalArgumentException.class)
  public void preprocessWrongStr() {
    SelTypeUtil.preprocess("\"\\\"");
  }
}
