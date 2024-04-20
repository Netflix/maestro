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

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelUtilFuncTest {

  @Before
  public void setUp() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(12345L);
  }

  @After
  public void tearDown() throws Exception {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testCallIncrementDateInt() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "incrementDateInt", new SelType[] {SelString.of("20190101"), SelLong.of(5)});
    assertEquals("STRING: 20190106", res.type() + ": " + res.toString());
    res =
        SelUtilFunc.INSTANCE.call(
            "incrementDateInt", new SelType[] {SelLong.of("20190101"), SelLong.of(5)});
    assertEquals("STRING: 20190106", res.type() + ": " + res.toString());
  }

  @Test(expected = ClassCastException.class)
  public void testCallIncrementDateIntInvalid() {
    SelUtilFunc.INSTANCE.call(
        "incrementDateInt", new SelType[] {SelString.of("20190101"), SelString.of("5")});
  }

  @Test
  public void testCallDateIntToTs() {
    SelType res =
        SelUtilFunc.INSTANCE.call("dateIntToTs", new SelType[] {SelString.of("20190101")});
    assertEquals("LONG: 1546300800000", res.type() + ": " + res.toString());
    res = SelUtilFunc.INSTANCE.call("dateIntToTs", new SelType[] {SelLong.of("20190101")});
    assertEquals("LONG: 1546300800000", res.type() + ": " + res.toString());
  }

  @Test(expected = IllegalFieldValueException.class)
  public void testCallDateIntToTsInvalid() {
    SelUtilFunc.INSTANCE.call("dateIntToTs", new SelType[] {SelLong.of(20200230)});
  }

  @Test
  public void testCallDateIntHourToTs() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "dateIntHourToTs",
            new SelType[] {
              SelString.of("20190101"),
              SelString.of("01"),
              SelString.of("UTC"),
              SelString.of("01"),
              SelString.of("01")
            });
    assertEquals("LONG: 1546214400000", res.type() + ": " + res.toString());
    res =
        SelUtilFunc.INSTANCE.call(
            "dateIntHourToTs",
            new SelType[] {
              SelLong.of("20190101"),
              SelLong.of("01"),
              SelString.of("UTC"),
              SelLong.of("01"),
              SelLong.of("01")
            });
    assertEquals("LONG: 1546214400000", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallTimeoutForDateTimeDeadline() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "timeoutForDateTimeDeadline",
            new SelType[] {
              SelJodaDateTime.of(new DateTime("2019-01-01", DateTimeZone.UTC)),
              SelString.of("1 day")
            });
    assertEquals("STRING: 1546387187655 milliseconds", res.type() + ": " + res.toString());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallTimeoutForDateTimeDeadlineInvalid() {
    SelUtilFunc.INSTANCE.call("timeoutForDateTimeDeadline", new SelType[] {SelString.of("1 day")});
  }

  @Test
  public void testCallTimeoutForDateIntDeadline() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "timeoutForDateIntDeadline",
            new SelType[] {SelString.of("20190101"), SelString.of("1 day")});
    assertEquals("STRING: 1546387187655 milliseconds", res.type() + ": " + res.toString());
    res =
        SelUtilFunc.INSTANCE.call(
            "timeoutForDateIntDeadline",
            new SelType[] {SelLong.of(20190101), SelString.of("1 day")});
    assertEquals("STRING: 1546387187655 milliseconds", res.type() + ": " + res.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCallTimeoutForDateIntDeadlineInvalidInput() {
    SelUtilFunc.INSTANCE.call(
        "timeoutForDateIntDeadline",
        new SelType[] {SelString.of("2019010101"), SelString.of("1 day")});
  }

  @Test
  public void testCallTsToDateInt() {
    SelType res = SelUtilFunc.INSTANCE.call("tsToDateInt", new SelType[] {SelLong.of("12345")});
    assertEquals("STRING: 19700101", res.type() + ": " + res.toString());
    res = SelUtilFunc.INSTANCE.call("tsToDateInt", new SelType[] {SelString.of("12345")});
    assertEquals("STRING: 19700101", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallDateIntsBetween() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelString.of("20190226"), SelLong.of(20190303), SelString.of("1")});
    assertEquals(
        "LONG_ARRAY: [20190226, 20190227, 20190228, 20190301, 20190302]",
        res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelLong.of("20181228"), SelString.of("20191002"), SelLong.of(90)});
    assertEquals(
        "LONG_ARRAY: [20181228, 20190328, 20190626, 20190924]", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelLong.of(20200226), SelLong.of(20200303), SelLong.of(1)});
    assertEquals(
        "LONG_ARRAY: [20200226, 20200227, 20200228, 20200229, 20200301, 20200302]",
        res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelLong.of(20191226), SelLong.of(20190303), SelLong.of(1)});
    assertEquals("LONG_ARRAY: []", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelString.of("20190303"), SelLong.of(20190226), SelString.of("-1")});
    assertEquals(
        "LONG_ARRAY: [20190302, 20190301, 20190228, 20190227, 20190226]",
        res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelLong.of("20181228"), SelString.of("20191002"), SelLong.of(-90)});
    assertEquals("LONG_ARRAY: []", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallIntsBetween() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "intsBetween", new SelType[] {SelString.of("5"), SelLong.of("10"), SelLong.of(2)});
    assertEquals("LONG_ARRAY: [5, 7, 9]", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "intsBetween", new SelType[] {SelLong.of("-5"), SelString.of("1"), SelLong.of(2)});
    assertEquals("LONG_ARRAY: [-5, -3, -1]", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "intsBetween", new SelType[] {SelLong.of("5"), SelString.of("1"), SelString.of("2")});
    assertEquals("LONG_ARRAY: []", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "intsBetween", new SelType[] {SelLong.of("-5"), SelString.of("1"), SelLong.of(-2)});
    assertEquals("LONG_ARRAY: []", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "intsBetween", new SelType[] {SelLong.of("1"), SelString.of("-5"), SelLong.of(-2)});
    assertEquals("LONG_ARRAY: [1, -1, -3]", res.type() + ": " + res.toString());

    res =
        SelUtilFunc.INSTANCE.call(
            "intsBetween", new SelType[] {SelLong.of("5"), SelString.of("1"), SelString.of("-2")});
    assertEquals("LONG_ARRAY: [5, 3]", res.type() + ": " + res.toString());
  }

  @Test(expected = NumberFormatException.class)
  public void testCallTsToDateIntInvalid() {
    SelUtilFunc.INSTANCE.call("tsToDateInt", new SelType[] {SelString.of("123.45")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCall() {
    SelUtilFunc.INSTANCE.call("invalidMethod", new SelType[] {SelString.of("12345")});
  }

  @Test(expected = IllegalFieldValueException.class)
  public void testInvalidCallDateIntsBetween() {
    SelUtilFunc.INSTANCE.call(
        "dateIntsBetween",
        new SelType[] {SelLong.of(20190229), SelLong.of(20190303), SelLong.of(1)});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCallDateIntsBetweenWithZeroIncrement() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelLong.of("20181228"), SelString.of("20191002"), SelLong.of(0)});
    assertEquals("LONG_ARRAY: []", res.type() + ": " + res.toString());
  }

  @Test(expected = NumberFormatException.class)
  public void testInvalidCallDateIntsBetweenBadNumber() {
    SelType res =
        SelUtilFunc.INSTANCE.call(
            "dateIntsBetween",
            new SelType[] {SelLong.of("20181228"), SelString.of("20191002"), SelLong.of("abc")});
    assertEquals("LONG_ARRAY: []", res.type() + ": " + res.toString());
  }

  @Test(expected = NumberFormatException.class)
  public void testInvalidCallIntsBetween() {
    SelUtilFunc.INSTANCE.call(
        "intsBetween", new SelType[] {SelString.of("foo"), SelLong.of(3), SelLong.of(1)});
  }
}
