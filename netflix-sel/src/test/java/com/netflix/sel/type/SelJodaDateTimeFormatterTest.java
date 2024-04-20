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

import com.netflix.sel.visitor.SelOp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelJodaDateTimeFormatterTest {

  private SelJodaDateTimeFormatter one;
  private SelJodaDateTimeFormatter another;

  @Before
  public void setUp() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(12345L);
    one = SelJodaDateTimeFormatter.of(DateTimeFormat.forPattern("yyyy").withZoneUTC());
    another = SelJodaDateTimeFormatter.of(DateTimeFormat.forPattern("yyyyMMdd"));
  }

  @After
  public void tearDown() throws Exception {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testAssignOps() {
    one.assignOps(SelOp.ASSIGN, another);
    assertEquals(another.getInternalVal(), one.getInternalVal());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAssignType() {
    one.assignOps(SelOp.ASSIGN, SelString.of("foo"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOps() {
    one.assignOps(SelOp.ADD_ASSIGN, SelString.of("foo"));
  }

  @Test
  public void testCalls() {
    SelType res = one.call("withZone", new SelType[] {SelJodaDateTimeZone.of(DateTimeZone.UTC)});
    assertEquals(
        "DATETIME_FORMATTER: UTC",
        one.type() + ": " + ((SelJodaDateTimeFormatter) res).getInternalVal().getZone());
    res = one.call("parseDateTime", new SelType[] {SelString.of("2019")});
    assertEquals("DATETIME: 2019-01-01T00:00:00.000Z", res.type() + ": " + res);
    res = one.call("parseDateTime", new SelType[] {SelLong.of(2019)});
    assertEquals("DATETIME: 2019-01-01T00:00:00.000Z", res.type() + ": " + res);
    res = one.call("parseMillis", new SelType[] {SelString.of("2019")});
    assertEquals("LONG: 1546300800000", res.type() + ": " + res);
    res = one.call("forPattern", new SelType[] {SelString.of("yyyyMMdd")});
    assertEquals(another.getInternalVal(), res.getInternalVal());
    res = one.call("print", new SelType[] {SelLong.of(12345)});
    assertEquals("STRING: 1970", res.type() + ": " + res);
    res = another.call("print", new SelType[] {SelJodaDateTime.of(new DateTime(DateTimeZone.UTC))});
    assertEquals("STRING: 19700101", res.type() + ": " + res);
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidCallArg() {
    one.call("withZone", new SelType[] {SelType.NULL});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallMethod() {
    one.call("invalid", new SelType[] {});
  }
}
