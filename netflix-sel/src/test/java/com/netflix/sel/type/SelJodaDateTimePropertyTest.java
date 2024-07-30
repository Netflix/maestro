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

import static org.junit.Assert.assertEquals;

import com.netflix.sel.visitor.SelOp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelJodaDateTimePropertyTest {

  private SelJodaDateTimeProperty one;
  private SelJodaDateTimeProperty another;

  @Before
  public void setUp() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(12345L);
    one = SelJodaDateTimeProperty.of(new DateTime(DateTimeZone.UTC).dayOfWeek());
    another = SelJodaDateTimeProperty.of(new DateTime(DateTimeZone.UTC).dayOfMonth());
  }

  @After
  public void tearDown() throws Exception {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testAssignOps() {
    assertEquals("DATETIME_PROPERTY: Property[dayOfWeek]", one.type() + ": " + one);
    one.assignOps(SelOp.ASSIGN, another);
    assertEquals("DATETIME_PROPERTY: Property[dayOfMonth]", one.type() + ": " + one);
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
    SelType res = one.call("getAsText", new SelType[0]);
    assertEquals("STRING: Thursday", res.type() + ": " + res);
    res = one.call("withMinimumValue", new SelType[0]);
    assertEquals("DATETIME: 1969-12-29T00:00:12.345Z", res.type() + ": " + res);
    res = one.call("withMaximumValue", new SelType[0]);
    assertEquals("DATETIME: 1970-01-04T00:00:12.345Z", res.type() + ": " + res);
    res = one.call("get", new SelType[0]);
    assertEquals("LONG: 4", res.type() + ": " + res);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallArg() {
    one.call("getAsText", new SelType[] {SelType.NULL});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallMethod() {
    one.call("getAsString", new SelType[] {});
  }
}
