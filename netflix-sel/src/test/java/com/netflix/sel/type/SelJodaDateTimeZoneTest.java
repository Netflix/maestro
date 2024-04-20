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
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

public class SelJodaDateTimeZoneTest {

  private SelJodaDateTimeZone one;
  private SelJodaDateTimeZone another;

  @Before
  public void setUp() throws Exception {
    one = SelJodaDateTimeZone.of(DateTimeZone.UTC);
    another = SelJodaDateTimeZone.of(DateTimeZone.forID("America/Los_Angeles"));
  }

  @Test
  public void testAssignOps() {
    one.assignOps(SelOp.ASSIGN, another);
    assertEquals("DATETIME_ZONE: America/Los_Angeles", one.type() + ": " + one);
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
    SelType res = one.call("forID", new SelType[] {SelString.of("UTC")});
    assertEquals("DATETIME_ZONE: UTC", res.type() + ": " + res);
    res =
        one.call(
            "getOffset",
            new SelType[] {
              SelJodaDateTime.of(new DateTime(0, DateTimeZone.forID("America/Los_Angeles")))
            });
    assertEquals("LONG: 0", res.type() + ": " + res);
    res =
        another.call(
            "getOffset",
            new SelType[] {
              SelJodaDateTime.of(new DateTime(0, DateTimeZone.forID("America/Los_Angeles")))
            });
    assertEquals("LONG: -28800000", res.type() + ": " + res);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCall() {
    one.call("invalid", new SelType[] {SelLong.of(123)});
  }

  @Test
  public void testField() {
    SelType res = one.field(SelString.of("UTC"));
    assertEquals("DATETIME_ZONE: UTC", res.type() + ": " + res);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidField() {
    one.field(SelString.of("ABC"));
  }
}
