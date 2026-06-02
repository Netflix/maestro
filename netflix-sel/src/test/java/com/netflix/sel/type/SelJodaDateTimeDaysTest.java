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
import java.time.LocalDate;
import java.time.ZoneId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelJodaDateTimeDaysTest {
  private SelJodaDateTimeDays one;
  private SelJodaDateTimeDays another;

  @Before
  public void setUp() throws Exception {
    one = SelJodaDateTimeDays.of(2);
    another = SelJodaDateTimeDays.of(5);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testAssignOps() {
    one.assignOps(SelOp.ASSIGN, another);
    assertEquals("DATETIME_DAYS: 5", one.type() + ": " + one);
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
    SelType res = one.call("getDays", new SelType[0]);
    assertEquals("LONG: 2", res.type() + ": " + res);
    res =
        one.call(
            "daysBetween",
            new SelType[] {
              SelJodaDateTime.of(LocalDate.parse("2019-01-01").atStartOfDay(ZoneId.of("UTC"))),
              SelJodaDateTime.of(LocalDate.parse("2019-02-01").atStartOfDay(ZoneId.of("UTC")))
            });
    assertEquals("DATETIME_DAYS: 31", res.type() + ": " + res);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallArg() {
    one.call("getDays", new SelType[] {SelType.NULL});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallMethod() {
    one.call("getWeeks", new SelType[] {});
  }
}
