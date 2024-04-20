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

import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelMiscFuncTest {

  @Before
  public void setUp() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(12345L);
  }

  @After
  public void tearDown() throws Exception {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testCallCurrentTimeMillis() {
    SelType res = SelMiscFunc.INSTANCE.call("currentTimeMillis", new SelType[0]);
    assertEquals(SelTypes.LONG, res.type());
    assertEquals(12345L, ((SelLong) res).longVal());
  }

  @Test
  public void testCallArraysAsList() {
    SelType res =
        SelMiscFunc.INSTANCE.call(
            "asList", new SelType[] {SelArray.of(new long[] {1}, SelTypes.LONG_ARRAY)});
    assertEquals(SelTypes.LONG_ARRAY, res.type());
    assertEquals("[1]", (res).toString());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallCurrentTimeMillisWithWrongArgs() {
    SelMiscFunc.INSTANCE.call("currentTimeMillis", new SelType[1]);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallOtherMethods() {
    SelMiscFunc.INSTANCE.call("nonExisting", new SelType[0]);
  }

  @Test
  public void testSundayField() {
    SelLong res = SelMiscFunc.INSTANCE.field(SelString.of("SUNDAY"));
    assertEquals(7, res.longVal());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidField() {
    SelLong res = SelMiscFunc.INSTANCE.field(SelString.of("Tomorrow"));
  }
}
