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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.exceptions.MaestroInvalidExpressionException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import org.junit.Test;

public class ExprEvaluatorTest extends MaestroEngineBaseTest {

  @Test
  public void testEvalLiterals() {
    assertEquals(Boolean.TRUE, evaluator.eval("1 + 1 == 2", Collections.emptyMap()));
    assertEquals(Boolean.TRUE, evaluator.eval("1 + 1 > 1;", Collections.emptyMap()));
    assertEquals(Boolean.FALSE, evaluator.eval("1 + 1 < 1", Collections.emptyMap()));
  }

  @Test
  public void testThrowUserProvidedErrors() {
    AssertHelper.assertThrows(
        "support throwing user provided errors",
        MaestroInvalidExpressionException.class,
        "Expression throws an error [ERROR: user provided error] for expr",
        () -> evaluator.eval("1 + 1 == 2; throw 'user provided error';", Collections.emptyMap()));
    AssertHelper.assertThrows(
        "support throwing user provided errors",
        MaestroInvalidExpressionException.class,
        "Expression throws an error [ERROR: user provided error] for expr",
        () ->
            evaluator.eval(
                "if (1 + 1 == 2) throw 'user provided error'; else return 0;",
                Collections.emptyMap()));
  }

  @Test
  public void testTimeApis() {
    long expected =
        ZonedDateTime.of(2005, 3, 26, 20, 0, 0, 0, ZoneId.of("UTC"))
            .plusHours(2)
            .toInstant()
            .toEpochMilli();
    assertEquals(
        expected,
        evaluator.eval(
            "(new DateTime(2005, 3, 26, 12, 0, 0, 0, DateTimeZone.forID(\"America/Los_Angeles\"))"
                + ".toDateTime(DateTimeZone.UTC).plusHours(2).getMillis());",
            Collections.emptyMap()));
  }

  @Test
  public void testIncludedMethods() {
    assertArrayEquals(
        new long[] {20210101, 20210108, 20210115},
        (long[])
            evaluator.eval(
                "return Util.dateIntsBetween(startDateTime, startDateTime + 15, 7);",
                Collections.singletonMap("startDateTime", 20210101)));
  }

  @Test
  public void testMissingSemicolon() {
    assertEquals(11L, evaluator.eval("x + 1", Collections.singletonMap("x", 10)));
  }

  @Test
  public void testDefaultReturn() {
    assertEquals(1L, evaluator.eval("x = 1", Collections.singletonMap("x", 10)));
  }

  @Test
  public void testExtFunction() {
    assertEquals("\"10\"", evaluator.eval("Util.toJson(x)", Collections.singletonMap("x", "10")));
    var map = new HashMap<String, Integer>();
    map.put("foo", 10);
    assertEquals(
        "{\"foo\":10}", evaluator.eval("Util.toJson(x)", Collections.singletonMap("x", map)));
    assertEquals(
        "[10,20]",
        evaluator.eval("Util.toJson(x)", Collections.singletonMap("x", new long[] {10, 20})));
  }
}
