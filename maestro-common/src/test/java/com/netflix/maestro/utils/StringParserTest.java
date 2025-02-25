/*
 * Copyright 2025 Netflix, Inc.
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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.parameter.Parameter;
import org.junit.Test;

public class StringParserTest extends MaestroBaseTest {

  @Test
  public void testParseWithParam() {
    String input = "Hello ${name}";
    String expected = "Hello World";
    String actual =
        StringParser.parseWithParam(
            input,
            paramDefinition -> {
              Parameter param = paramDefinition.toParameter();
              param.setEvaluatedResult("Hello World");
              param.setEvaluatedTime(1L);
              return param;
            });
    assertEquals(expected, actual);
  }

  @Test
  public void testParseWithNullParam() {
    String input = "Hello ${name}";
    String expected = "Hello ${name}";
    String actual = StringParser.parseWithParam(input, paramDefinition -> null);
    assertEquals(expected, actual);
  }
}
