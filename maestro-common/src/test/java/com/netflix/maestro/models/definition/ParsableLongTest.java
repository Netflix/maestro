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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import lombok.Data;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParsableLongTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Data
  private static class Numbers {
    ParsableLong number1;
    ParsableLong number2;
    ParsableLong number3;
  }

  @Test
  public void testDeserializationNumbers() throws Exception {
    Numbers deserialized =
        loadObject("fixtures/workflows/definition/sample-numbers.json", Numbers.class);
    Numbers expected = new Numbers();
    expected.setNumber1(ParsableLong.of(1L));
    expected.setNumber2(ParsableLong.of(0L));
    expected.setNumber3(ParsableLong.of("4"));

    assertEquals(expected, deserialized);
  }

  @Test
  public void testDeserializationStrings() throws Exception {
    Numbers deserialized =
        loadObject("fixtures/workflows/definition/sample-retry-strings.json", Numbers.class);
    Numbers expected = new Numbers();
    expected.setNumber1(ParsableLong.of("${foo}"));
    expected.setNumber2(ParsableLong.of("bar"));
    expected.setNumber3(ParsableLong.of("1+4"));

    assertEquals(expected, deserialized);
  }

  @Test
  public void testSerialization() throws Exception {
    Numbers numbers = new Numbers();
    numbers.setNumber1(ParsableLong.of(1L));
    numbers.setNumber2(ParsableLong.of(0L));
    numbers.setNumber3(ParsableLong.of("4"));

    String serialized = MAPPER.writeValueAsString(numbers);
    String expected = "{\"number1\":1,\"number2\":0,\"number3\":\"4\"}";

    assertEquals(expected, serialized);
  }
}
