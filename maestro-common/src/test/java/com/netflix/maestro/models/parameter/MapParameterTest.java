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
package com.netflix.maestro.models.parameter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.BeforeClass;
import org.junit.Test;

public class MapParameterTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowDefinition expected =
        loadObject("fixtures/parameters/sample-wf-map-params.json", WorkflowDefinition.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    WorkflowDefinition actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), WorkflowDefinition.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdeForEvaluatedResult() throws Exception {
    // given
    long[] la = new long[] {1L, 2L};
    boolean[] ba = new boolean[] {true, false};
    double[] da = new double[] {1.0, 2.0};
    String[] sa = new String[] {"Map", "Param"};
    long l = 3L;
    boolean b = true;
    double d = 1.0;
    String s = "string";
    Map<String, String> stringMap = new LinkedHashMap<>();
    stringMap.put("stringKey", "stringValue");

    Map<String, Object> nestedMap = new LinkedHashMap<>();
    nestedMap.put("long", l);
    nestedMap.put("boolean", b);
    nestedMap.put("double", d);
    nestedMap.put("String", s);
    nestedMap.put("longArray", la);
    nestedMap.put("booleanArray", ba);
    nestedMap.put("doubleArray", da);
    nestedMap.put("bigDecimalArray", Arrays.asList(new BigDecimal("1.0"), new BigDecimal("2.0")));
    nestedMap.put("stringArray", sa);
    nestedMap.put("emptyList", Collections.emptyList());
    nestedMap.put("stringmap", stringMap);

    Map<String, Object> expectedMap = new LinkedHashMap<>();
    expectedMap.put("nested", nestedMap);

    MapParameter writtenValue = MapParameter.builder().evaluatedResult(expectedMap).build();

    // when
    MapParameter readValue =
        MAPPER.readValue(MAPPER.writeValueAsString(writtenValue), MapParameter.class);

    // then
    assertEquals(writtenValue.getEvaluatedResult().size(), readValue.getEvaluatedResult().size());
    Map<String, Object> nestedWrittenMap =
        (Map<String, Object>) writtenValue.getEvaluatedResult().get("nested");
    Map<String, Object> nestedReadMap =
        (Map<String, Object>) readValue.getEvaluatedResult().get("nested");
    assertEquals(nestedWrittenMap.size(), nestedReadMap.size());
    for (Map.Entry<String, Object> entry : nestedWrittenMap.entrySet()) {
      assertTrue(Objects.deepEquals(entry.getValue(), nestedReadMap.get(entry.getKey())));
    }
  }
}
