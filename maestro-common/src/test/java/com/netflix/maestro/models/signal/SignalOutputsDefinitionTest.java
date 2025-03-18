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
package com.netflix.maestro.models.signal;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import java.util.Collections;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class SignalOutputsDefinitionTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    var def = new SignalOutputsDefinition.SignalOutputDefinition();
    def.setName("out");
    def.setParams(
        Map.of(
            "p1",
            LongParamDefinition.builder().name("p1").value(1L).build(),
            "p2",
            StringParamDefinition.builder().name("p2").expression("1+1").build()));
    SignalOutputsDefinition definition =
        new SignalOutputsDefinition(Collections.singletonList(def));

    String jsonStr = MAPPER.writeValueAsString(definition);
    SignalOutputsDefinition deserializedDef =
        MAPPER.readValue(jsonStr, SignalOutputsDefinition.class);
    Assertions.assertThat(definition).usingRecursiveComparison().isEqualTo(deserializedDef);
    Assert.assertEquals(
        loadJson("fixtures/step_outputs/step_outputs_definition.json")
            .replace("\n", "")
            .replace(" ", "")
            .replace("\r", ""),
        MAPPER.writeValueAsString(
            MAPPER.readValue(
                loadJson("fixtures/step_outputs/step_outputs_definition.json"),
                SignalOutputsDefinition.class)));
  }
}
