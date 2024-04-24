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
package com.netflix.maestro.models.definition;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class StepOutputsDefinitionTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    Map<String, ParamDefinition> def = new HashMap<>();
    def.put("name", StringParamDefinition.builder().value("out").build());
    def.put("p1", LongParamDefinition.builder().value(1L).build());
    def.put("p2", StringParamDefinition.builder().expression("1+1").build());

    MapParamDefinition mapParameter = MapParamDefinition.builder().value(def).build();
    StepOutputsDefinition definition =
        new SignalOutputsDefinition(Collections.singletonList(mapParameter));
    String jsonStr = MAPPER.writeValueAsString(definition);
    StepOutputsDefinition deserializedDef = MAPPER.readValue(jsonStr, StepOutputsDefinition.class);
    Assertions.assertThat(definition).usingRecursiveComparison().isEqualTo(deserializedDef);
    Assert.assertEquals(
        loadJson("fixtures/step_outputs/step_outputs_definition.json")
            .replace("\n", "")
            .replace(" ", ""),
        MAPPER.writeValueAsString(
            MAPPER.readValue(
                loadJson("fixtures/step_outputs/step_outputs_definition.json"),
                StepOutputsDefinition.class)));
  }
}
