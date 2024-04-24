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
package com.netflix.maestro.models.instance;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.parameter.MapParameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class StepOutputsTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    Map<String, Object> evaluatedResults = new HashMap<>();
    evaluatedResults.put("name", "sigOutput");
    evaluatedResults.put("p1", 1L);
    evaluatedResults.put("p2", "v2");

    MapParameter mapParameter = MapParameter.builder().evaluatedResult(evaluatedResults).build();
    OutputSignalInstance outputSignalInstance = new OutputSignalInstance();
    outputSignalInstance.setOutputSignalInstanceId("123");
    outputSignalInstance.setAnnouncedTime(123L);
    StepOutputs stepOutputs =
        new SignalStepOutputs(
            Collections.singletonList(
                new SignalStepOutputs.SignalStepOutput(mapParameter, outputSignalInstance)));

    Assertions.assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(stepOutputs), StepOutputs.class))
        .usingRecursiveComparison()
        .isEqualTo(stepOutputs);
  }
}
