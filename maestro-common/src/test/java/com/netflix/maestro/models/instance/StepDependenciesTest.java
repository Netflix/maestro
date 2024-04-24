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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.SignalOperator;
import com.netflix.maestro.models.parameter.SignalParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StepDependenciesTest extends MaestroBaseTest {
  private MapParameter mapParameter;
  private StepDependencies stepDependencies;

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Before
  public void before() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").value("signal_a").build());
    paramDefMap.put(
        "param_a",
        SignalParamDefinition.builder()
            .operator(SignalOperator.EQUALS_TO)
            .parameter(StringParamDefinition.builder().name("param_a").value("test123").build())
            .build());
    Map<String, Object> evaluatedResult = new HashMap<>();
    evaluatedResult.put("name", "signal_a");
    evaluatedResult.put("param_a", "test123");

    this.mapParameter =
        MapParameter.builder()
            .value(paramDefMap)
            .evaluatedResult(evaluatedResult)
            .evaluatedTime(12345L)
            .build();

    this.stepDependencies =
        new StepDependencies(StepDependencyType.SIGNAL, Collections.singletonList(mapParameter));
  }

  @Test
  public void shouldSerDe() throws IOException {
    StepDependencies expected =
        loadObject(
            "fixtures/instances/sample-step-dependencies-summary.json", StepDependencies.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    StepDependencies actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), StepDependencies.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void shouldInitializeWithPendingStatus() {
    assertThat(stepDependencies.getStatuses())
        .hasSize(1)
        .first()
        .matches(i -> i.getStatus().equals(StepDependencyMatchStatus.PENDING))
        .matches(
            i ->
                i.getParams()
                    .getEvaluatedParam("name")
                    .getEvaluatedResultString()
                    .equals("signal_a"));
    assertThat(stepDependencies.getType()).isEqualTo(StepDependencyType.SIGNAL);
    assertThat(stepDependencies.getInfo()).isNull();
  }

  @Test
  public void shouldByPassStepDependencies() {
    User user = User.create("maestro");
    long actionTime = 12345L;
    stepDependencies.bypass(user, actionTime);
    assertThat(stepDependencies.isSatisfied()).isTrue();
    assertThat(stepDependencies.getInfo())
        .extracting("message")
        .isEqualTo("Step dependencies have been bypassed by user maestro");
    assertThat(stepDependencies.getInfo()).extracting("timestamp").isEqualTo(actionTime);
  }
}
