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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalDependenciesTest extends MaestroBaseTest {
  private SignalDependencies signalDependencies;

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Before
  public void before() {
    var dependency = new SignalDependencies.SignalDependency();
    dependency.setName("signal_a");
    dependency.setMatchParams(
        Map.of(
            "param_a",
            SignalMatchParam.builder()
                .value(SignalParamValue.of("test123"))
                .operator(SignalOperator.EQUALS_TO)
                .build()));
    dependency.setStatus(StepDependencyMatchStatus.PENDING);
    this.signalDependencies = new SignalDependencies();
    this.signalDependencies.setDependencies(List.of(dependency));
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    signalDependencies =
        loadObject(
            "fixtures/signal/sample-signal-multiple-dependencies.json", SignalDependencies.class);
    String ser1 = MAPPER.writeValueAsString(signalDependencies);
    SignalDependencies actual =
        MAPPER.readValue(MAPPER.writeValueAsString(signalDependencies), SignalDependencies.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(signalDependencies, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void shouldSerDe() throws IOException {
    SignalDependencies expected =
        loadObject("fixtures/signal/sample-signal-dependencies.json", SignalDependencies.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    SignalDependencies actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), SignalDependencies.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void shouldInitializeWithPendingStatus() {
    assertThat(signalDependencies.getDependencies())
        .hasSize(1)
        .first()
        .matches(i -> i.getStatus().equals(StepDependencyMatchStatus.PENDING))
        .matches(i -> i.getMatchParams().get("param_a").getValue().getString().equals("test123"));
    assertThat(signalDependencies.getInfo()).isNull();
  }

  @Test
  public void shouldByPassStepDependencies() {
    User user = User.create("maestro");
    long actionTime = 12345L;
    signalDependencies.bypass(user, actionTime);
    assertThat(signalDependencies.isSatisfied()).isTrue();
    assertThat(signalDependencies.getInfo())
        .extracting("message")
        .isEqualTo("Signal step dependencies have been bypassed by user [maestro]");
    assertThat(signalDependencies.getInfo()).extracting("timestamp").isEqualTo(actionTime);
  }
}
