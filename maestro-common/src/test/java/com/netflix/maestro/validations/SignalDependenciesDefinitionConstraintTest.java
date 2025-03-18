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
package com.netflix.maestro.validations;

import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.signal.SignalDependenciesDefinition;
import com.netflix.maestro.models.signal.SignalMatchParamDef;
import com.netflix.maestro.models.signal.SignalOperator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class SignalDependenciesDefinitionConstraintTest extends BaseConstraintTest {

  private interface TestStep {
    SignalDependenciesDefinition getDependencies();
  }

  @AllArgsConstructor
  @Data
  private static class TestStepImpl implements TestStep {
    @SignalDependenciesDefinitionConstraint SignalDependenciesDefinition dependencies;
  }

  @Test
  public void testNoConstraintViolations() {
    var def = new SignalDependenciesDefinition.SignalDependencyDefinition();
    def.setName("foo");
    def.setMatchParams(
        Map.of(
            "bar",
            SignalMatchParamDef.builder()
                .operator(SignalOperator.EQUALS_TO)
                .param(StringParamDefinition.builder().name("bar").value("123").build())
                .build()));
    SignalDependenciesDefinition definition =
        new SignalDependenciesDefinition(Collections.singletonList(def));
    Set<ConstraintViolation<TestStep>> constraintViolations =
        validator.validate(new TestStepImpl(definition));
    Assertions.assertThat(constraintViolations).isEmpty();
  }

  @Test
  public void testNullOrEmptyDefinitions() {
    SignalDependenciesDefinition dependencies = new SignalDependenciesDefinition(null);
    TestStep testStep = new TestStepImpl(dependencies);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "signal dependencies definitions cannot be null or empty or contain null elements");

    dependencies =
        new SignalDependenciesDefinition(
            Collections.singletonList(
                new SignalDependenciesDefinition.SignalDependencyDefinition()));
    testStep = new TestStepImpl(dependencies);
    constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo("Signal step dependency definition doesn't contain mandatory name field");
  }

  @Test
  public void testSignalNameConstraint() {
    var def = new SignalDependenciesDefinition.SignalDependencyDefinition();
    def.setMatchParams(
        Map.of(
            "bar",
            SignalMatchParamDef.builder()
                .operator(SignalOperator.EQUALS_TO)
                .param(StringParamDefinition.builder().name("bar").value("123").build())
                .build()));
    SignalDependenciesDefinition definition =
        new SignalDependenciesDefinition(Collections.singletonList(def));
    TestStep testStep = new TestStepImpl(definition);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(testStep.getDependencies()).isEqualTo(definition);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo("Signal step dependency definition doesn't contain mandatory name field");
  }

  @Test
  public void testMoreThanOneSignalRangeParamConstraint() {
    var def = new SignalDependenciesDefinition.SignalDependencyDefinition();
    def.setName("signal_a");
    Map<String, SignalMatchParamDef> matchParams = new LinkedHashMap<>();
    matchParams.put(
        "p1",
        SignalMatchParamDef.builder()
            .operator(SignalOperator.GREATER_THAN)
            .param(StringParamDefinition.builder().value("p1").build())
            .build());
    matchParams.put(
        "p2",
        SignalMatchParamDef.builder()
            .operator(SignalOperator.LESS_THAN_EQUALS_TO)
            .param(StringParamDefinition.builder().value("p2").build())
            .build());
    def.setMatchParams(matchParams);
    SignalDependenciesDefinition definition =
        new SignalDependenciesDefinition(Collections.singletonList(def));

    TestStep testStep = new TestStepImpl(definition);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "Invalid signal dependency for signal [signal_a] with more than one range params: [p1] and [p2]");
  }
}
