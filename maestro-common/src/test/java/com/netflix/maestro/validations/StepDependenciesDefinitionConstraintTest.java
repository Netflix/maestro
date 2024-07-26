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

import com.netflix.maestro.models.definition.StepDependenciesDefinition;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.SignalOperator;
import com.netflix.maestro.models.parameter.SignalParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import jakarta.validation.ConstraintViolation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class StepDependenciesDefinitionConstraintTest extends BaseConstraintTest {

  private interface TestStep {
    Map<StepDependencyType, StepDependenciesDefinition> getDependencies();
  }

  @AllArgsConstructor
  @Data
  private static class TestStepImpl implements TestStep {
    Map<StepDependencyType, @StepDependenciesDefinitionConstraint StepDependenciesDefinition>
        dependencies;
  }

  @Test
  public void testNoConstraintViolations() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").value("foo").build());
    paramDefMap.put(
        "bar",
        SignalParamDefinition.builder()
            .operator(SignalOperator.EQUALS_TO)
            .parameter(StringParamDefinition.builder().name("bar").value("123").build())
            .build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();

    StepDependenciesDefinition definition =
        new StepDependenciesDefinition(
            Collections.singletonList(mapParamDefinition), StepDependencyType.SIGNAL);
    Set<ConstraintViolation<TestStep>> constraintViolations =
        validator.validate(
            new TestStepImpl(Collections.singletonMap(StepDependencyType.SIGNAL, definition)));
    Assertions.assertThat(constraintViolations).isEmpty();
  }

  @Test
  public void testNullOrEmptyDefinitions() {
    StepDependenciesDefinition definition =
        new StepDependenciesDefinition(Collections.singletonList(null), StepDependencyType.SIGNAL);
    Map<StepDependencyType, StepDependenciesDefinition> dependencies =
        Collections.singletonMap(StepDependencyType.SIGNAL, definition);
    TestStep testStep = new TestStepImpl(dependencies);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "signal dependencies definitions cannot be null or empty or contain null elements");

    definition = new StepDependenciesDefinition(null, StepDependencyType.SIGNAL);
    dependencies = Collections.singletonMap(StepDependencyType.SIGNAL, definition);
    testStep = new TestStepImpl(dependencies);
    constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "signal dependencies definitions cannot be null or empty or contain null elements");
  }

  @Test
  public void testSignalNameConstraint() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put(
        "bar",
        SignalParamDefinition.builder()
            .operator(SignalOperator.EQUALS_TO)
            .parameter(StringParamDefinition.builder().name("bar").value("123").build())
            .build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();

    StepDependenciesDefinition definition =
        new StepDependenciesDefinition(
            Collections.singletonList(mapParamDefinition), StepDependencyType.SIGNAL);
    Map<StepDependencyType, StepDependenciesDefinition> dependencies =
        Collections.singletonMap(StepDependencyType.SIGNAL, definition);
    TestStep testStep = new TestStepImpl(dependencies);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(testStep.getDependencies()).isEqualTo(dependencies);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "step dependency definition doesn't contain mandatory name parameter definition");
  }

  @Test
  public void testMoreThanOneSignalRangeParamConstraint() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").value("signal_a").build());
    paramDefMap.put(
        "p1",
        SignalParamDefinition.builder()
            .operator(SignalOperator.GREATER_THAN)
            .parameter(StringParamDefinition.builder().value("p1").build())
            .build());
    paramDefMap.put(
        "p2",
        SignalParamDefinition.builder()
            .operator(SignalOperator.LESS_THAN_EQUALS_TO)
            .parameter(StringParamDefinition.builder().value("p2").build())
            .build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();

    StepDependenciesDefinition definition =
        new StepDependenciesDefinition(
            Collections.singletonList(mapParamDefinition), StepDependencyType.SIGNAL);
    Map<StepDependencyType, StepDependenciesDefinition> dependencies =
        Collections.singletonMap(StepDependencyType.SIGNAL, definition);
    TestStep testStep = new TestStepImpl(dependencies);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "signal dependencies can not have two range params, signal name [signal_a] has two parameters [p1] and [p2] using range operators");
  }
}
