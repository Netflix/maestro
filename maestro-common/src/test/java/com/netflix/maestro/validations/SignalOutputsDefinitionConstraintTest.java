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
import com.netflix.maestro.models.signal.SignalOutputsDefinition;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class SignalOutputsDefinitionConstraintTest extends BaseConstraintTest {

  private interface TestStep {
    SignalOutputsDefinition getOutputs();
  }

  @AllArgsConstructor
  @Data
  private static class TestStepImpl implements TestStep {
    @SignalOutputsDefinitionConstraint SignalOutputsDefinition outputs;
  }

  @Test
  public void testNoConstraintViolations() {
    var def = new SignalOutputsDefinition.SignalOutputDefinition();
    def.setName("foo");
    def.setParams(Map.of("bar", StringParamDefinition.builder().expression("123").build()));
    SignalOutputsDefinition definition =
        new SignalOutputsDefinition(Collections.singletonList(def));

    Set<ConstraintViolation<TestStep>> constraintViolations =
        validator.validate(new TestStepImpl(definition));
    Assertions.assertThat(constraintViolations).isEmpty();
  }

  @Test
  public void testNullOrEmptyDefinitions() {
    SignalOutputsDefinition outputs = new SignalOutputsDefinition(null);
    TestStep testStep = new TestStepImpl(outputs);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "Signal step outputs definitions cannot be null or empty or contain null elements");

    outputs = new SignalOutputsDefinition(Collections.emptyList());
    testStep = new TestStepImpl(outputs);
    constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo(
            "Signal step outputs definitions cannot be null or empty or contain null elements");
  }

  @Test
  public void testSignalNameConstraint() {
    var def = new SignalOutputsDefinition.SignalOutputDefinition();
    def.setParams(Map.of("bar", StringParamDefinition.builder().expression("123").build()));
    SignalOutputsDefinition definition =
        new SignalOutputsDefinition(Collections.singletonList(def));

    TestStep testStep = new TestStepImpl(definition);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(testStep.getOutputs()).isEqualTo(definition);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo("Signal step outputs definition doesn't contain mandatory name field");
  }
}
