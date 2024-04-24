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

import com.netflix.maestro.models.definition.SignalOutputsDefinition;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class StepOutputsDefinitionConstraintTest extends BaseConstraintTest {

  private interface TestStep {
    Map<StepOutputsDefinition.StepOutputType, StepOutputsDefinition> getOutputs();
  }

  @AllArgsConstructor
  @Data
  private static class TestStepImpl implements TestStep {
    Map<
            StepOutputsDefinition.StepOutputType,
            @StepOutputsDefinitionConstraint StepOutputsDefinition>
        outputs;
  }

  @Test
  public void testNoConstraintViolations() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("name", StringParamDefinition.builder().name("name").value("foo").build());
    paramDefMap.put("bar", StringParamDefinition.builder().name("bar").value("123").build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();

    StepOutputsDefinition definition =
        new SignalOutputsDefinition(Collections.singletonList(mapParamDefinition));
    Set<ConstraintViolation<TestStep>> constraintViolations =
        validator.validate(
            new TestStepImpl(
                Collections.singletonMap(StepOutputsDefinition.StepOutputType.SIGNAL, definition)));
    Assertions.assertThat(constraintViolations).isEmpty();
  }

  @Test
  public void testNullOrEmptyDefinitions() {
    StepOutputsDefinition definition = new SignalOutputsDefinition(Collections.singletonList(null));
    Map<StepOutputsDefinition.StepOutputType, StepOutputsDefinition> outputs =
        Collections.singletonMap(StepOutputsDefinition.StepOutputType.SIGNAL, definition);
    TestStep testStep = new TestStepImpl(outputs);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo("step outputs definitions cannot be null or empty or contain null elements");

    definition = new SignalOutputsDefinition(null);
    outputs = Collections.singletonMap(StepOutputsDefinition.StepOutputType.SIGNAL, definition);
    testStep = new TestStepImpl(outputs);
    constraintViolations = validator.validate(testStep);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo("step outputs definitions cannot be null or empty or contain null elements");
  }

  @Test
  public void testSignalNameConstraint() {
    Map<String, ParamDefinition> paramDefMap = new LinkedHashMap<>();
    paramDefMap.put("bar", StringParamDefinition.builder().name("bar").value("123").build());
    MapParamDefinition mapParamDefinition = MapParamDefinition.builder().value(paramDefMap).build();

    StepOutputsDefinition definition =
        new SignalOutputsDefinition(Collections.singletonList(mapParamDefinition));
    Map<StepOutputsDefinition.StepOutputType, StepOutputsDefinition> dependencies =
        Collections.singletonMap(StepOutputsDefinition.StepOutputType.SIGNAL, definition);
    TestStep testStep = new TestStepImpl(dependencies);
    Set<ConstraintViolation<TestStep>> constraintViolations = validator.validate(testStep);
    Assertions.assertThat(testStep.getOutputs()).isEqualTo(dependencies);
    Assertions.assertThat(constraintViolations).isNotEmpty();
    Assertions.assertThat(constraintViolations)
        .element(0)
        .extracting("message")
        .isEqualTo("step outputs definition doesn't contain mandatory name parameter definition");
  }
}
