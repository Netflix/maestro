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
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TypedStepTest extends MaestroBaseTest {
  private Validator validator;

  @Before
  public void setup() {
    this.validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Test
  public void testSerDeStepDependencyDef() throws Exception {
    assertSerDe("fixtures/typedsteps/sample-typed-step.json");
  }

  private void assertSerDe(String fileName) throws Exception {
    TypedStep typedStep1 = (TypedStep) loadObject(fileName, Step.class);

    Set<ConstraintViolation<TypedStep>> constraintViolations = validator.validate(typedStep1);
    Assertions.assertThat(constraintViolations).isEmpty();

    Assert.assertNotNull(typedStep1);
    Assert.assertNotNull(typedStep1.getSignalDependencies());
    Assert.assertNotNull(typedStep1.getSignalOutputs());

    TypedStep typedStep2 =
        (TypedStep) MAPPER.readValue(MAPPER.writeValueAsString(typedStep1), Step.class);
    Assertions.assertThat(typedStep1).usingRecursiveComparison().isEqualTo(typedStep2);
  }
}
