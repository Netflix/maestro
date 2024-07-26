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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Test;

public class PropertiesConstraintTest extends BaseConstraintTest {
  private static class TestProperties {
    @PropertiesConstraint Properties properties;

    TestProperties(Properties properties) {
      this.properties = properties;
    }
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestProperties>> violations =
        validator.validate(new TestProperties(null));
    assertEquals(0, violations.size());
  }

  @Test
  public void isWorkflowConcurrencyOverLimit() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    step.setType(StepType.SLEEP);
    Properties properties = new Properties();
    Set<ConstraintViolation<TestProperties>> violations =
        validator.validate(new TestProperties(properties));
    assertEquals(0, violations.size());

    properties.setRunStrategy(RunStrategy.create(3L));
    violations = validator.validate(new TestProperties(properties));
    assertEquals(0, violations.size());

    properties.setRunStrategy(RunStrategy.create(-1L));
    violations = validator.validate(new TestProperties(properties));
    assertEquals(1, violations.size());
    ConstraintViolation<TestProperties> violation = violations.iterator().next();
    assertEquals(
        "properties.runStrategy.workflowConcurrency", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow properties run_strategy] workflow_concurrency should be positive and no greater than "
            + Constants.WORKFLOW_CONCURRENCY_MAX_LIMIT
            + " - rejected value is [-1]",
        violation.getMessage());

    properties.setRunStrategy(RunStrategy.create(Constants.WORKFLOW_CONCURRENCY_MAX_LIMIT + 1));
    violations = validator.validate(new TestProperties(properties));
    assertEquals(1, violations.size());
    violation = violations.iterator().next();
    assertEquals(
        "properties.runStrategy.workflowConcurrency", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow properties run_strategy] workflow_concurrency should be positive and no greater than "
            + Constants.WORKFLOW_CONCURRENCY_MAX_LIMIT
            + " - rejected value is ["
            + (Constants.WORKFLOW_CONCURRENCY_MAX_LIMIT + 1)
            + "]",
        violation.getMessage());
  }

  @Test
  public void isStepConcurrencyOverLimit() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    step.setType(StepType.SLEEP);
    Properties properties = new Properties();
    Set<ConstraintViolation<TestProperties>> violations =
        validator.validate(new TestProperties(properties));
    assertEquals(0, violations.size());

    properties.setStepConcurrency(3L);
    violations = validator.validate(new TestProperties(properties));
    assertEquals(0, violations.size());

    properties.setStepConcurrency(-1L);
    violations = validator.validate(new TestProperties(properties));
    assertEquals(1, violations.size());
    ConstraintViolation<TestProperties> violation = violations.iterator().next();
    assertEquals("properties.stepConcurrency", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow properties] step_concurrency should be positive and no greater than "
            + Constants.STEP_CONCURRENCY_MAX_LIMIT
            + " - rejected value is [-1]",
        violation.getMessage());

    properties.setStepConcurrency(Constants.STEP_CONCURRENCY_MAX_LIMIT + 1);
    violations = validator.validate(new TestProperties(properties));
    assertEquals(1, violations.size());
    violation = violations.iterator().next();
    assertEquals("properties.stepConcurrency", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow properties] step_concurrency should be positive and no greater than "
            + Constants.STEP_CONCURRENCY_MAX_LIMIT
            + " - rejected value is ["
            + (Constants.STEP_CONCURRENCY_MAX_LIMIT + 1)
            + "]",
        violation.getMessage());
  }
}
