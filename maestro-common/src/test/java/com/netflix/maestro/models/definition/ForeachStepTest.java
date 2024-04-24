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
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ForeachStepTest extends MaestroBaseTest {

  private Validator validator;

  @Before
  public void setup() {
    this.validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Test
  public void testSerde() throws Exception {
    ForeachStep def =
        (ForeachStep) loadObject("fixtures/foreachsteps/sample-foreach-step.json", Step.class);
    Assert.assertNotNull(def);
    ForeachStep def2 = (ForeachStep) MAPPER.readValue(MAPPER.writeValueAsString(def), Step.class);
    Assert.assertEquals(def, def2);
  }

  @Test
  public void testConcurrencyValidation() throws Exception {
    ForeachStep def =
        (ForeachStep) loadObject("fixtures/foreachsteps/sample-foreach-step.json", Step.class);
    def.setConcurrency(5L);
    Set<ConstraintViolation<ForeachStep>> violations = validator.validate(def);
    Assert.assertEquals(0, violations.size());

    def.setConcurrency(5000L);
    Assert.assertNotNull(def);
    violations = validator.validate(def);
    Assert.assertFalse(violations.isEmpty());
  }

  @Test
  public void testConcurrencyValidationNull() throws Exception {
    ForeachStep def =
        (ForeachStep) loadObject("fixtures/foreachsteps/sample-foreach-step.json", Step.class);
    def.setConcurrency(null);
    Set<ConstraintViolation<ForeachStep>> violations = validator.validate(def);
    Assert.assertEquals(0, violations.size());
  }
}
