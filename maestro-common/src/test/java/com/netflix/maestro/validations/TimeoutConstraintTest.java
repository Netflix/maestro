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

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ParsableLong;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TimeoutConstraintTest extends BaseConstraintTest {
  private static class DummyWorkflow {
    @TimeoutConstraint ParsableLong timeout;

    DummyWorkflow(long timeout) {
      this.timeout = ParsableLong.of(timeout);
    }

    DummyWorkflow(String timeout) {
      this.timeout = ParsableLong.of(timeout);
    }
  }

  @Test
  public void testValid() {
    Assert.assertTrue(validator.validate(new DummyWorkflow(100L)).isEmpty());
    Assert.assertTrue(validator.validate(new DummyWorkflow("100")).isEmpty());
    Assert.assertTrue(validator.validate(new DummyWorkflow("10 min")).isEmpty());
    Assert.assertTrue(validator.validate(new DummyWorkflow("${foo}")).isEmpty());
  }

  @Test
  public void testInValid() {
    Set<ConstraintViolation<DummyWorkflow>> violations = validator.validate(new DummyWorkflow(0L));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [0 ms]/[0] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());

    violations = validator.validate(new DummyWorkflow(-10L));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [-10000 ms]/[-10] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());

    violations =
        validator.validate(new DummyWorkflow(Constants.MAX_TIME_OUT_LIMIT_IN_MILLIS / 1000 + 1));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [10368001000 ms]/[10368001] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());

    violations = validator.validate(new DummyWorkflow("0"));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [0 ms]/[\"0\"] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());

    violations = validator.validate(new DummyWorkflow("-10"));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [-10000 ms]/[\"-10\"] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());

    violations = validator.validate(new DummyWorkflow("foo"));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [0 ms]/[\"foo\"] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());

    violations = validator.validate(new DummyWorkflow("1+1"));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "timeout [0 ms]/[\"1+1\"] cannot be non-positive or more than system limit: 120 days",
        violations.iterator().next().getMessage());
  }
}
