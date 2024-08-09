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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Set;
import javax.validation.ConstraintViolation;
import org.junit.Test;

public class CronConstraintTest extends BaseConstraintTest {
  private static class TestCron {
    @CronConstraint String cron;

    TestCron(String cron) {
      this.cron = cron;
    }
  }

  @Test
  public void testValid() {
    Set<ConstraintViolation<TestCron>> violations = validator.validate(new TestCron("0 0 * * *"));
    assertEquals(0, violations.size());
    violations = validator.validate(new TestCron("0-55/5 * * * *"));
    assertEquals(0, violations.size());
  }

  @Test
  public void testInvalid() {
    Set<ConstraintViolation<TestCron>> violations =
        validator.validate(new TestCron("* * blah blah"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestCron> violation = violations.iterator().next();
    assertThat(violation.getMessage())
        .contains("[cron expression] is not valid " + "- rejected value is [* * blah blah]");
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestCron>> violations = validator.validate(new TestCron(null));
    assertEquals(1, violations.size());
    ConstraintViolation<TestCron> violation = violations.iterator().next();
    assertNull(violation.getInvalidValue());
    assertEquals("[cron expression] cannot be null or empty", violation.getMessage());
  }
}
