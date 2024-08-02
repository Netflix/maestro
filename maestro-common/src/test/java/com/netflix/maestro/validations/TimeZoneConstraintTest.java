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

import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Test;

public class TimeZoneConstraintTest extends BaseConstraintTest {
  private static class TestTimeZone {
    @TimeZoneConstraint String timezone;

    TestTimeZone(String timezone) {
      this.timezone = timezone;
    }
  }

  @Test
  public void testValid() {
    Set<ConstraintViolation<TestTimeZone>> violations =
        validator.validate(new TestTimeZone("US/Pacific"));
    assertEquals(0, violations.size());
  }

  @Test
  public void testInvalid() {
    Set<ConstraintViolation<TestTimeZone>> violations =
        validator.validate(new TestTimeZone("US/Nowhere"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestTimeZone> violation = violations.iterator().next();
    assertThat(violation.getMessage()).contains("[timezone expression] is not valid");
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestTimeZone>> violations = validator.validate(new TestTimeZone(null));
    assertEquals(0, violations.size());
  }
}
