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

import com.netflix.maestro.models.definition.Tct;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Test;

public class TctConstraintTest extends BaseConstraintTest {
  private static class TestTct {
    @TctConstraint Tct tct;

    TestTct(Tct tct) {
      this.tct = tct;
    }
  }

  @Test
  public void testMultipleFieldsSet() {
    Tct tct = new Tct();
    tct.setDurationMinutes(60);
    tct.setCompletedByHour(1);
    tct.setTz("UTC");
    tct.setCompletedByTs(1612485805477L);

    Set<ConstraintViolation<TestTct>> violations = validator.validate(new TestTct(tct));
    assertEquals(1, violations.size());
    ConstraintViolation<TestTct> violation = violations.iterator().next();
    assertEquals("tct", violation.getPropertyPath().toString());
    assertEquals(
        "[TCT definition] is invalid, one and only one time field has to be set: " + tct,
        violation.getMessage());
  }
}
