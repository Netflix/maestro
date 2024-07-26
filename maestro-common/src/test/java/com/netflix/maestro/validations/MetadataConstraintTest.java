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
import static org.junit.Assert.assertNull;

import com.netflix.maestro.models.definition.Metadata;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Test;

public class MetadataConstraintTest extends BaseConstraintTest {
  private static class TestMetadata {
    @MetadataConstraint Metadata metadata;

    TestMetadata(Metadata metadata) {
      this.metadata = metadata;
    }
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestMetadata>> violations = validator.validate(new TestMetadata(null));
    assertEquals(1, violations.size());
    ConstraintViolation<TestMetadata> violation = violations.iterator().next();
    assertNull(violation.getInvalidValue());
    assertEquals("[workflow metadata] cannot be null", violation.getMessage());
  }
}
