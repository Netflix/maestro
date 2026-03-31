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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.ValidationLimits;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ValidationLimitsTest extends BaseConstraintTest {

  private int savedIdLimit;
  private int savedNameLimit;

  @Before
  public void saveDefaults() {
    savedIdLimit = ValidationLimits.getIdLengthLimit();
    savedNameLimit = ValidationLimits.getNameLengthLimit();
  }

  @After
  public void restoreDefaults() {
    ValidationLimits.initialize(savedIdLimit, savedNameLimit);
  }

  // ---- helper classes for validator tests ----

  private static class TestId {
    @MaestroIdConstraint String id;

    TestId(String id) {
      this.id = id;
    }
  }

  private static class TestName {
    @MaestroNameConstraint String name;

    TestName(String name) {
      this.name = name;
    }
  }

  private static class TestRefId {
    @MaestroReferenceIdConstraint String id;

    TestRefId(String id) {
      this.id = id;
    }
  }

  private static class TestNameSize {
    @MaestroNameSizeConstraint String name;

    TestNameSize(String name) {
      this.name = name;
    }
  }

  // ---- ValidationLimits defaults ----

  @Test
  public void defaultIdLimitMatchesConstants() {
    assertEquals(Constants.ID_LENGTH_LIMIT, ValidationLimits.getIdLengthLimit());
  }

  @Test
  public void defaultNameLimitMatchesConstants() {
    assertEquals(Constants.NAME_LENGTH_LIMIT, ValidationLimits.getNameLengthLimit());
  }

  // ---- ValidationLimits.initialize() ----

  @Test
  public void initializeOverridesLimits() {
    ValidationLimits.initialize(200, 300);
    assertEquals(200, ValidationLimits.getIdLengthLimit());
    assertEquals(300, ValidationLimits.getNameLengthLimit());
  }

  @Test
  public void initializeThrowsOnZeroIdLimit() {
    try {
      ValidationLimits.initialize(0, 256);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("idLengthLimit=0"));
    }
  }

  @Test
  public void initializeThrowsOnNegativeIdLimit() {
    try {
      ValidationLimits.initialize(-1, 256);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("idLengthLimit=-1"));
    }
  }

  @Test
  public void initializeThrowsOnZeroNameLimit() {
    try {
      ValidationLimits.initialize(128, 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("nameLengthLimit=0"));
    }
  }

  @Test
  public void initializeThrowsOnNegativeNameLimit() {
    try {
      ValidationLimits.initialize(128, -5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("nameLengthLimit=-5"));
    }
  }

  // ---- MaestroIdConstraint respects custom limit ----

  @Test
  public void idConstraintAllowsIdAtCustomLimit() {
    ValidationLimits.initialize(200, savedNameLimit);
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId(repeat("a", 200)));
    assertEquals(0, violations.size());
  }

  @Test
  public void idConstraintRejectsIdExceedingCustomLimit() {
    ValidationLimits.initialize(200, savedNameLimit);
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId(repeat("a", 201)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> v = violations.iterator().next();
    assertTrue(v.getMessage().contains("id length limit 200"));
    assertTrue(v.getMessage().contains("rejected length is [201]"));
  }

  @Test
  public void idConstraintRejectsIdThatWasValidUnderDefaultButExceedsCustomLimit() {
    ValidationLimits.initialize(40, savedNameLimit);
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId(repeat("a", 50)));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("id length limit 40"));
  }

  // ---- MaestroNameConstraint respects custom limit ----

  @Test
  public void nameConstraintAllowsNameAtCustomLimit() {
    ValidationLimits.initialize(savedIdLimit, 300);
    Set<ConstraintViolation<TestName>> violations =
        validator.validate(new TestName(repeat("a", 300)));
    assertEquals(0, violations.size());
  }

  @Test
  public void nameConstraintRejectsNameExceedingCustomLimit() {
    ValidationLimits.initialize(savedIdLimit, 300);
    Set<ConstraintViolation<TestName>> violations =
        validator.validate(new TestName(repeat("a", 301)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> v = violations.iterator().next();
    assertTrue(v.getMessage().contains("name length limit 300"));
    assertTrue(v.getMessage().contains("rejected length is [301]"));
  }

  @Test
  public void nameConstraintRejectsNameThatWasValidUnderDefaultButExceedsCustomLimit() {
    ValidationLimits.initialize(savedIdLimit, 50);
    Set<ConstraintViolation<TestName>> violations =
        validator.validate(new TestName(repeat("a", 60)));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("name length limit 50"));
  }

  // ---- MaestroReferenceIdConstraint respects custom limit ----

  @Test
  public void referenceIdConstraintAllowsIdAtCustomLimit() {
    ValidationLimits.initialize(200, savedNameLimit);
    Set<ConstraintViolation<TestRefId>> violations =
        validator.validate(new TestRefId(repeat("a", 200)));
    assertEquals(0, violations.size());
  }

  @Test
  public void referenceIdConstraintRejectsIdExceedingCustomLimit() {
    ValidationLimits.initialize(200, savedNameLimit);
    Set<ConstraintViolation<TestRefId>> violations =
        validator.validate(new TestRefId(repeat("a", 201)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestRefId> v = violations.iterator().next();
    assertTrue(v.getMessage().contains("id length limit 200"));
    assertTrue(v.getMessage().contains("rejected length is [201]"));
  }

  // ---- MaestroNameSizeConstraint respects custom limit ----

  @Test
  public void nameSizeConstraintPassesForNull() {
    ValidationLimits.initialize(savedIdLimit, 50);
    Set<ConstraintViolation<TestNameSize>> violations =
        validator.validate(new TestNameSize(null));
    assertEquals(0, violations.size());
  }

  @Test
  public void nameSizeConstraintAllowsNameAtCustomLimit() {
    ValidationLimits.initialize(savedIdLimit, 100);
    Set<ConstraintViolation<TestNameSize>> violations =
        validator.validate(new TestNameSize(repeat("a", 100)));
    assertEquals(0, violations.size());
  }

  @Test
  public void nameSizeConstraintRejectsNameExceedingCustomLimit() {
    ValidationLimits.initialize(savedIdLimit, 100);
    Set<ConstraintViolation<TestNameSize>> violations =
        validator.validate(new TestNameSize(repeat("a", 101)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestNameSize> v = violations.iterator().next();
    assertTrue(v.getMessage().contains("name length limit 100"));
    assertTrue(v.getMessage().contains("rejected length is [101]"));
  }

  @Test
  public void nameSizeConstraintRejectsNameThatWasValidUnderDefaultButExceedsCustomLimit() {
    ValidationLimits.initialize(savedIdLimit, 50);
    Set<ConstraintViolation<TestNameSize>> violations =
        validator.validate(new TestNameSize(repeat("a", 60)));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("name length limit 50"));
  }

  // ---- helper ----

  private static String repeat(String ch, int times) {
    return new String(new char[times]).replace("\0", ch);
  }
}
