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

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.utils.MaestroIdNameValidationLimits;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorFactory;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.lang.reflect.Field;
import java.util.Set;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.junit.Test;

public class MaestroIdNameValidationLimitsTest extends MaestroBaseTest {

  // ---- helper: create a Validator that injects a custom MaestroIdNameValidationLimits ----

  private static Validator buildValidatorWith(MaestroIdNameValidationLimits limits) {
    return Validation.byProvider(ApacheValidationProvider.class)
        .configure()
        .constraintValidatorFactory(
            new ConstraintValidatorFactory() {
              @Override
              public <T extends ConstraintValidator<?, ?>> T getInstance(Class<T> key) {
                try {
                  T instance = key.getDeclaredConstructor().newInstance();
                  for (Field field : key.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Inject.class)
                        && field.getType().equals(MaestroIdNameValidationLimits.class)) {
                      field.setAccessible(true);
                      field.set(instance, limits);
                    }
                  }
                  return instance;
                } catch (ReflectiveOperationException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void releaseInstance(ConstraintValidator<?, ?> instance) {}
            })
        .buildValidatorFactory()
        .getValidator();
  }

  private static MaestroIdNameValidationLimits limitsOf(int idLimit, int nameLimit) {
    return new MaestroIdNameValidationLimits() {
      @Override
      public int getIdLengthLimit() {
        return idLimit;
      }

      @Override
      public int getNameLengthLimit() {
        return nameLimit;
      }
    };
  }

  // ---- helper classes ----

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

  // ---- MaestroIdConstraint respects custom limit ----

  @Test
  public void idConstraintAllowsIdAtCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(200, Constants.NAME_LENGTH_LIMIT));
    assertEquals(0, v.validate(new TestId(repeat("a", 200))).size());
  }

  @Test
  public void idConstraintRejectsIdExceedingCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(200, Constants.NAME_LENGTH_LIMIT));
    Set<ConstraintViolation<TestId>> violations = v.validate(new TestId(repeat("a", 201)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> cv = violations.iterator().next();
    assertTrue(cv.getMessage().contains("id length limit 200"));
    assertTrue(cv.getMessage().contains("rejected length is [201]"));
  }

  @Test
  public void idConstraintRejectsIdThatWasValidUnderDefaultButExceedsCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(40, Constants.NAME_LENGTH_LIMIT));
    Set<ConstraintViolation<TestId>> violations = v.validate(new TestId(repeat("a", 50)));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("id length limit 40"));
  }

  // ---- MaestroNameConstraint respects custom limit ----

  @Test
  public void nameConstraintAllowsNameAtCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 300));
    assertEquals(0, v.validate(new TestName(repeat("a", 300))).size());
  }

  @Test
  public void nameConstraintRejectsNameExceedingCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 300));
    Set<ConstraintViolation<TestName>> violations = v.validate(new TestName(repeat("a", 301)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> cv = violations.iterator().next();
    assertTrue(cv.getMessage().contains("name length limit 300"));
    assertTrue(cv.getMessage().contains("rejected length is [301]"));
  }

  @Test
  public void nameConstraintRejectsNameThatWasValidUnderDefaultButExceedsCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 50));
    Set<ConstraintViolation<TestName>> violations = v.validate(new TestName(repeat("a", 60)));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("name length limit 50"));
  }

  // ---- MaestroReferenceIdConstraint respects custom limit ----

  @Test
  public void referenceIdConstraintAllowsIdAtCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(200, Constants.NAME_LENGTH_LIMIT));
    assertEquals(0, v.validate(new TestRefId(repeat("a", 200))).size());
  }

  @Test
  public void referenceIdConstraintRejectsIdExceedingCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(200, Constants.NAME_LENGTH_LIMIT));
    Set<ConstraintViolation<TestRefId>> violations = v.validate(new TestRefId(repeat("a", 201)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestRefId> cv = violations.iterator().next();
    assertTrue(cv.getMessage().contains("id length limit 200"));
    assertTrue(cv.getMessage().contains("rejected length is [201]"));
  }

  // ---- MaestroNameSizeConstraint respects custom limit ----

  @Test
  public void nameSizeConstraintPassesForNull() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 50));
    assertEquals(0, v.validate(new TestNameSize(null)).size());
  }

  @Test
  public void nameSizeConstraintAllowsNameAtCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 100));
    assertEquals(0, v.validate(new TestNameSize(repeat("a", 100))).size());
  }

  @Test
  public void nameSizeConstraintRejectsNameExceedingCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 100));
    Set<ConstraintViolation<TestNameSize>> violations =
        v.validate(new TestNameSize(repeat("a", 101)));
    assertEquals(1, violations.size());
    ConstraintViolation<TestNameSize> cv = violations.iterator().next();
    assertTrue(cv.getMessage().contains("name length limit 100"));
    assertTrue(cv.getMessage().contains("rejected length is [101]"));
  }

  @Test
  public void nameSizeConstraintRejectsNameThatWasValidUnderDefaultButExceedsCustomLimit() {
    Validator v = buildValidatorWith(limitsOf(Constants.ID_LENGTH_LIMIT, 50));
    Set<ConstraintViolation<TestNameSize>> violations =
        v.validate(new TestNameSize(repeat("a", 60)));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("name length limit 50"));
  }

  // ---- helper ----

  private static String repeat(String ch, int times) {
    return new String(new char[times]).replace("\0", ch);
  }
}
