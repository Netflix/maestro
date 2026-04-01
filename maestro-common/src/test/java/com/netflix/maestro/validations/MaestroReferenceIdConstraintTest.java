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

import com.netflix.maestro.models.Constants;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorFactory;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.junit.Test;

public class MaestroReferenceIdConstraintTest extends BaseConstraintTest {

  private static class TestId {
    @MaestroReferenceIdConstraint String id;

    TestId(String id) {
      this.id = id;
    }
  }

  @Test
  public void isValidId() {
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId("_foo_.-bar1_.-_"));
    assertEquals(0, violations.size());
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId(null));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> violation = violations.iterator().next();
    assertNull(violation.getInvalidValue());
    assertEquals("[maestro id or name reference] cannot be null or empty", violation.getMessage());
  }

  @Test
  public void isEmpty() {
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId(""));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> violation = violations.iterator().next();
    assertEquals("", violation.getInvalidValue());
    assertEquals("[maestro id or name reference] cannot be null or empty", violation.getMessage());
  }

  @Test
  public void isIdTooLong() {
    Set<ConstraintViolation<TestId>> violations =
        validator.validate(
            new TestId(new String(new char[Constants.ID_LENGTH_LIMIT + 1]).replace("\0", "a")));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> violation = violations.iterator().next();
    assertEquals(129, ((String) violation.getInvalidValue()).length());
    assertEquals(
        String.format(
            "[maestro id or name reference] cannot be more than id length limit 128 "
                + "- rejected length is [%s] for value [%s]",
            129, new String(new char[Constants.ID_LENGTH_LIMIT + 1]).replace("\0", "a")),
        violation.getMessage());
  }

  @Test
  public void isIdUsingValidChars() {
    Set<ConstraintViolation<TestId>> violations = new LinkedHashSet<>();
    String[] invalidIds =
        new String[] {"0foo", "-foo", ".foo", "$foo", "foo$bar", "foo{}", "foo()"};
    for (String invalidId : invalidIds) {
      violations.addAll(validator.validate(new TestId(invalidId)));
    }
    assertEquals(invalidIds.length, violations.size());
    int idx = 0;
    for (ConstraintViolation<TestId> violation : violations) {
      assertEquals(invalidIds[idx++], violation.getInvalidValue());
      assertEquals(
          "[maestro id or name reference] does not follow the regex rule: [_a-zA-Z][.\\-_a-zA-Z0-9]*+ "
              + "- rejected value is ["
              + violation.getInvalidValue()
              + "]",
          violation.getMessage());
    }
  }

  @Test
  public void isIdUsingDoubleUnderscores() {
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId("foo__bar"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> violation = violations.iterator().next();
    assertEquals("foo__bar", violation.getInvalidValue());
    assertEquals(
        "[maestro id or name reference] cannot contain the step param separator '__' - rejected value is [foo__bar]",
        violation.getMessage());
  }

  @Test
  public void isIdValidWithCustomSeparator() {
    Set<ConstraintViolation<TestId>> violations =
        validatorWithSeparator("___").validate(new TestId("foo__bar"));
    assertEquals(0, violations.size());
  }

  @Test
  public void isIdUsingCustomSeparator() {
    Set<ConstraintViolation<TestId>> violations =
        validatorWithSeparator("___").validate(new TestId("foo___bar"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> violation = violations.iterator().next();
    assertEquals("foo___bar", violation.getInvalidValue());
    assertEquals(
        "[maestro id or name reference] cannot contain the step param separator '___' - rejected value is [foo___bar]",
        violation.getMessage());
  }

  private Validator validatorWithSeparator(String separator) {
    return Validation.byProvider(ApacheValidationProvider.class)
        .configure()
        .constraintValidatorFactory(
            new ConstraintValidatorFactory() {
              @Override
              public <T extends ConstraintValidator<?, ?>> T getInstance(Class<T> key) {
                try {
                  T instance = key.getDeclaredConstructor().newInstance();
                  if (instance instanceof MaestroReferenceIdConstraint.MaestroIdValidator) {
                    ((MaestroReferenceIdConstraint.MaestroIdValidator) instance)
                        .setStepParamSeparator(() -> separator);
                  }
                  return instance;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void releaseInstance(ConstraintValidator<?, ?> instance) {}
            })
        .buildValidatorFactory()
        .getValidator();
  }

  @Test
  public void isIdUsingReservedPrefix() {
    Set<ConstraintViolation<TestId>> violations = validator.validate(new TestId("maestro_foo"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestId> violation = violations.iterator().next();
    assertEquals("maestro_foo", violation.getInvalidValue());
    assertEquals(
        "[maestro id or name reference] cannot start with reserved prefix: maestro_ - rejected value is [maestro_foo]",
        violation.getMessage());
  }
}
