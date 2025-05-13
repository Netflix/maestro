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
import jakarta.validation.ConstraintViolation;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.Test;

public class MaestroNameConstraintTest extends BaseConstraintTest {

  private static class TestName {
    @MaestroNameConstraint String name;

    TestName(String name) {
      this.name = name;
    }
  }

  @Test
  public void isValidName() {
    Set<ConstraintViolation<TestName>> violations =
        validator.validate(new TestName("123__foo_.-bar1@nflx:com/ _.,-_"));
    assertEquals(0, violations.size());
    violations = validator.validate(new TestName("__foo_.-bar 1_.-_ ,"));
    assertEquals(0, violations.size());
    violations = validator.validate(new TestName("foo_.-bar1__.-_:/@.com "));
    assertEquals(0, violations.size());

    String[] validIds =
        new String[] {
          "-foo", ".foo", "$foo", "foo$bar", "foo{}", "foo()", " foo", ":foo", "@foo", ",foo",
          "/foo", "foo[]", "foo,bar", "foo&bar", "'foo", "foo_bar", "foo=bar", "foo&bar"
        };
    for (String validId : validIds) {
      violations.addAll(validator.validate(new TestName(validId)));
    }
    assertEquals(0, violations.size());
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestName>> violations = validator.validate(new TestName(null));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> violation = violations.iterator().next();
    assertNull(violation.getInvalidValue());
    assertEquals("[maestro name] cannot be null or empty or blank spaces", violation.getMessage());
  }

  @Test
  public void isEmpty() {
    Set<ConstraintViolation<TestName>> violations = validator.validate(new TestName(""));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> violation = violations.iterator().next();
    assertEquals("", violation.getInvalidValue());
    assertEquals("[maestro name] cannot be null or empty or blank spaces", violation.getMessage());
  }

  @Test
  public void isEmptySpace() {
    Set<ConstraintViolation<TestName>> violations = validator.validate(new TestName("     "));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> violation = violations.iterator().next();
    assertEquals("     ", violation.getInvalidValue());
    assertEquals("[maestro name] cannot be null or empty or blank spaces", violation.getMessage());
  }

  @Test
  public void isNameTooLong() {
    Set<ConstraintViolation<TestName>> violations =
        validator.validate(
            new TestName(new String(new char[Constants.NAME_LENGTH_LIMIT + 1]).replace("\0", "a")));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> violation = violations.iterator().next();
    assertEquals(257, ((String) violation.getInvalidValue()).length());
    assertEquals(
        String.format(
            "[maestro name] cannot be more than name length limit 256 "
                + "- rejected length is [%s] for value [%s]",
            257, new String(new char[Constants.NAME_LENGTH_LIMIT + 1]).replace("\0", "a")),
        violation.getMessage());
  }

  @Test
  public void isNameUsingValidChars() {
    Set<ConstraintViolation<TestName>> violations = new LinkedHashSet<>();
    String[] invalidIds = new String[] {"~foo", "`foo"};
    for (String invalidId : invalidIds) {
      violations.addAll(validator.validate(new TestName(invalidId)));
    }
    assertEquals(invalidIds.length, violations.size());
    int idx = 0;
    for (ConstraintViolation<TestName> violation : violations) {
      assertEquals(invalidIds[idx++], violation.getInvalidValue());
      assertEquals(
          "[maestro name] does not follow the regex rule: [\\w \\.,:@&='(){}$+\\[\\]\\-\\/\\]+ "
              + "- rejected value is ["
              + violation.getInvalidValue()
              + "]",
          violation.getMessage());
    }
  }

  @Test
  public void isNameUsingReservedPrefix() {
    Set<ConstraintViolation<TestName>> violations = validator.validate(new TestName("maestro_foo"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> violation = violations.iterator().next();
    assertEquals("maestro_foo", violation.getInvalidValue());
    assertEquals(
        "[maestro name] cannot start with reserved prefix: maestro_ - rejected value is [maestro_foo]",
        violation.getMessage());
  }

  @Test
  public void isNameUsingReservedSuffix() {
    Set<ConstraintViolation<TestName>> violations = validator.validate(new TestName("foo_maestro"));
    assertEquals(1, violations.size());
    ConstraintViolation<TestName> violation = violations.iterator().next();
    assertEquals("foo_maestro", violation.getInvalidValue());
    assertEquals(
        "[maestro name] cannot end with reserved suffix: _maestro - rejected value is [foo_maestro]",
        violation.getMessage());
  }
}
