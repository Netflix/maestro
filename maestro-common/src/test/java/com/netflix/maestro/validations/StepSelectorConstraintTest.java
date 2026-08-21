/*
 * Copyright 2026 Netflix, Inc.
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

import com.netflix.maestro.models.instance.StepSelector;
import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.junit.Test;

public class StepSelectorConstraintTest extends BaseConstraintTest {
  private static class TestSelector {
    @StepSelectorConstraint StepSelector selector;

    TestSelector(StepSelector selector) {
      this.selector = selector;
    }
  }

  private Set<ConstraintViolation<TestSelector>> validatePattern(String pattern) {
    return validator.validate(
        new TestSelector(StepSelector.builder().stepIdPattern(pattern).build()));
  }

  @Test
  public void testValidPatternsAccepted() {
    for (String pattern :
        new String[] {
          "load_.*",
          "load_users|load_orders|transform",
          "[a-z_]+",
          "load_(users|orders)",
          "(load|save)_.*",
          "(load_)?users",
          "step_\\d{1,3}",
          "\\(literal\\)+"
        }) {
      assertTrue("expected [" + pattern + "] to be accepted", validatePattern(pattern).isEmpty());
    }
  }

  @Test
  public void testSelectorWithoutPatternAccepted() {
    assertTrue(
        validator
            .validate(new TestSelector(StepSelector.builder().stepIds(Set.of("load")).build()))
            .isEmpty());
    assertTrue(validator.validate(new TestSelector(null)).isEmpty());
  }

  @Test
  public void testRepeatedGroupRejected() {
    for (String pattern :
        new String[] {
          "(a+)+", // nested quantifier
          "(a*)*",
          "(a+)*",
          "(.*)+",
          "(a{2,})+",
          "(a|a)+", // overlapping alternation
          "(a|ab)+",
          "(x|xy)*",
          "((a+))+", // nested deeper than one group
          "(?:(?:a)+)+",
          "((a)+)+",
          "(a?b?)+", // optional elements under a quantifier
          "(\\d*\\w*)+",
          "(ab)+", // repeated literal sequence
          "(load_.*)+"
        }) {
      Set<ConstraintViolation<TestSelector>> violations = validatePattern(pattern);
      assertEquals("expected [" + pattern + "] to be rejected", 1, violations.size());
      assertTrue(violations.iterator().next().getMessage().contains("repeats a group"));
    }
  }

  @Test
  public void testInvalidRegexRejected() {
    Set<ConstraintViolation<TestSelector>> violations = validatePattern("load_(");
    assertEquals(1, violations.size());
    assertTrue(
        violations.iterator().next().getMessage().contains("is not a valid regular expression"));
  }

  @Test
  public void testOverlyLongPatternRejected() {
    Set<ConstraintViolation<TestSelector>> violations = validatePattern("a".repeat(513));
    assertEquals(1, violations.size());
    assertTrue(violations.iterator().next().getMessage().contains("is over the limit"));
  }
}
