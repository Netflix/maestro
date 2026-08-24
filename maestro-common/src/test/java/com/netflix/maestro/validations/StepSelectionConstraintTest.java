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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.models.instance.StepSelection;
import com.netflix.maestro.models.instance.StepSelector;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Valid;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class StepSelectionConstraintTest extends BaseConstraintTest {
  private static class TestSelection {
    @Valid @StepSelectionConstraint StepSelection selection;

    TestSelection(StepSelection selection) {
      this.selection = selection;
    }
  }

  private Set<ConstraintViolation<TestSelection>> validate(StepSelection selection) {
    return validator.validate(new TestSelection(selection));
  }

  @Test
  public void testUnsetSelectionAccepted() {
    assertTrue(validate(null).isEmpty());
  }

  @Test
  public void testSelectionWithCriteriaAccepted() {
    assertTrue(
        validate(
                StepSelection.builder()
                    .include(StepSelector.builder().stepIds(Set.of("load_users")).build())
                    .build())
            .isEmpty());
    assertTrue(
        validate(
                StepSelection.builder()
                    .exclude(StepSelector.builder().stepIdPrefixes(Set.of("report_")).build())
                    .build())
            .isEmpty());
  }

  @Test
  public void testSelectionWithNeitherSideCannotBeBuilt() {
    AssertHelper.assertThrows(
        "a selection has to carry a side",
        IllegalArgumentException.class,
        "Step selection must set include or exclude or both",
        () -> StepSelection.builder().build());
  }

  @Test
  public void testEmptyIncludeRejected() {
    Set<ConstraintViolation<TestSelection>> violations =
        validate(StepSelection.builder().include(StepSelector.builder().build()).build());
    assertEquals(1, violations.size());
    assertEquals(
        "[step selection] include must set at least one step id, prefix, infix or suffix",
        violations.iterator().next().getMessage());
  }

  @Test
  public void testNullCriterionRejected() {
    Set<ConstraintViolation<TestSelection>> violations =
        validate(
            StepSelection.builder()
                .include(
                    StepSelector.builder()
                        .stepIdPrefixes(new HashSet<>(Collections.singletonList(null)))
                        .build())
                .build());
    assertBlankCriterion(violations);
  }

  @Test
  public void testBlankCriterionRejected() {
    Set<ConstraintViolation<TestSelection>> violations =
        validate(
            StepSelection.builder()
                .exclude(StepSelector.builder().stepIdInfixes(Set.of(" ")).build())
                .build());
    assertBlankCriterion(violations);
  }

  private static void assertBlankCriterion(Set<ConstraintViolation<TestSelection>> violations) {
    assertFalse(violations.isEmpty());
    violations.forEach(violation -> assertEquals("must not be blank", violation.getMessage()));
  }

  @Test
  public void testEmptyStepIdsListRejected() {
    Set<ConstraintViolation<TestSelection>> violations =
        validate(
            StepSelection.builder()
                .exclude(StepSelector.builder().stepIds(Set.of()).build())
                .build());
    assertEquals(1, violations.size());
    assertEquals(
        "[step selection] exclude must set at least one step id, prefix, infix or suffix",
        violations.iterator().next().getMessage());
  }
}
