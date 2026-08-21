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
package com.netflix.maestro.models.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Set;
import org.junit.Test;

public class StepSelectionTest extends MaestroBaseTest {

  private static StepSelection selection(String includePattern, String excludePattern) {
    return StepSelection.builder()
        .include(
            includePattern == null
                ? null
                : StepSelector.builder().stepIdPattern(includePattern).build())
        .exclude(
            excludePattern == null
                ? null
                : StepSelector.builder().stepIdPattern(excludePattern).build())
        .build();
  }

  @Test
  public void testStepIdsMatchExactly() {
    StepSelection selection =
        StepSelection.builder()
            .include(StepSelector.builder().stepIds(Set.of("load_users", "transform")).build())
            .build();
    assertFalse(selection.isSkipped("load_users"));
    assertFalse(selection.isSkipped("transform"));
    assertTrue(selection.isSkipped("load_orders"));
    assertTrue(selection.isSkipped("load_users_extra"));
  }

  @Test
  public void testStepIdsAndPatternBothApply() {
    StepSelection selection =
        StepSelection.builder()
            .include(
                StepSelector.builder()
                    .stepIds(Set.of("transform"))
                    .stepIdPattern("load_.*")
                    .build())
            .build();
    assertFalse(selection.isSkipped("transform"));
    assertFalse(selection.isSkipped("load_users"));
    assertTrue(selection.isSkipped("publish"));
  }

  @Test
  public void testStepIdsInExcludeAndExcludeStillWins() {
    StepSelection selection =
        StepSelection.builder()
            .include(StepSelector.builder().stepIdPattern("load_.*").build())
            .exclude(StepSelector.builder().stepIds(Set.of("load_expensive")).build())
            .build();
    assertFalse(selection.isSkipped("load_users"));
    assertTrue(selection.isSkipped("load_expensive"));
    assertTrue(selection.isSkipped("publish"));
  }

  @Test
  public void testEmptyStepIdsMatchNothing() {
    StepSelector selector = StepSelector.builder().stepIds(Set.of()).build();
    assertTrue(selector.isEmpty());
    assertFalse(selector.matches("load_users"));
  }

  @Test
  public void testIncludeOnlySkipsEverythingElse() {
    StepSelection selection = selection("load_.*", null);
    assertFalse(selection.isSkipped("load_users"));
    assertTrue(selection.isSkipped("transform"));
  }

  @Test
  public void testExcludeOnlySkipsOnlyMatches() {
    StepSelection selection = selection(null, "load_.*");
    assertTrue(selection.isSkipped("load_users"));
    assertFalse(selection.isSkipped("transform"));
  }

  @Test
  public void testExcludeWinsOverInclude() {
    StepSelection selection = selection("load_.*", "load_expensive");
    assertFalse(selection.isSkipped("load_users"));
    assertTrue(selection.isSkipped("load_expensive"));
    assertTrue(selection.isSkipped("transform"));
  }

  @Test
  public void testEmptySelectionSkipsNothing() {
    assertTrue(StepSelection.builder().build().isEmpty());
    assertFalse(StepSelection.builder().build().isSkipped("load_users"));
    StepSelection blank = selection(null, null);
    assertTrue(blank.isEmpty());
    assertFalse(blank.isSkipped("load_users"));
    StepSelection blankSelectors =
        StepSelection.builder()
            .include(StepSelector.builder().build())
            .exclude(StepSelector.builder().build())
            .build();
    assertTrue(blankSelectors.isEmpty());
    assertFalse(blankSelectors.isSkipped("load_users"));
  }

  @Test
  public void testPatternMatchesTheWholeStepId() {
    StepSelection selection = selection("load", null);
    assertFalse(selection.isSkipped("load"));
    assertTrue(selection.isSkipped("download_users"));
    assertTrue(selection.isSkipped("load_users"));
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    StepSelection selection = selection("load_.*", "load_expensive");
    assertEquals(
        selection, MAPPER.readValue(MAPPER.writeValueAsString(selection), StepSelection.class));
  }

  @Test
  public void testFromJson() throws Exception {
    StepSelection selection =
        MAPPER.readValue(
            """
            {
              "include": {"step_ids": ["transform"], "step_id_pattern": "load_.*"},
              "exclude": {"step_ids": ["load_expensive"]}
            }
            """,
            StepSelection.class);
    assertEquals(Set.of("transform"), selection.getInclude().getStepIds());
    assertEquals("load_.*", selection.getInclude().getStepIdPattern());
    assertEquals(Set.of("load_expensive"), selection.getExclude().getStepIds());
    assertFalse(selection.isEmpty());
  }
}
