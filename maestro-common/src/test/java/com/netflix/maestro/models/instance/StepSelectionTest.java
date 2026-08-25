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

  private static StepSelection selection(String includePrefix, String excludePrefix) {
    return StepSelection.builder()
        .include(
            includePrefix == null
                ? null
                : StepSelector.builder().stepIdStartsWith(Set.of(includePrefix)).build())
        .exclude(
            excludePrefix == null
                ? null
                : StepSelector.builder().stepIdStartsWith(Set.of(excludePrefix)).build())
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
  public void testStepIdsAndPrefixesBothApply() {
    StepSelection selection =
        StepSelection.builder()
            .include(
                StepSelector.builder()
                    .stepIds(Set.of("transform"))
                    .stepIdStartsWith(Set.of("load_"))
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
            .include(StepSelector.builder().stepIdStartsWith(Set.of("load_")).build())
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
    StepSelection selection = selection("load_", null);
    assertFalse(selection.isSkipped("load_users"));
    assertTrue(selection.isSkipped("transform"));
  }

  @Test
  public void testExcludeOnlySkipsOnlyMatches() {
    StepSelection selection = selection(null, "load_");
    assertTrue(selection.isSkipped("load_users"));
    assertFalse(selection.isSkipped("transform"));
  }

  @Test
  public void testExcludeWinsOverInclude() {
    StepSelection selection = selection("load_", "load_expensive");
    assertFalse(selection.isSkipped("load_users"));
    assertTrue(selection.isSkipped("load_expensive"));
    assertTrue(selection.isSkipped("transform"));
  }

  @Test
  public void testInfixMatchesAnywhereInTheStepId() {
    StepSelection selection =
        StepSelection.builder()
            .include(StepSelector.builder().stepIdContains(Set.of("region")).build())
            .build();
    assertFalse(selection.isSkipped("load_region"));
    assertFalse(selection.isSkipped("region_report"));
    assertFalse(selection.isSkipped("loop_regions"));
    assertTrue(selection.isSkipped("load_users"));
  }

  @Test
  public void testSuffixMatchesTheEndOnly() {
    StepSelection selection =
        StepSelection.builder()
            .exclude(StepSelector.builder().stepIdEndsWith(Set.of("_child")).build())
            .build();
    assertTrue(selection.isSkipped("load_child"));
    assertTrue(selection.isSkipped("fanout_child"));
    assertFalse(selection.isSkipped("child_loader"));
    assertFalse(selection.isSkipped("load_users"));
  }

  @Test
  public void testEachCriterionMatchesOnItsOwnRule() {
    StepSelector selector =
        StepSelector.builder()
            .stepIds(Set.of("exact"))
            .stepIdStartsWith(Set.of("pre_"))
            .stepIdContains(Set.of("_mid_"))
            .stepIdEndsWith(Set.of("_post"))
            .build();
    assertTrue(selector.matches("exact"));
    assertTrue(selector.matches("pre_anything"));
    assertTrue(selector.matches("a_mid_b"));
    assertTrue(selector.matches("anything_post"));
    assertFalse(selector.matches("exact_but_longer"));
    assertFalse(selector.matches("not_pre_at_start"));
    assertFalse(selector.matches("_post_is_not_at_the_end"));
    assertFalse(selector.matches("unrelated"));
  }

  @Test
  public void testPrefixMatchesFromTheStartOnly() {
    StepSelection selection = selection("load", null);
    assertFalse(selection.isSkipped("load"));
    assertTrue(selection.isSkipped("download_users"));
    assertFalse(selection.isSkipped("load_users"));
  }

  @Test
  public void testSelectorDescribesOnlyTheCriteriaItCarries() {
    assertEquals(
        "ids [load_expensive]",
        StepSelector.builder().stepIds(Set.of("load_expensive")).build().describe());
    assertEquals(
        "starts_with [load_]",
        StepSelector.builder().stepIdStartsWith(Set.of("load_")).build().describe());
    assertEquals(
        "contains [region]",
        StepSelector.builder().stepIdContains(Set.of("region")).build().describe());
    assertEquals(
        "ends_with [_child]",
        StepSelector.builder().stepIdEndsWith(Set.of("_child")).build().describe());
  }

  @Test
  public void testSelectorDescribesSeveralCriteriaInFixedOrder() {
    StepSelector selector =
        StepSelector.builder()
            .stepIds(Set.of("b", "a"))
            .stepIdStartsWith(Set.of("load_"))
            .stepIdContains(Set.of("region"))
            .stepIdEndsWith(Set.of("_child"))
            .build();
    assertEquals(
        "ids [a, b], starts_with [load_], contains [region], ends_with [_child]",
        selector.describe());
  }

  @Test
  public void testSelectionDescribesWhatItDoes() {
    assertEquals(
        "includes only steps matching starts_with [load_]", selection("load_", null).describe());
    assertEquals(
        "excludes steps matching starts_with [report_]", selection(null, "report_").describe());
    assertEquals(
        "includes only steps matching starts_with [load_], and excludes steps matching starts_with"
            + " [load_expensive]",
        selection("load_", "load_expensive").describe());
  }

  @Test
  public void testDescriptionsNeverRenderNull() {
    for (String rendered :
        new String[] {
          StepSelector.builder().stepIds(Set.of("a")).build().describe(),
          selection("load_", null).describe(),
          selection(null, "report_").describe(),
          selection("load_", "report_").describe()
        }) {
      assertFalse("rendered null in [" + rendered + "]", rendered.contains("null"));
      assertFalse("leaked a class name in [" + rendered + "]", rendered.contains("StepSelect"));
    }
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    StepSelection selection = selection("load_", "load_expensive");
    assertEquals(
        selection, MAPPER.readValue(MAPPER.writeValueAsString(selection), StepSelection.class));
  }

  @Test
  public void testFromJson() throws Exception {
    StepSelection selection =
        MAPPER.readValue(
            """
            {
              "include": {
                "step_ids": ["transform"],
                "step_id_starts_with": ["load_"],
                "step_id_contains": ["region"],
                "step_id_ends_with": ["_child"]
              },
              "exclude": {"step_ids": ["load_expensive"]}
            }
            """,
            StepSelection.class);
    assertEquals(Set.of("transform"), selection.getInclude().getStepIds());
    assertEquals(Set.of("load_"), selection.getInclude().getStepIdStartsWith());
    assertEquals(Set.of("region"), selection.getInclude().getStepIdContains());
    assertEquals(Set.of("_child"), selection.getInclude().getStepIdEndsWith());
    assertEquals(Set.of("load_expensive"), selection.getExclude().getStepIds());
  }
}
