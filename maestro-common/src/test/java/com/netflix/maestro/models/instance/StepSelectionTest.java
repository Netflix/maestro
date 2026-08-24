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
                : StepSelector.builder().stepIdPrefixes(Set.of(includePrefix)).build())
        .exclude(
            excludePrefix == null
                ? null
                : StepSelector.builder().stepIdPrefixes(Set.of(excludePrefix)).build())
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
                    .stepIdPrefixes(Set.of("load_"))
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
            .include(StepSelector.builder().stepIdPrefixes(Set.of("load_")).build())
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
        StepSelector.builder().stepIds(Set.of("load_expensive")).build().toString());
    assertEquals(
        "prefixes [load_]",
        StepSelector.builder().stepIdPrefixes(Set.of("load_")).build().toString());
    assertEquals(
        "infixes [region]",
        StepSelector.builder().stepIdInfixes(Set.of("region")).build().toString());
    assertEquals(
        "postfixes [_child]",
        StepSelector.builder().stepIdPostfixes(Set.of("_child")).build().toString());
  }

  @Test
  public void testSelectorDescribesSeveralCriteriaInFixedOrder() {
    StepSelector selector =
        StepSelector.builder()
            .stepIds(Set.of("b", "a"))
            .stepIdPrefixes(Set.of("load_"))
            .stepIdInfixes(Set.of("region"))
            .stepIdPostfixes(Set.of("_child"))
            .build();
    assertEquals(
        "ids [a, b], prefixes [load_], infixes [region], postfixes [_child]", selector.toString());
  }

  @Test
  public void testSelectionDescribesWhatItDoes() {
    assertEquals(
        "includes only steps matching prefixes [load_]", selection("load_", null).toString());
    assertEquals(
        "excludes steps matching prefixes [report_]", selection(null, "report_").toString());
    assertEquals(
        "includes only steps matching prefixes [load_], and excludes steps matching prefixes"
            + " [load_expensive]",
        selection("load_", "load_expensive").toString());
    assertEquals("selects every step", StepSelection.builder().build().toString());
  }

  @Test
  public void testDescriptionsNeverRenderNull() {
    for (String rendered :
        new String[] {
          StepSelector.builder().stepIds(Set.of("a")).build().toString(),
          selection("load_", null).toString(),
          selection(null, "report_").toString(),
          selection("load_", "report_").toString(),
          StepSelection.builder().build().toString()
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
                "step_id_prefixes": ["load_"],
                "step_id_infixes": ["region"],
                "step_id_postfixes": ["_child"]
              },
              "exclude": {"step_ids": ["load_expensive"]}
            }
            """,
            StepSelection.class);
    assertEquals(Set.of("transform"), selection.getInclude().getStepIds());
    assertEquals(Set.of("load_"), selection.getInclude().getStepIdPrefixes());
    assertEquals(Set.of("region"), selection.getInclude().getStepIdInfixes());
    assertEquals(Set.of("_child"), selection.getInclude().getStepIdPostfixes());
    assertEquals(Set.of("load_expensive"), selection.getExclude().getStepIds());
    assertFalse(selection.isEmpty());
  }
}
