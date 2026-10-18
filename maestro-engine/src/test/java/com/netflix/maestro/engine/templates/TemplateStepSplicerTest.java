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
package com.netflix.maestro.engine.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TemplateStepSplicerTest extends MaestroBaseTest {
  private static TypedStep step(String id, Map<String, String> successors) {
    return step(id, List.of(), successors);
  }

  private static TypedStep step(
      String id, List<String> predecessors, Map<String, String> successors) {
    TypedStep step = new TypedStep();
    step.setId(id);
    step.setType(StepType.NOOP);
    StepTransition transition = new StepTransition();
    transition.setSuccessors(new LinkedHashMap<>(successors));
    transition.setPredecessors(new ArrayList<>(predecessors));
    step.setTransition(transition);
    return step;
  }

  /** The prepared steps of a model: write, audit, publish, with a conditional skip. */
  private static List<Step> prepared() {
    return new ArrayList<>(
        List.of(
            step("write", Map.of("audit", "write__has_data", "skip", "!write__has_data")),
            step("audit", Map.of("publish", "true")),
            step("publish", Map.of()),
            step("skip", Map.of())));
  }

  private static Map<String, String> successors(List<Step> steps, String id) {
    return steps.stream()
        .filter(s -> s.getId().equals(id))
        .findFirst()
        .orElseThrow()
        .getTransition()
        .getSuccessors();
  }

  @Test
  public void testSpliceAddsStepBeforeAPreparedStep() {
    List<Step> steps =
        TemplateStepSplicer.splice(prepared(), List.of(step("wait", Map.of("write", "true"))));

    assertEquals(
        List.of("write", "audit", "publish", "skip", "wait"),
        steps.stream().map(Step::getId).toList());
    assertEquals(Map.of("write", "true"), successors(steps, "wait"));
    assertEquals(
        Map.of("audit", "write__has_data", "skip", "!write__has_data"), successors(steps, "write"));
    steps.forEach(step -> assertTrue(step.getTransition().getPredecessors().isEmpty()));
  }

  @Test
  public void testSpliceAddsStepBetweenPreparedSteps() {
    List<Step> steps =
        TemplateStepSplicer.splice(
            prepared(), List.of(step("validate", List.of("audit"), Map.of("publish", "true"))));

    // audit keeps its edge to publish and gains one to validate; publish waits for both
    assertEquals(Map.of("publish", "true", "validate", "true"), successors(steps, "audit"));
    assertEquals(Map.of("publish", "true"), successors(steps, "validate"));
  }

  @Test
  public void testSpliceAddsStepAfterAPreparedStepAsALeaf() {
    List<Step> steps =
        TemplateStepSplicer.splice(
            prepared(), List.of(step("notify", List.of("publish"), Map.of())));

    assertEquals(Map.of("notify", "true"), successors(steps, "publish"));
    assertTrue(successors(steps, "notify").isEmpty());
  }

  @Test
  public void testSpliceChainsSuppliedSteps() {
    List<Step> steps =
        TemplateStepSplicer.splice(
            prepared(),
            List.of(
                step("write_a", Map.of("merge", "true")),
                step("write_b", Map.of("merge", "write_b__rows > 0")),
                step("merge", Map.of("write", "true"))));

    assertEquals(Map.of("merge", "true"), successors(steps, "write_a"));
    assertEquals(Map.of("merge", "write_b__rows > 0"), successors(steps, "write_b"));
    assertEquals(Map.of("write", "true"), successors(steps, "merge"));
  }

  @Test
  public void testSpliceWithoutStepInputs() {
    assertEquals(4, TemplateStepSplicer.splice(prepared(), null).size());
    assertEquals(4, TemplateStepSplicer.splice(prepared(), List.of()).size());
  }

  @Test
  public void testSpliceRejectsIdCollision() {
    AssertHelper.assertThrows(
        "supplied id collides with a prepared step id",
        IllegalArgumentException.class,
        "Template step input [audit] collides with a prepared step id or another step input",
        () ->
            TemplateStepSplicer.splice(
                prepared(), List.of(step("audit", Map.of("publish", "true")))));
  }

  @Test
  public void testSpliceRejectsDuplicateSuppliedId() {
    AssertHelper.assertThrows(
        "supplied steps have duplicate ids",
        IllegalArgumentException.class,
        "Template step input [wait] collides with a prepared step id or another step input",
        () ->
            TemplateStepSplicer.splice(
                prepared(),
                List.of(
                    step("wait", Map.of("write", "true")), step("wait", Map.of("write", "true")))));
  }

  @Test
  public void testSpliceRejectsStepWithoutPosition() {
    AssertHelper.assertThrows(
        "supplied step names neither predecessor nor successor",
        IllegalArgumentException.class,
        "Template step input [loose] names no predecessor or successor",
        () -> TemplateStepSplicer.splice(prepared(), List.of(step("loose", Map.of()))));
  }

  @Test
  public void testSpliceRejectsUnknownSuccessor() {
    AssertHelper.assertThrows(
        "supplied step names an unknown successor",
        IllegalArgumentException.class,
        "Template step input [wait] names successor [nowhere], which is neither a prepared step nor a step input",
        () ->
            TemplateStepSplicer.splice(
                prepared(), List.of(step("wait", Map.of("nowhere", "true")))));
  }

  @Test
  public void testSpliceRejectsUnknownPredecessor() {
    AssertHelper.assertThrows(
        "supplied step names an unknown predecessor",
        NullPointerException.class,
        "Template step input [notify] names predecessor [nowhere], which is neither a prepared step nor a step input",
        () ->
            TemplateStepSplicer.splice(
                prepared(), List.of(step("notify", List.of("nowhere"), Map.of()))));
  }

  @Test
  public void testAppendCollector() {
    List<Step> steps =
        TemplateStepSplicer.splice(
            prepared(), List.of(step("notify", List.of("publish"), Map.of())));
    Map<String, ParamDefinition> outputParams =
        Map.of("row_count", ParamDefinition.buildParamDefinition("row_count", 1L));

    TemplateStepSplicer.appendCollector(steps, outputParams);

    Step collector = steps.getLast();
    assertEquals(Constants.TEMPLATE_OUTPUTS_STEP_ID, collector.getId());
    assertEquals(StepType.NOOP, collector.getType());
    assertEquals(outputParams, collector.getParams());
    assertEquals(Map.of(Constants.TEMPLATE_OUTPUTS_STEP_ID, "true"), successors(steps, "notify"));
    assertEquals(Map.of(Constants.TEMPLATE_OUTPUTS_STEP_ID, "true"), successors(steps, "skip"));
    assertEquals(Map.of("notify", "true"), successors(steps, "publish"));
    assertTrue(collector.getTransition().getSuccessors().isEmpty());
  }

  @Test
  public void testAppendCollectorWithoutOutputParams() {
    List<Step> steps = TemplateStepSplicer.appendCollector(prepared(), null);
    assertTrue(steps.getLast().getParams().isEmpty());
  }

  @Test
  public void testAppendCollectorRejectsReservedId() {
    List<Step> steps = new ArrayList<>(List.of(step(Constants.TEMPLATE_OUTPUTS_STEP_ID, Map.of())));
    AssertHelper.assertThrows(
        "collector id is reserved",
        IllegalArgumentException.class,
        "is reserved for the output collector",
        () -> TemplateStepSplicer.appendCollector(steps, null));
  }
}
