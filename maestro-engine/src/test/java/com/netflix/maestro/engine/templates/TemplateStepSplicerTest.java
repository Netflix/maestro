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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplateStepInput;
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

  /** A write job as an author attaches it: publishes row_count. */
  private static TypedStep write(String id, Map<String, String> successors) {
    TypedStep step = step(id, successors);
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    params.put(
        "row_count",
        LongParamDefinition.builder().name("row_count").value(0L).mode(ParamMode.MUTABLE).build());
    step.setParams(params);
    return step;
  }

  /** The prepared steps of the template: audit -> publish, plus a conditional skip off audit. */
  private static List<Step> prepared() {
    return new ArrayList<>(
        List.of(
            step("audit", Map.of("publish", "audit__has_data", "skip", "!audit__has_data")),
            step("publish", Map.of()),
            step("skip", Map.of())));
  }

  private static TemplateStepInput position(
      List<String> predecessors,
      List<String> successors,
      List<String> params,
      Map<String, ParamType> outputs) {
    TemplateStepInput.TemplateStepInputBuilder position =
        TemplateStepInput.builder()
            .predecessors(predecessors)
            .successors(successors)
            .params(params);
    if (outputs != null) {
      Map<String, TemplateStepInput.Output> declared = new LinkedHashMap<>();
      outputs.forEach(
          (name, type) ->
              declared.put(name, TemplateStepInput.Output.builder().type(type).build()));
      position.outputs(declared);
    }
    return position.build();
  }

  /**
   * The wap template: input table, promised snapshot, a writes position collecting row_count before
   * audit and an after_publish position exposing the promised snapshot.
   */
  private static JobTemplate.Definition definition() {
    JobTemplate.Definition definition = new JobTemplate.Definition();
    definition.setJobType("wap");
    definition.setStepType(StepType.TEMPLATE);
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    params.put(
        "table", StringParamDefinition.builder().name("table").value("db.sample_table").build());
    params.put(
        "snapshot",
        StringParamDefinition.builder()
            .name("snapshot")
            .expression("publish__snapshot;")
            .mode(ParamMode.IMMUTABLE)
            .build());
    definition.setParams(params);
    Map<String, TemplateStepInput> positions = new LinkedHashMap<>();
    positions.put(
        "writes",
        position(null, List.of("audit"), List.of("table"), Map.of("row_count", ParamType.LONG)));
    positions.put(
        "after_publish", position(List.of("publish"), null, List.of("table", "snapshot"), null));
    positions.put("between", position(List.of("audit"), List.of("publish"), null, null));
    definition.setStepInputs(positions);
    return definition;
  }

  private static Step find(List<Step> steps, String id) {
    return steps.stream().filter(s -> s.getId().equals(id)).findFirst().orElseThrow();
  }

  private static Map<String, String> successors(List<Step> steps, String id) {
    return find(steps, id).getTransition().getSuccessors();
  }

  @Test
  public void testSpliceAttachesParallelWritesThroughTheJoinStep() {
    Map<String, List<Step>> inputs =
        Map.of("writes", List.of(write("write_a", Map.of()), write("write_b", Map.of())));

    List<Step> steps = TemplateStepSplicer.splice(prepared(), definition(), inputs);

    assertEquals(
        List.of("audit", "publish", "skip", "write_a", "write_b", "writes"),
        steps.stream().map(Step::getId).toList());
    // both writes are roots and leaves: nothing before them, the join step after them
    assertEquals(Map.of("writes", "true"), successors(steps, "write_a"));
    assertEquals(Map.of("writes", "true"), successors(steps, "write_b"));
    assertEquals(Map.of("audit", "true"), successors(steps, "writes"));
    Step join = find(steps, "writes");
    assertEquals(StepType.NOOP, join.getType());
    ParamDefinition collected = join.getParams().get("row_count");
    assertEquals(ParamType.LONG_ARRAY, collected.getType());
    assertEquals(ParamMode.IMMUTABLE, collected.getMode());
    assertEquals(
        "return new long[]{write_a__row_count, write_b__row_count};", collected.getExpression());
    // the prepared edges are untouched
    assertEquals(
        Map.of("publish", "audit__has_data", "skip", "!audit__has_data"),
        successors(steps, "audit"));
    steps.forEach(step -> assertTrue(step.getTransition().getPredecessors().isEmpty()));
    // table is an input and resolves as an instance param, so nothing is added for it
    assertEquals(List.of("row_count"), List.copyOf(find(steps, "write_a").getParams().keySet()));
  }

  @Test
  public void testSpliceWiresTheDagOfAPosition() {
    Map<String, List<Step>> inputs =
        Map.of(
            "writes",
            List.of(
                write("write_a", Map.of("merge", "true")),
                write("write_b", Map.of("merge", "write_b__row_count > 0")),
                write("merge", Map.of())));

    List<Step> steps = TemplateStepSplicer.splice(prepared(), definition(), inputs);

    assertEquals(Map.of("merge", "true"), successors(steps, "write_a"));
    assertEquals(Map.of("merge", "write_b__row_count > 0"), successors(steps, "write_b"));
    assertEquals(Map.of("writes", "true"), successors(steps, "merge"));
    assertEquals(
        "return new long[]{write_a__row_count, write_b__row_count, merge__row_count};",
        find(steps, "writes").getParams().get("row_count").getExpression());
  }

  @Test
  public void testSpliceAttachesAfterAPredecessorAndExposesPromisedParams() {
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    params.put(
        "msg",
        StringParamDefinition.builder().name("msg").expression("'published ' + snapshot;").build());
    TypedStep announce = step("announce", Map.of());
    announce.setParams(params);
    Map<String, List<Step>> inputs = Map.of("after_publish", List.of(announce));

    List<Step> steps = TemplateStepSplicer.splice(prepared(), definition(), inputs);

    assertEquals(
        List.of("audit", "publish", "skip", "announce"), steps.stream().map(Step::getId).toList());
    assertEquals(Map.of("announce", "true"), successors(steps, "publish"));
    assertTrue(successors(steps, "announce").isEmpty());
    Map<String, ParamDefinition> attached = find(steps, "announce").getParams();
    assertEquals(List.of("msg", "snapshot"), List.copyOf(attached.keySet()));
    ParamDefinition snapshot = attached.get("snapshot");
    assertEquals("publish__snapshot;", snapshot.getExpression());
    assertEquals(ParamMode.IMMUTABLE, snapshot.getMode());
    assertEquals(ParamSource.TEMPLATE_OUTPUT, snapshot.getSource());
    assertNull(snapshot.getValue());
  }

  @Test
  public void testSpliceAttachesBetweenPreparedSteps() {
    Map<String, List<Step>> inputs = Map.of("between", List.of(step("validate", Map.of())));

    List<Step> steps = TemplateStepSplicer.splice(prepared(), definition(), inputs);

    // audit keeps its conditional edges and gains one to validate; publish waits for both
    assertEquals(
        Map.of("publish", "audit__has_data", "skip", "!audit__has_data", "validate", "true"),
        successors(steps, "audit"));
    assertEquals(Map.of("publish", "true"), successors(steps, "validate"));
  }

  @Test
  public void testSpliceWithPredecessorInsideThePosition() {
    Map<String, List<Step>> inputs =
        Map.of(
            "between",
            List.of(step("check", Map.of()), step("validate", List.of("check"), Map.of())));

    List<Step> steps = TemplateStepSplicer.splice(prepared(), definition(), inputs);

    assertEquals(Map.of("validate", "true"), successors(steps, "check"));
    assertEquals(Map.of("publish", "true"), successors(steps, "validate"));
    assertTrue(successors(steps, "audit").containsKey("check"));
    assertFalse(successors(steps, "audit").containsKey("validate"));
  }

  @Test
  public void testSpliceWithoutStepInputs() {
    assertEquals(3, TemplateStepSplicer.splice(prepared(), definition(), null).size());
    assertEquals(3, TemplateStepSplicer.splice(prepared(), definition(), Map.of()).size());
  }

  @Test
  public void testSpliceRejectsUndeclaredPosition() {
    AssertHelper.assertThrows(
        "position is not declared by the template",
        IllegalArgumentException.class,
        "Template step input position [nowhere] is not declared by the template",
        () ->
            TemplateStepSplicer.splice(
                prepared(), definition(), Map.of("nowhere", List.of(step("x", Map.of())))));
  }

  @Test
  public void testSpliceRejectsIdCollision() {
    AssertHelper.assertThrows(
        "supplied id collides with a prepared step id",
        IllegalArgumentException.class,
        "Template step input [audit] collides with a prepared step id, a position name or another step input",
        () ->
            TemplateStepSplicer.splice(
                prepared(), definition(), Map.of("writes", List.of(write("audit", Map.of())))));
    AssertHelper.assertThrows(
        "supplied id collides with a position name",
        IllegalArgumentException.class,
        "Template step input [writes] collides with a prepared step id, a position name or another step input",
        () ->
            TemplateStepSplicer.splice(
                prepared(), definition(), Map.of("writes", List.of(write("writes", Map.of())))));
  }

  @Test
  public void testSpliceRejectsTransitionOutsideThePosition() {
    AssertHelper.assertThrows(
        "supplied step names a successor outside its position",
        IllegalArgumentException.class,
        "Template step input [write_a] at position [writes] names successor [audit], which is not a step of the same position",
        () ->
            TemplateStepSplicer.splice(
                prepared(),
                definition(),
                Map.of("writes", List.of(write("write_a", Map.of("audit", "true"))))));
    AssertHelper.assertThrows(
        "supplied step names a predecessor outside its position",
        NullPointerException.class,
        "Template step input [validate] at position [between] names predecessor [audit], which is not a step of the same position",
        () ->
            TemplateStepSplicer.splice(
                prepared(),
                definition(),
                Map.of("between", List.of(step("validate", List.of("audit"), Map.of())))));
  }

  @Test
  public void testSpliceRejectsMissingOutputParam() {
    AssertHelper.assertThrows(
        "supplied step does not publish the declared output",
        IllegalArgumentException.class,
        "Template step input [write_a] at position [writes] must declare the output param [row_count] of type [LONG]",
        () ->
            TemplateStepSplicer.splice(
                prepared(), definition(), Map.of("writes", List.of(step("write_a", Map.of())))));
  }

  @Test
  public void testSpliceRejectsSuppliedParamNamedAfterAPromisedParam() {
    TypedStep announce = step("announce", Map.of());
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    params.put("snapshot", StringParamDefinition.builder().name("snapshot").value("mine").build());
    announce.setParams(params);
    AssertHelper.assertThrows(
        "supplied step defines a promised param",
        IllegalArgumentException.class,
        "Template step input [announce] defines [snapshot], which is a param the template provides",
        () ->
            TemplateStepSplicer.splice(
                prepared(), definition(), Map.of("after_publish", List.of(announce))));
  }

  @Test
  public void testSpliceRejectsPositionNamingUnknownPreparedStep() {
    JobTemplate.Definition definition = definition();
    definition.getStepInputs().put("late", position(List.of("archive"), null, null, null));
    AssertHelper.assertThrows(
        "position names a step the expansion does not have",
        NullPointerException.class,
        "Template step input position [late] names predecessor [archive], which is not a prepared step",
        () ->
            TemplateStepSplicer.splice(
                prepared(), definition, Map.of("late", List.of(step("x", Map.of())))));
  }

  @Test
  public void testAppendCollector() {
    List<Step> steps =
        TemplateStepSplicer.splice(
            prepared(), definition(), Map.of("after_publish", List.of(step("announce", Map.of()))));

    TemplateStepSplicer.appendCollector(steps, definition().promisedParams());

    Step collector = steps.getLast();
    assertEquals(Constants.TEMPLATE_OUTPUTS_STEP_ID, collector.getId());
    assertEquals(StepType.NOOP, collector.getType());
    assertEquals(List.of("snapshot"), List.copyOf(collector.getParams().keySet()));
    ParamDefinition snapshot = collector.getParams().get("snapshot");
    assertEquals("publish__snapshot;", snapshot.getExpression());
    assertEquals(ParamMode.IMMUTABLE, snapshot.getMode());
    assertEquals(ParamSource.TEMPLATE_OUTPUT, snapshot.getSource());
    assertEquals(Map.of(Constants.TEMPLATE_OUTPUTS_STEP_ID, "true"), successors(steps, "announce"));
    assertEquals(Map.of(Constants.TEMPLATE_OUTPUTS_STEP_ID, "true"), successors(steps, "skip"));
    assertEquals(Map.of("announce", "true"), successors(steps, "publish"));
    assertTrue(collector.getTransition().getSuccessors().isEmpty());
  }

  @Test
  public void testAppendCollectorWithoutPromisedParams() {
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
