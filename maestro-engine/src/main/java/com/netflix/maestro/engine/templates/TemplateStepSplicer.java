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

import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.BooleanArrayParamDefinition;
import com.netflix.maestro.models.parameter.DoubleArrayParamDefinition;
import com.netflix.maestro.models.parameter.LongArrayParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.StringArrayParamDefinition;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplateStepInput;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds the step list of a template execution instance from the prepared steps: attaches the steps
 * the workflow supplied at the positions the template declares and appends the collector step that
 * evaluates the promised params.
 *
 * <p>The steps attached at one position form their own DAG. Those without an incoming edge from the
 * same position run after the position's predecessors; those without successors run before the
 * position's successors. When the position declares outputs, a join step named after the position
 * is placed between the attached steps and the successors; it collects every output as an array
 * over the attached steps, and the template's own steps read it as {@code <position>__<output>}.
 * Promised params the position exposes are added to every attached step with the template's
 * expression, so the attached steps reference them by name and never an internal step id. No
 * prepared edge is removed. Predecessor lists are cleared afterwards and derived again when the
 * instance is created. The steps passed in are mutated, so callers pass copies.
 */
public final class TemplateStepSplicer {
  private static final String ALWAYS = "true";
  private static final String UNKNOWN_POSITION_STEP =
      "Template step input position [%s] names %s [%s], which is not a prepared step";
  private static final String COLLISION =
      "Template step input [%s] collides with a prepared step id, a position name or another step input";
  private static final Map<ParamType, String> ARRAY_ELEMENT_TYPES =
      Map.of(
          ParamType.STRING, "String",
          ParamType.LONG, "long",
          ParamType.DOUBLE, "double",
          ParamType.BOOLEAN, "boolean");

  private TemplateStepSplicer() {}

  /**
   * Attach the supplied steps to the prepared steps at the positions the template declares.
   *
   * @param prepared the prepared steps, mutated in place
   * @param definition the registered template definition, read for positions and params
   * @param stepInputs the supplied steps keyed by position name, possibly null, mutated in place
   * @return the step list of the execution instance without the collector
   */
  public static List<Step> splice(
      List<Step> prepared, JobTemplate.Definition definition, Map<String, List<Step>> stepInputs) {
    List<Step> steps = new ArrayList<>(prepared);
    if (stepInputs != null && !stepInputs.isEmpty()) {
      Map<String, TemplateStepInput> positions =
          definition.getStepInputs() == null ? Map.of() : definition.getStepInputs();
      Map<String, Step> byId = new LinkedHashMap<>();
      for (Step step : steps) {
        byId.put(step.getId(), step);
      }
      Set<String> taken = new HashSet<>(byId.keySet());
      taken.addAll(positions.keySet());
      for (Map.Entry<String, List<Step>> entry : stepInputs.entrySet()) {
        Checks.checkTrue(
            positions.containsKey(entry.getKey()),
            "Template step input position [%s] is not declared by the template; declared positions: %s",
            entry.getKey(),
            positions.keySet());
        for (Step supplied : entry.getValue()) {
          Checks.checkTrue(taken.add(supplied.getId()), COLLISION, supplied.getId());
        }
      }
      for (Map.Entry<String, List<Step>> entry : stepInputs.entrySet()) {
        attach(
            steps,
            byId,
            definition,
            entry.getKey(),
            positions.get(entry.getKey()),
            entry.getValue());
      }
    }
    steps.forEach(step -> step.getTransition().setPredecessors(new ArrayList<>()));
    return steps;
  }

  private static void attach(
      List<Step> steps,
      Map<String, Step> byId,
      JobTemplate.Definition definition,
      String name,
      TemplateStepInput position,
      List<Step> attached) {
    Set<String> ids = attached.stream().map(Step::getId).collect(Collectors.toSet());
    Set<String> withIncoming = new HashSet<>();
    for (Step supplied : attached) {
      for (String successor : supplied.getTransition().getSuccessors().keySet()) {
        Checks.checkTrue(
            ids.contains(successor),
            "Template step input [%s] at position [%s] names successor [%s], which is not a step of the same position",
            supplied.getId(),
            name,
            successor);
        withIncoming.add(successor);
      }
      for (String predecessor : supplied.getTransition().getPredecessors()) {
        Step from =
            Checks.notNull(
                ids.contains(predecessor) ? byIdOf(attached, predecessor) : null,
                "Template step input [%s] at position [%s] names predecessor [%s], which is not a step of the same position",
                supplied.getId(),
                name,
                predecessor);
        from.getTransition().getSuccessors().putIfAbsent(supplied.getId(), ALWAYS);
        withIncoming.add(supplied.getId());
      }
    }
    List<Step> roots = attached.stream().filter(s -> !withIncoming.contains(s.getId())).toList();
    List<Step> leaves =
        attached.stream().filter(s -> s.getTransition().getSuccessors().isEmpty()).toList();

    List<String> predecessors =
        position.getPredecessors() == null ? List.of() : position.getPredecessors();
    for (String predecessor : predecessors) {
      Step from =
          Checks.notNull(
              byId.get(predecessor), UNKNOWN_POSITION_STEP, name, "predecessor", predecessor);
      roots.forEach(root -> from.getTransition().getSuccessors().putIfAbsent(root.getId(), ALWAYS));
    }
    List<String> successors =
        position.getSuccessors() == null ? List.of() : position.getSuccessors();
    successors.forEach(
        successor ->
            Checks.checkTrue(
                byId.containsKey(successor), UNKNOWN_POSITION_STEP, name, "successor", successor));

    List<Step> added = new ArrayList<>(attached);
    List<Step> exits = leaves;
    if (position.hasOutputs()) {
      Step join = createJoinStep(name, position, attached);
      leaves.forEach(leaf -> leaf.getTransition().getSuccessors().put(name, ALWAYS));
      added.add(join);
      exits = List.of(join);
    }
    for (Step exit : exits) {
      successors.forEach(
          successor -> exit.getTransition().getSuccessors().putIfAbsent(successor, ALWAYS));
    }
    exposeParams(definition, name, position, attached);
    for (Step supplied : added) {
      byId.put(supplied.getId(), supplied);
    }
    steps.addAll(added);
  }

  private static Step byIdOf(List<Step> steps, String id) {
    return steps.stream().filter(s -> s.getId().equals(id)).findFirst().orElse(null);
  }

  /**
   * The join step carries one array param per declared output, built over every attached step, e.g.
   * {@code return new long[]{write_a__row_count, write_b__row_count};}.
   */
  private static Step createJoinStep(String name, TemplateStepInput position, List<Step> attached) {
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    position
        .getOutputs()
        .forEach(
            (output, decl) -> {
              ParamType type = decl.getType();
              Checks.checkTrue(
                  ARRAY_ELEMENT_TYPES.containsKey(type),
                  "Template step input position [%s] output [%s] has type [%s], which cannot be collected",
                  name,
                  output,
                  type);
              for (Step supplied : attached) {
                ParamDefinition declared =
                    supplied.getParams() == null ? null : supplied.getParams().get(output);
                Checks.checkTrue(
                    declared != null && declared.getType() == type,
                    "Template step input [%s] at position [%s] must declare the output param [%s] of type [%s]",
                    supplied.getId(),
                    name,
                    output,
                    type);
              }
              String refs =
                  attached.stream()
                      .map(s -> s.getId() + Constants.DEFAULT_STEP_PARAM_SEPARATOR + output)
                      .collect(Collectors.joining(", "));
              String expression =
                  String.format("return new %s[]{%s};", ARRAY_ELEMENT_TYPES.get(type), refs);
              params.put(output, arrayParam(output, type, expression));
            });
    TypedStep join = new TypedStep();
    join.setId(name);
    join.setType(StepType.NOOP);
    join.setParams(params);
    return join;
  }

  private static ParamDefinition arrayParam(String name, ParamType elementType, String expression) {
    return switch (elementType) {
      case STRING ->
          StringArrayParamDefinition.builder()
              .name(name)
              .expression(expression)
              .mode(ParamMode.IMMUTABLE)
              .build();
      case LONG ->
          LongArrayParamDefinition.builder()
              .name(name)
              .expression(expression)
              .mode(ParamMode.IMMUTABLE)
              .build();
      case DOUBLE ->
          DoubleArrayParamDefinition.builder()
              .name(name)
              .expression(expression)
              .mode(ParamMode.IMMUTABLE)
              .build();
      case BOOLEAN ->
          BooleanArrayParamDefinition.builder()
              .name(name)
              .expression(expression)
              .mode(ParamMode.IMMUTABLE)
              .build();
      default -> throw new MaestroInternalError("Unsupported output type [%s]", elementType);
    };
  }

  /** Add every promised param the position exposes to each attached step. */
  private static void exposeParams(
      JobTemplate.Definition definition,
      String name,
      TemplateStepInput position,
      List<Step> attached) {
    if (position.getParams() == null || definition.getParams() == null) {
      return;
    }
    for (String param : position.getParams()) {
      ParamDefinition def =
          Checks.notNull(
              definition.getParams().get(param),
              "Template step input position [%s] exposes [%s], which is not a template param",
              name,
              param);
      if (!JobTemplate.Definition.isPromised(def)) {
        continue; // inputs are instance params and resolve by name without help
      }
      for (Step supplied : attached) {
        Checks.checkTrue(
            supplied.getParams() == null || !supplied.getParams().containsKey(param),
            "Template step input [%s] defines [%s], which is a param the template provides",
            supplied.getId(),
            param);
        addParam(supplied, templateOutput(def));
      }
    }
  }

  /**
   * A copy of a promised param as it is placed on a step: IMMUTABLE with the template output
   * source.
   */
  private static ParamDefinition templateOutput(ParamDefinition def) {
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put(Constants.METADATA_SOURCE_KEY, ParamSource.TEMPLATE_OUTPUT.name());
    return def.copyAndUpdate(
        null, def.getExpression(), ParamMode.IMMUTABLE, meta, def.getTags(), null);
  }

  private static void addParam(Step step, ParamDefinition def) {
    Map<String, ParamDefinition> params =
        step.getParams() == null ? new LinkedHashMap<>() : new LinkedHashMap<>(step.getParams());
    params.put(def.getName(), def);
    ((AbstractStep) step).setParams(params);
  }

  /**
   * Append the collector step carrying the promised params and point every step without successors
   * to it.
   *
   * @param steps the spliced steps, mutated in place
   * @param promised the promised params, possibly null
   * @return the final step list of the execution instance
   */
  public static List<Step> appendCollector(
      List<Step> steps, Map<String, ParamDefinition> promised) {
    Set<String> ids = new LinkedHashSet<>();
    steps.forEach(step -> ids.add(step.getId()));
    Checks.checkTrue(
        !ids.contains(Constants.TEMPLATE_OUTPUTS_STEP_ID),
        "Template step id [%s] is reserved for the output collector",
        Constants.TEMPLATE_OUTPUTS_STEP_ID);
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    if (promised != null) {
      promised.forEach((name, def) -> params.put(name, templateOutput(def)));
    }
    TypedStep collector = new TypedStep();
    collector.setId(Constants.TEMPLATE_OUTPUTS_STEP_ID);
    collector.setType(StepType.NOOP);
    collector.setParams(params);
    for (Step step : steps) {
      if (step.getTransition().getSuccessors().isEmpty()) {
        step.getTransition().getSuccessors().put(Constants.TEMPLATE_OUTPUTS_STEP_ID, ALWAYS);
      }
    }
    steps.add(collector);
    return steps;
  }
}
