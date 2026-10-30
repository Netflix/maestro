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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Preparer whose expansion payload is produced by a registered generator step. The generator runs
 * as a one-step inline instance, the prepare instance, with instance id {@value
 * #PREPARE_INSTANCE_ID}. The runtime declares the {@code expansion} output param on it; the
 * generator publishes the payload as a JSON string through the output-data path like any other
 * output param.
 *
 * <p>The generator receives every template param of the template step plus two platform params:
 * {@code maestro_step_inputs}, the supplied steps keyed by position name as a JSON string, and
 * {@code maestro_template_identity}, the job type and version being expanded as a JSON string.
 */
@Slf4j
@AllArgsConstructor
public class StepTemplatePreparer implements TemplatePreparer {
  /** Inline instance id of the prepare instance. */
  public static final long PREPARE_INSTANCE_ID = 1L;

  // the supplied steps are written through the declared generic type so that every step carries
  // its type id
  private static final TypeReference<Map<String, List<Step>>> STEP_INPUTS_TYPE =
      new TypeReference<>() {};
  private static final String JOB_TYPE_FIELD = "job_type";
  private static final String VERSION_FIELD = "version";

  private final TemplateInstanceRunner instanceRunner;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final ObjectMapper objectMapper;

  @Override
  public String type() {
    return TemplatePrepare.STEP;
  }

  @Override
  public PrepareResult prepare(PrepareContext context) {
    TemplateArtifact artifact = context.artifact();
    if (artifact.getPrepareInstanceId() == null) {
      return launchPrepareInstance(context);
    }
    return trackPrepareInstance(context);
  }

  private PrepareResult launchPrepareInstance(PrepareContext context) {
    TemplateArtifact artifact = context.artifact();
    Step generator = createGeneratorStep(context.definition().getPrepare().getStep());
    Workflow inlineWorkflow =
        TemplateInstanceRunner.createInlineWorkflow(
            artifact.getTemplateWorkflowId(),
            artifact.getTemplateIdentity(),
            context.step(),
            Collections.singletonList(generator));

    Optional<Details> details =
        instanceRunner.run(
            context.workflowSummary(),
            context.runtimeSummary(),
            context.step(),
            artifact,
            PREPARE_INSTANCE_ID,
            inlineWorkflow,
            createGeneratorRunParams(context),
            false);
    if (details.isPresent()) {
      LOG.warn(
          "In step runtime {}{}, failed to start the template prepare instance of {}, will retry",
          context.workflowSummary().getIdentity(),
          context.runtimeSummary().getIdentity(),
          artifact.getTemplateWorkflowId());
      return PrepareResult.pending(TimelineDetailsEvent.from(details.get()), false);
    }
    artifact.setPrepareInstanceId(PREPARE_INSTANCE_ID);
    LOG.info(
        "In step runtime {}{}, started the template prepare instance [{}][{}][{}]",
        context.workflowSummary().getIdentity(),
        context.runtimeSummary().getIdentity(),
        artifact.getTemplateWorkflowId(),
        PREPARE_INSTANCE_ID,
        artifact.getTemplateRunId());
    return PrepareResult.pending(
        TimelineLogEvent.info(
            "Started the template prepare instance: [%s][%s][%s]",
            artifact.getTemplateWorkflowId(), PREPARE_INSTANCE_ID, artifact.getTemplateRunId()),
        true);
  }

  /**
   * Copy the registered generator step and declare the {@code expansion} output param on it. The
   * registered definition is cached and must not be mutated.
   */
  private Step createGeneratorStep(Step registered) {
    Step generator = objectMapper.convertValue(registered, Step.class);
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    if (generator.getParams() != null) {
      params.putAll(generator.getParams());
    }
    params.put(
        Constants.TEMPLATE_EXPANSION_PARAM,
        StringParamDefinition.builder()
            .name(Constants.TEMPLATE_EXPANSION_PARAM)
            .value("")
            .mode(ParamMode.MUTABLE)
            .build());
    ((AbstractStep) generator).setParams(params);
    return generator;
  }

  private Map<String, ParamDefinition> createGeneratorRunParams(PrepareContext context) {
    Map<String, ParamDefinition> runParams =
        new LinkedHashMap<>(TemplateParams.createRunParams(context));
    Map<String, List<Step>> stepInputs =
        context.step() instanceof TemplateStep templateStep && templateStep.getStepInputs() != null
            ? templateStep.getStepInputs()
            : Collections.emptyMap();
    Map<String, String> identity = new LinkedHashMap<>();
    identity.put(JOB_TYPE_FIELD, context.definition().getJobType());
    identity.put(VERSION_FIELD, context.artifact().getTemplateVersion());
    try {
      runParams.put(
          Constants.TEMPLATE_STEP_INPUTS_PARAM,
          StringParamDefinition.builder()
              .name(Constants.TEMPLATE_STEP_INPUTS_PARAM)
              .value(objectMapper.writerFor(STEP_INPUTS_TYPE).writeValueAsString(stepInputs))
              .build());
      runParams.put(
          Constants.TEMPLATE_IDENTITY_PARAM,
          StringParamDefinition.builder()
              .name(Constants.TEMPLATE_IDENTITY_PARAM)
              .value(objectMapper.writeValueAsString(identity))
              .build());
    } catch (JsonProcessingException e) {
      throw new MaestroInternalError(
          e, "Cannot serialize the generator run params for template step [%s]", context.step());
    }
    return runParams;
  }

  @SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
  private PrepareResult trackPrepareInstance(PrepareContext context) {
    TemplateArtifact artifact = context.artifact();
    WorkflowInstance instance =
        instanceDao.getWorkflowInstanceRun(
            artifact.getTemplateWorkflowId(),
            artifact.getPrepareInstanceId(),
            artifact.getTemplateRunId());
    switch (instance.getStatus()) {
      case CREATED:
      case IN_PROGRESS:
      case PAUSED:
        return PrepareResult.pending(null, false);
      case SUCCEEDED:
        TemplateExpansion expansion = readExpansion(context, instance);
        return PrepareResult.ready(
            expansion,
            TimelineLogEvent.info(
                "Template prepare instance %s SUCCEEDED, expansion has %s steps and %s params",
                instance.getIdentity(),
                expansion.getSteps() == null ? 0 : expansion.getSteps().size(),
                expansion.getParams() == null ? 0 : expansion.getParams().size()));
      case FAILED:
        return failed(StepRuntime.State.FATAL_ERROR, instance);
      case STOPPED:
        return failed(StepRuntime.State.STOPPED, instance);
      case TIMED_OUT:
        return failed(StepRuntime.State.TIMED_OUT, instance);
      default:
        throw new MaestroInternalError(
            "Invalid status: %s for the template prepare instance %s of step %s",
            instance.getStatus(), instance.getIdentity(), context.runtimeSummary().getIdentity());
    }
  }

  private static PrepareResult failed(StepRuntime.State state, WorkflowInstance instance) {
    return PrepareResult.failed(
        state,
        TimelineLogEvent.info(
            "Step is in %s status because its template prepare instance %s is in %s status",
            state, instance.getIdentity(), instance.getStatus()));
  }

  private TemplateExpansion readExpansion(PrepareContext context, WorkflowInstance instance) {
    String generatorId = context.definition().getPrepare().getStep().getId();
    StepInstance generator =
        stepInstanceDao.getStepInstanceView(
            instance.getWorkflowId(), instance.getWorkflowInstanceId(), generatorId);
    Parameter param =
        generator.getParams() == null
            ? null
            : generator.getParams().get(Constants.TEMPLATE_EXPANSION_PARAM);
    String payload = param == null || !param.isEvaluated() ? null : param.asString();
    Checks.checkTrue(
        payload != null && !payload.isBlank(),
        "Template prepare instance %s generator step [%s] published no expansion",
        instance.getIdentity(),
        generatorId);
    try {
      return objectMapper.readValue(payload, TemplateExpansion.class);
    } catch (JsonProcessingException e) {
      throw new MaestroInternalError(
          e,
          "Template prepare instance %s generator step [%s] published an invalid expansion",
          instance.getIdentity(),
          generatorId);
    }
  }
}
