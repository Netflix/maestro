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
package com.netflix.maestro.engine.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.engine.templates.PrepareContext;
import com.netflix.maestro.engine.templates.PrepareResult;
import com.netflix.maestro.engine.templates.TemplateInstanceRunner;
import com.netflix.maestro.engine.templates.TemplateParams;
import com.netflix.maestro.engine.templates.TemplatePreparer;
import com.netflix.maestro.engine.templates.TemplateStepSplicer;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.Checks;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Template step runtime. It resolves the job template registered for the step's sub type, lets the
 * registered preparer produce the step list, adds the steps the workflow supplied where their
 * transitions place them, appends the output collector step, and runs the result as one inline
 * workflow instance, the execution instance. The outcome of that instance is the outcome of the
 * step: its output params are published onto the template step through the output-data path, and a
 * step inside it that ended COMPLETED_WITH_ERROR ends the template step COMPLETED_WITH_ERROR as
 * well.
 *
 * <p>The inline workflow id is stable for a given parent instance and template step. A step
 * preparer runs its generator in a prepare instance (id 1) before the execution instance (id 2);
 * the other preparers run the execution instance as id 1. The run id is the largest run id of the
 * inline workflow plus one every time the template step is restarted or retried.
 */
@Slf4j
public class TemplateStepRuntime implements StepRuntime {
  // the supplied steps are copied before the splice mutates their transitions and params
  private static final TypeReference<Map<String, List<Step>>> STEP_INPUTS_TYPE =
      new TypeReference<>() {};

  private final TemplateInstanceRunner instanceRunner;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final MaestroQueueSystem queueSystem;
  private final JobTemplateManager jobTemplateManager;
  private final OutputDataManager outputDataManager;
  private final ObjectMapper objectMapper;
  private final Map<String, TemplatePreparer> preparers;

  /**
   * Constructor.
   *
   * @param preparers the preparers available to template steps, keyed by their type; a job template
   *     whose prepare type has no preparer fails at start
   */
  public TemplateStepRuntime(
      TemplateInstanceRunner instanceRunner,
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroQueueSystem queueSystem,
      JobTemplateManager jobTemplateManager,
      OutputDataManager outputDataManager,
      ObjectMapper objectMapper,
      List<TemplatePreparer> preparers) {
    this.instanceRunner = instanceRunner;
    this.instanceDao = instanceDao;
    this.stepInstanceDao = stepInstanceDao;
    this.queueSystem = queueSystem;
    this.jobTemplateManager = jobTemplateManager;
    this.outputDataManager = outputDataManager;
    this.objectMapper = objectMapper;
    this.preparers =
        preparers.stream()
            .collect(Collectors.toUnmodifiableMap(TemplatePreparer::type, Function.identity()));
  }

  /** Template start resolves the registration and initializes the template artifact. */
  @Override
  public Result start(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    try {
      TemplateArtifact artifact = createArtifact(workflowSummary, step, runtimeSummary);
      return new Result(
          State.DONE,
          Collections.singletonMap(artifact.getType().key(), artifact),
          Collections.emptyList());
    } catch (MaestroRetryableError mre) {
      // retryable error, will retry by the parent logics
      throw mre;
    } catch (Exception e) {
      LOG.error(
          "Failed to start template step runtime for {}{}",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      return new Result(
          State.FATAL_ERROR,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineDetailsEvent.from(
                  Details.create(
                      e, false, "Failed to start template step runtime with an error"))));
    }
  }

  /**
   * Build the artifact of this run. A restart that is not a fresh run reuses the execution instance
   * of the previous run, with the template version that instance ran: its id is copied here and
   * {@link #execute} restarts it. Every other run prepares the step list again.
   */
  private TemplateArtifact createArtifact(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    String version = jobTemplateManager.getJobTemplateVersion(workflowSummary, step);
    JobTemplate.Definition definition = jobTemplateManager.loadDefinition(step, version);
    getPreparer(definition);

    TemplateArtifact artifact = new TemplateArtifact();
    artifact.setTemplateWorkflowId(
        StepHelper.generateInlineWorkflowId(workflowSummary, runtimeSummary));
    artifact.setTemplateIdentity(workflowSummary.getIdentity() + runtimeSummary.getIdentity());
    artifact.setTemplateRunId(1L);
    artifact.setJobType(step.getSubType());
    artifact.setTemplateVersion(version);
    artifact.setPrepareType(definition.getPrepare().getType());

    if (!workflowSummary.isFreshRun() || !runtimeSummary.getStepRetry().isRetryable()) {
      artifact.setTemplateRunId(
          instanceDao.getLargestTemplateRunIdFromRuns(artifact.getTemplateWorkflowId()) + 1L);
      if (!workflowSummary.isFreshRun()) {
        TemplateArtifact prevArtifact =
            stepInstanceDao.getLatestTemplateArtifact(
                workflowSummary.getWorkflowId(),
                workflowSummary.getWorkflowInstanceId(),
                runtimeSummary.getStepId());
        if (prevArtifact != null && prevArtifact.getTemplateInstanceId() != null) {
          artifact.setPrepareType(prevArtifact.getPrepareType());
          artifact.setTemplateVersion(prevArtifact.getTemplateVersion());
          artifact.setTemplateInstanceId(prevArtifact.getTemplateInstanceId());
          artifact.setExpansion(prevArtifact.getExpansion());
        }
      }
    }
    return artifact;
  }

  /**
   * Template execute prepares the step list, launches the execution instance and monitors it. An
   * artifact with an execution instance id but no execution instance uuid is a restart whose
   * instance has not been relaunched yet.
   */
  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    try {
      TemplateArtifact artifact =
          runtimeSummary.getArtifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
      if (artifact.getTemplateInstanceId() == null) {
        return prepare(workflowSummary, step, runtimeSummary, artifact);
      }
      if (artifact.getTemplateUuid() == null) {
        return restartExecutionInstance(workflowSummary, step, runtimeSummary, artifact);
      }
      return trackExecutionInstance(workflowSummary, step, runtimeSummary, artifact);
    } catch (MaestroRetryableError mre) {
      LOG.info(
          "Failed to execute template {}{}, will retry",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          mre);
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(TimelineDetailsEvent.from(mre.getDetails())));
    } catch (Exception e) {
      LOG.warn(
          "Failed to execute template step runtime {}{}, with error",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      return new Result(
          State.FATAL_ERROR,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineDetailsEvent.from(
                  Details.create(
                      e, false, "Failed to execute template step runtime with an error"))));
    }
  }

  /** Restart the previous run's execution instance with the definition of the version it ran. */
  private Result restartExecutionInstance(
      WorkflowSummary workflowSummary,
      Step step,
      StepRuntimeSummary runtimeSummary,
      TemplateArtifact artifact) {
    JobTemplate.Definition definition =
        jobTemplateManager.loadDefinition(step, artifact.getTemplateVersion());
    PrepareContext context =
        new PrepareContext(workflowSummary, step, runtimeSummary, definition, artifact);
    return launchExecutionInstance(context, Collections.emptyList(), null, true);
  }

  /** Runs the preparer and, once the expansion is ready, launches the execution instance. */
  private Result prepare(
      WorkflowSummary workflowSummary,
      Step step,
      StepRuntimeSummary runtimeSummary,
      TemplateArtifact artifact) {
    JobTemplate.Definition definition =
        jobTemplateManager.loadDefinition(step, artifact.getTemplateVersion());
    TemplatePreparer preparer = getPreparer(definition);
    PrepareContext context =
        new PrepareContext(workflowSummary, step, runtimeSummary, definition, artifact);
    PrepareResult result = preparer.prepare(context);
    List<TimelineEvent> timeline =
        result.event() == null ? Collections.emptyList() : List.of(result.event());
    if (result.isReady()) {
      Checks.checkTrue(
          result.expansion().getSteps() != null && !result.expansion().getSteps().isEmpty(),
          "Template step [%s] prepared an expansion without steps",
          step.getId());
      // the artifact keeps the expansion as returned; the splice works on a copy
      artifact.setExpansion(objectMapper.convertValue(result.expansion(), TemplateExpansion.class));
      TemplateExpansion expansion =
          objectMapper.convertValue(result.expansion(), TemplateExpansion.class);
      Map<String, List<Step>> stepInputs =
          step instanceof TemplateStep templateStep && templateStep.getStepInputs() != null
              ? copyStepInputs(templateStep.getStepInputs())
              : Collections.emptyMap();
      List<Step> steps = TemplateStepSplicer.splice(expansion.getSteps(), definition, stepInputs);
      TemplateStepSplicer.appendCollector(steps, definition.promisedParams());
      artifact.setTemplateInstanceId(
          artifact.getPrepareInstanceId() == null ? 1L : artifact.getPrepareInstanceId() + 1L);
      Result launched = launchExecutionInstance(context, steps, expansion.getParams(), false);
      if (timeline.isEmpty()) {
        return launched;
      }
      List<TimelineEvent> events = new ArrayList<>(timeline);
      events.addAll(launched.timeline());
      return new Result(launched.state(), launched.artifacts(), events);
    }
    if (result.state() == State.CONTINUE) {
      return new Result(
          State.CONTINUE,
          result.artifactChanged()
              ? Collections.singletonMap(artifact.getType().key(), artifact)
              : Collections.emptyMap(),
          timeline);
    }
    return new Result(
        result.state(), Collections.singletonMap(artifact.getType().key(), artifact), timeline);
  }

  /**
   * Copy the supplied steps before the splice mutates their transitions. The copy goes through the
   * declared generic type so that every step is written with its type id.
   */
  private Map<String, List<Step>> copyStepInputs(Map<String, List<Step>> stepInputs) {
    try {
      return objectMapper.readValue(
          objectMapper.writerFor(STEP_INPUTS_TYPE).writeValueAsString(stepInputs),
          STEP_INPUTS_TYPE);
    } catch (JsonProcessingException e) {
      throw new MaestroInternalError(e, "Cannot copy the template step inputs %s", stepInputs);
    }
  }

  private TemplatePreparer getPreparer(JobTemplate.Definition definition) {
    String type = definition.getPrepare().getType();
    return Checks.notNull(
        preparers.get(type),
        "Job template [%s] uses the prepare type [%s], which has no preparer; available types: %s",
        definition.getJobType(),
        type,
        preparers.keySet());
  }

  private Result launchExecutionInstance(
      PrepareContext context,
      List<Step> steps,
      Map<String, ParamDefinition> expansionParams,
      boolean restartPrevious) {
    TemplateArtifact artifact = context.artifact();
    Workflow inlineWorkflow =
        TemplateInstanceRunner.createInlineWorkflow(
            artifact.getTemplateWorkflowId(),
            artifact.getTemplateIdentity(),
            context.step(),
            steps);
    Map<String, ParamDefinition> runParams = TemplateParams.createRunParams(context);
    if (expansionParams != null) {
      runParams.putAll(expansionParams);
    }

    Optional<Details> details =
        instanceRunner.run(
            context.workflowSummary(),
            context.runtimeSummary(),
            context.step(),
            artifact,
            artifact.getTemplateInstanceId(),
            inlineWorkflow,
            runParams,
            restartPrevious);
    if (details.isPresent()) {
      LOG.warn(
          "In step runtime {}{}, failed to start template execution instance {}, will try it again",
          context.workflowSummary().getIdentity(),
          context.runtimeSummary().getIdentity(),
          artifact.getIdentity());
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(TimelineDetailsEvent.from(details.get())));
    }
    LOG.info(
        "In step runtime {}{}, started a template execution instance {} of job template [{}][{}]",
        context.workflowSummary().getIdentity(),
        context.runtimeSummary().getIdentity(),
        artifact.getIdentity(),
        artifact.getJobType(),
        artifact.getTemplateVersion());
    return new Result(
        State.CONTINUE,
        Collections.singletonMap(artifact.getType().key(), artifact),
        Collections.singletonList(
            TimelineLogEvent.info(
                "Started a template execution instance: " + artifact.getIdentity())));
  }

  @SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
  private Result trackExecutionInstance(
      WorkflowSummary workflowSummary,
      Step step,
      StepRuntimeSummary runtimeSummary,
      TemplateArtifact artifact) {
    WorkflowInstance instance = getWorkflowInstance(artifact);
    State state;
    switch (instance.getStatus()) {
      case CREATED:
      case IN_PROGRESS:
      case PAUSED:
        state = State.CONTINUE;
        break;
      case SUCCEEDED:
        publishOutputParams(workflowSummary, step, runtimeSummary, artifact);
        state =
            hasCompletedWithErrorStep(instance.getRuntimeOverview())
                ? State.COMPLETED_WITH_ERROR
                : State.DONE;
        break;
      case FAILED:
        state = State.FATAL_ERROR; // no retry for template step
        break;
      case STOPPED:
        state = State.STOPPED;
        break;
      case TIMED_OUT:
        state = State.TIMED_OUT;
        break;
      default:
        throw new MaestroInternalError(
            "Invalid status: %s for template step %s%s",
            instance.getStatus(), instance.getIdentity(), runtimeSummary.getIdentity());
    }

    artifact.setTemplateOverview(instance.getRuntimeOverview());

    TimelineEvent timelineEvent = null;
    if (instance.getStatus().isTerminal()) {
      timelineEvent =
          TimelineLogEvent.info(
              "Step is in %s status because its template execution instance is in %s status",
              state, instance.getStatus());
    }

    return new Result(
        state,
        Collections.singletonMap(artifact.getType().key(), artifact),
        timelineEvent == null ? Collections.emptyList() : Collections.singletonList(timelineEvent));
  }

  private static boolean hasCompletedWithErrorStep(WorkflowRuntimeOverview overview) {
    if (overview == null || overview.getStepOverview() == null) {
      return false;
    }
    WorkflowStepStatusSummary summary =
        overview.getStepOverview().get(StepInstance.Status.COMPLETED_WITH_ERROR);
    return summary != null && summary.getCnt() > 0;
  }

  /**
   * Save the collector step's evaluated output params as the template step's output data, keyed by
   * the template step instance uuid. The FINISHING phase merges them onto the template step.
   */
  private void publishOutputParams(
      WorkflowSummary workflowSummary,
      Step step,
      StepRuntimeSummary runtimeSummary,
      TemplateArtifact artifact) {
    JobTemplate.Definition definition =
        jobTemplateManager.loadDefinition(step, artifact.getTemplateVersion());
    Map<String, ParamDefinition> promised = definition.promisedParams();
    if (promised.isEmpty()) {
      return;
    }
    StepInstance collector =
        stepInstanceDao.getStepInstanceView(
            artifact.getTemplateWorkflowId(),
            artifact.getTemplateInstanceId(),
            Constants.TEMPLATE_OUTPUTS_STEP_ID);
    Map<String, Parameter> outputs = new LinkedHashMap<>();
    for (String name : promised.keySet()) {
      Parameter param = collector.getParams() == null ? null : collector.getParams().get(name);
      Checks.checkTrue(
          param != null && param.isEvaluated(),
          "Template execution instance %s did not evaluate the output param [%s]",
          artifact.getIdentity(),
          name);
      outputs.put(name, toLiteral(param));
    }
    long now = System.currentTimeMillis();
    outputDataManager.saveOutputData(
        new OutputData(
            runtimeSummary.getType(),
            runtimeSummary.getStepInstanceUuid(),
            workflowSummary.getWorkflowId(),
            now,
            now,
            outputs,
            Collections.emptyMap()));
  }

  /**
   * Output data carries values, not expressions. The collector evaluates each output param inside
   * the inline instance, so its evaluated result becomes the literal value published on the
   * template step.
   */
  private static Parameter toLiteral(Parameter param) {
    Parameter literal =
        param
            .toDefinition()
            .copyAndUpdate(param.getEvaluatedResult(), null, ParamMode.MUTABLE, null, null, null)
            .toParameter();
    literal.setEvaluatedResult(param.getEvaluatedResult());
    literal.setEvaluatedTime(param.getEvaluatedTime());
    return literal;
  }

  /**
   * Terminate the active inline instance, the execution instance once launched and the prepare
   * instance before that. No action is needed here other than monitoring that the instance is
   * stopped. The action related to termination has already been written to DB. The final step
   * instance termination callback will do the cleanup, including the step action deletion.
   */
  @Override
  public Result terminate(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    try {
      if (runtimeSummary.getArtifacts().containsKey(Artifact.Type.TEMPLATE.key())) {
        TemplateArtifact artifact =
            runtimeSummary.getArtifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
        Long instanceId = activeInstanceId(artifact);
        if (instanceId == null) {
          LOG.debug(
              "Template step {}{} has not started an inline workflow instance and does nothing.",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity());
          return Result.of(State.STOPPED);
        }

        WorkflowInstance.Status status =
            instanceDao.getWorkflowInstanceStatus(
                artifact.getTemplateWorkflowId(), instanceId, artifact.getTemplateRunId());

        String identity = inlineIdentity(artifact, instanceId);
        if (status == null) {
          LOG.warn(
              "{}{} is terminating a not-found template inline workflow {}, which might be deleted.",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity(),
              identity);
          return new Result(
              State.STOPPED,
              Collections.emptyMap(),
              Collections.singletonList(
                  TimelineLogEvent.warn(
                      "Cannot find template inline workflow instance (might be deleted): "
                          + identity)));
        }

        LOG.debug(
            "{}{} is terminating the template inline workflow instance [{}][{}][{}] in a status [{}]",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity(),
            artifact.getTemplateWorkflowId(),
            instanceId,
            artifact.getTemplateRunId(),
            status);

        if (!status.isTerminal()) {
          tryTerminateQueuedInstanceIfNeeded(artifact, instanceId, status);
          wakeUpUnderlyingActor(workflowSummary.getGroupInfo(), artifact, instanceId);
          throw new MaestroRetryableError(
              "Termination at template step %s%s is not done and will retry it.",
              workflowSummary.getIdentity(), runtimeSummary.getIdentity());
        }

        // update with the final inline workflow runtime overview
        WorkflowInstance instance = getWorkflowInstance(artifact, instanceId);
        artifact.setTemplateOverview(instance.getRuntimeOverview());
        return new Result(
            State.STOPPED,
            Collections.singletonMap(artifact.getType().key(), artifact),
            Collections.singletonList(
                TimelineLogEvent.info(
                    "Terminated the running template inline workflow instance: " + identity)));
      } else {
        LOG.debug(
            "Template step {}{} haven't start a template inline workflow instance and do nothing.",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity());
      }
    } catch (MaestroNotFoundException e) {
      LOG.warn("Ignore termination as workflow instance is not stoppable due to", e);
    }
    return Result.of(State.STOPPED);
  }

  /**
   * Identity of an inline instance of this template step: the execution or the prepare instance.
   */
  private static String inlineIdentity(TemplateArtifact artifact, long instanceId) {
    return String.format(
        "[%s][%s][%s]", artifact.getTemplateWorkflowId(), instanceId, artifact.getTemplateRunId());
  }

  private static Long activeInstanceId(TemplateArtifact artifact) {
    return artifact.getTemplateInstanceId() != null
        ? artifact.getTemplateInstanceId()
        : artifact.getPrepareInstanceId();
  }

  /** If the inline workflow instance is queued, then terminate it now. */
  private void tryTerminateQueuedInstanceIfNeeded(
      TemplateArtifact artifact, long instanceId, WorkflowInstance.Status status) {
    if (status == WorkflowInstance.Status.CREATED || status == WorkflowInstance.Status.PAUSED) {
      WorkflowInstance toTerminate = getWorkflowInstance(artifact, instanceId);
      instanceDao.tryTerminateQueuedInstance(
          toTerminate,
          WorkflowInstance.Status.STOPPED,
          "The queued workflow instance is terminated by its upstream template step.");
    }
  }

  private WorkflowInstance getWorkflowInstance(TemplateArtifact artifact) {
    return getWorkflowInstance(artifact, artifact.getTemplateInstanceId());
  }

  private WorkflowInstance getWorkflowInstance(TemplateArtifact artifact, long instanceId) {
    return instanceDao.getWorkflowInstanceRun(
        artifact.getTemplateWorkflowId(), instanceId, artifact.getTemplateRunId());
  }

  private void wakeUpUnderlyingActor(long groupInfo, TemplateArtifact artifact, long instanceId) {
    var msg =
        MessageDto.createMessageForWakeUp(
            artifact.getTemplateWorkflowId(),
            groupInfo,
            Map.of(instanceId, artifact.getTemplateRunId()));
    queueSystem.notify(msg);
  }

  /**
   * Declare the registered template params and the preparer's own params on the template step. A
   * promised param is declared with its type's default value and MUTABLE mode in place of the
   * registered expression, which only evaluates inside the inline instance, so that the output-data
   * merge at the end of the step can overwrite it.
   */
  @Override
  public Map<String, ParamDefinition> injectRuntimeParams(
      WorkflowSummary workflowSummary, Step step) {
    Map<String, ParamDefinition> params =
        new LinkedHashMap<>(jobTemplateManager.loadRuntimeParams(workflowSummary, step));
    String version = jobTemplateManager.getJobTemplateVersion(workflowSummary, step);
    jobTemplateManager
        .findDefinition(step, version)
        .ifPresent(
            definition -> {
              definition
                  .promisedParams()
                  .forEach((name, def) -> params.put(name, declareOutputParam(def)));
              params.putAll(getPreparer(definition).injectParams(definition));
            });
    return Collections.unmodifiableMap(params);
  }

  private static ParamDefinition declareOutputParam(ParamDefinition def) {
    return def.copyAndUpdate(
        defaultValue(def.getType()), null, ParamMode.MUTABLE, null, def.getTags(), null);
  }

  private static Object defaultValue(ParamType type) {
    return switch (type) {
      case STRING -> "";
      case LONG -> 0L;
      case DOUBLE -> BigDecimal.ZERO;
      case BOOLEAN -> Boolean.FALSE;
      case STRING_MAP -> new LinkedHashMap<String, String>();
      case STRING_ARRAY -> new String[0];
      case LONG_ARRAY -> new long[0];
      case DOUBLE_ARRAY -> new BigDecimal[0];
      case BOOLEAN_ARRAY -> new boolean[0];
      case MAP -> new LinkedHashMap<String, ParamDefinition>();
    };
  }

  @Override
  public List<Tag> injectRuntimeTags(WorkflowSummary workflowSummary, Step step) {
    return jobTemplateManager.loadTags(workflowSummary, step);
  }
}
