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

import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.IdHelper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Template step runtime. It runs the step list registered in the job template of the step's
 * sub_type as one inline workflow instance and reports that instance's outcome as the step's
 * outcome. The inline instance bypasses the run strategy manager as the template step manages it.
 *
 * <p>The inline workflow id is stable for a given parent instance and template step. Its instance
 * id is always 1 and its run id increases by one every time the template step is restarted.
 */
@Slf4j
@AllArgsConstructor
public class TemplateStepRuntime implements StepRuntime {
  private static final String TEMPLATE_TAG_NAME = Constants.TEMPLATE_INLINE_WORKFLOW_PREFIX;
  private static final long TEMPLATE_INSTANCE_ID = 1L;
  private static final long FIRST_RUN_ID = 1L;

  private final WorkflowActionHandler actionHandler;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final MaestroQueueSystem queueSystem;
  private final InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;
  private final JobTemplateManager jobTemplateManager;

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    boolean isStarting =
        runtimeSummary.getArtifacts() == null
            || !runtimeSummary.getArtifacts().containsKey(Artifact.Type.TEMPLATE.key());
    String action = (isStarting ? "start" : "execute");
    try {
      if (isStarting) {
        return runTemplateInstance(workflowSummary, step, runtimeSummary);
      } else {
        return trackTemplateInstance(runtimeSummary);
      }
    } catch (MaestroRetryableError mre) {
      LOG.info(
          "Failed to {} template {}{}, will retry",
          action,
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          mre);
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(TimelineDetailsEvent.from(mre.getDetails())));
    } catch (Exception e) {
      LOG.warn(
          "Failed to {} template step runtime {}{}, with error",
          action,
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      return new Result(
          State.FATAL_ERROR,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineDetailsEvent.from(
                  Details.create(
                      e, false, "Failed to " + action + " template step runtime with an error"))));
    }
  }

  private Result runTemplateInstance(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    String version = jobTemplateManager.getJobTemplateVersion(workflowSummary, step);
    List<Step> steps = jobTemplateManager.loadSteps(step, version);
    TemplateArtifact artifact = createArtifact(workflowSummary, runtimeSummary, step, version);
    String templateIdentity = workflowSummary.getIdentity() + runtimeSummary.getIdentity();
    Workflow inlineWorkflow =
        createInlineWorkflow(artifact.getTemplateWorkflowId(), templateIdentity, step, steps);

    RunRequest runRequest =
        StepHelper.createInternalWorkflowRunRequest(
            workflowSummary,
            runtimeSummary,
            Collections.singletonList(Tag.create(TEMPLATE_TAG_NAME)),
            createTemplateRunParams(workflowSummary, step, runtimeSummary),
            artifact.getIdentity(),
            null);
    if (artifact.getTemplateRunId() == FIRST_RUN_ID) {
      // always reset runRequest to be START_FRESH_NEW_RUN as this is the first run
      runRequest.clearRestartFor(RunPolicy.START_FRESH_NEW_RUN);
    }
    artifact.setTemplateUuid(IdHelper.getOrCreateUuid(runRequest.getRequestId()));

    if (!instanceStepConcurrencyHandler.addInstance(runRequest)) {
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineLogEvent.info(
                  "Unavailable due to InstanceStepConcurrency and will retry later to launch the template inline workflow")));
    } // no need to release if exception

    Optional<Details> details =
        actionHandler.runTemplateInstance(
            inlineWorkflow,
            workflowSummary.getInternalId(), // inherit parent unique internalId
            workflowSummary.getWorkflowVersionId(), // inherit parent versionId
            workflowSummary.getRunProperties(), // inherit parent run properties
            step.getId(),
            artifact,
            runRequest);
    if (details.isPresent()) {
      LOG.warn(
          "In step runtime {}{}, failed to start template inline workflow {}, will try it again",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          artifact.getIdentity());
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(TimelineDetailsEvent.from(details.get())));
    }
    LOG.info(
        "In step runtime {}{}, started a template inline workflow instance {} of job template [{}][{}]",
        workflowSummary.getIdentity(),
        runtimeSummary.getIdentity(),
        artifact.getIdentity(),
        artifact.getJobType(),
        artifact.getTemplateVersion());
    return new Result(
        State.CONTINUE,
        Collections.singletonMap(artifact.getType().key(), artifact),
        Collections.singletonList(
            TimelineLogEvent.info(
                "Started a template inline workflow instance: " + artifact.getIdentity())));
  }

  /**
   * Create the template artifact. For a restart from the workflow or a restart of the step, the run
   * id continues from the latest artifact of the previous run. Otherwise, it is a fresh first run.
   */
  private TemplateArtifact createArtifact(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      Step step,
      String version) {
    TemplateArtifact artifact = new TemplateArtifact();
    artifact.setTemplateWorkflowId(
        StepHelper.generateInlineWorkflowId(workflowSummary, runtimeSummary));
    artifact.setTemplateInstanceId(TEMPLATE_INSTANCE_ID);
    artifact.setTemplateRunId(FIRST_RUN_ID);
    artifact.setJobType(step.getSubType());
    artifact.setTemplateVersion(version);

    if (!workflowSummary.isFreshRun() || !runtimeSummary.getStepRetry().isRetryable()) {
      TemplateArtifact prevArtifact =
          stepInstanceDao.getLatestTemplateArtifact(
              workflowSummary.getWorkflowId(),
              workflowSummary.getWorkflowInstanceId(),
              runtimeSummary.getStepId());
      if (prevArtifact != null) {
        artifact.setTemplateRunId(prevArtifact.getTemplateRunId() + 1);
      }
    }
    return artifact;
  }

  /**
   * Create an inline workflow definition for the registered template steps. It has empty params.
   * All workflow params and user defined step params of the template step are injected from
   * run_params by the workflow start request.
   */
  private static Workflow createInlineWorkflow(
      String templateWorkflowId, String templateIdentity, Step step, List<Step> steps) {
    return Workflow.builder()
        .id(templateWorkflowId)
        .name(templateIdentity)
        .description(
            "Maestro template inline workflow including registered steps created by "
                + templateIdentity)
        .tags(step.getTags())
        // no need to set workflow timeout using the template step timeout
        .params(Collections.emptyMap()) // no param in inline workflow definition
        .steps(steps)
        .build(); // inline steps won't inherit template step failure mode
  }

  private Map<String, ParamDefinition> createTemplateRunParams(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    Map<String, ParamDefinition> runParams = new LinkedHashMap<>();
    // all workflow params visible in the template step will be passed to the inline workflow
    workflowSummary.getParams().forEach((k, v) -> runParams.put(k, v.toDefinition()));
    // all user defined params in the step definition will be passed to the inline workflow
    step.getParams()
        .forEach((k, v) -> runParams.put(k, runtimeSummary.getParams().get(k).toDefinition()));
    return runParams;
  }

  @SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
  private Result trackTemplateInstance(StepRuntimeSummary runtimeSummary) {
    TemplateArtifact artifact =
        runtimeSummary.getArtifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
    WorkflowInstance instance = getWorkflowInstance(artifact);
    State state;
    switch (instance.getStatus()) {
      case CREATED:
      case IN_PROGRESS:
      case PAUSED:
        state = State.CONTINUE;
        break;
      case SUCCEEDED:
        state = State.DONE;
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
              "Step is in %s status because its template inline workflow instance is in %s status",
              state, instance.getStatus());
    }

    return new Result(
        state,
        Collections.singletonMap(artifact.getType().key(), artifact),
        timelineEvent == null ? Collections.emptyList() : Collections.singletonList(timelineEvent));
  }

  /**
   * Terminate the template inline workflow instance. No action is needed here and just monitor if
   * the instance is stopped. The action related to termination has already been written to DB. The
   * final step instance termination callback will do the cleanup, including the step action
   * deletion.
   */
  @Override
  public Result terminate(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    try {
      if (runtimeSummary.getArtifacts().containsKey(Artifact.Type.TEMPLATE.key())) {
        TemplateArtifact artifact =
            runtimeSummary.getArtifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();

        WorkflowInstance.Status status =
            instanceDao.getWorkflowInstanceStatus(
                artifact.getTemplateWorkflowId(),
                artifact.getTemplateInstanceId(),
                artifact.getTemplateRunId());

        if (status == null) {
          LOG.warn(
              "{}{} is terminating a not-found template inline workflow {}, which might be deleted.",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity(),
              artifact.getIdentity());
          return new Result(
              State.STOPPED,
              Collections.emptyMap(),
              Collections.singletonList(
                  TimelineLogEvent.warn(
                      "Cannot find template inline workflow instance (might be deleted): "
                          + artifact.getIdentity())));
        }

        LOG.debug(
            "{}{} is terminating the template inline workflow instance {} in a status [{}]",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity(),
            artifact.getIdentity(),
            status);

        if (!status.isTerminal()) {
          tryTerminateQueuedInstanceIfNeeded(artifact, status);
          wakeUpUnderlyingActor(workflowSummary.getGroupInfo(), artifact);
          throw new MaestroRetryableError(
              "Termination at template step %s%s is not done and will retry it.",
              workflowSummary.getIdentity(), runtimeSummary.getIdentity());
        }

        // update with the final template inline workflow runtime overview
        WorkflowInstance instance = getWorkflowInstance(artifact);
        artifact.setTemplateOverview(instance.getRuntimeOverview());
        return new Result(
            State.STOPPED,
            Collections.singletonMap(artifact.getType().key(), artifact),
            Collections.singletonList(
                TimelineLogEvent.info(
                    "Terminated the running template inline workflow instance: "
                        + artifact.getIdentity())));
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

  /** If the template inline workflow instance is queued, then terminate it now. */
  private void tryTerminateQueuedInstanceIfNeeded(
      TemplateArtifact artifact, WorkflowInstance.Status status) {
    if (status == WorkflowInstance.Status.CREATED || status == WorkflowInstance.Status.PAUSED) {
      WorkflowInstance toTerminate = getWorkflowInstance(artifact);
      instanceDao.tryTerminateQueuedInstance(
          toTerminate,
          WorkflowInstance.Status.STOPPED,
          "The queued workflow instance is terminated by its upstream template step.");
    }
  }

  private WorkflowInstance getWorkflowInstance(TemplateArtifact artifact) {
    return instanceDao.getWorkflowInstanceRun(
        artifact.getTemplateWorkflowId(),
        artifact.getTemplateInstanceId(),
        artifact.getTemplateRunId());
  }

  private void wakeUpUnderlyingActor(long groupInfo, TemplateArtifact artifact) {
    var msg =
        MessageDto.createMessageForWakeUp(
            artifact.getTemplateWorkflowId(),
            groupInfo,
            Map.of(artifact.getTemplateInstanceId(), artifact.getTemplateRunId()));
    queueSystem.notify(msg);
  }

  @Override
  public Map<String, ParamDefinition> injectRuntimeParams(
      WorkflowSummary workflowSummary, Step step) {
    return jobTemplateManager.loadRuntimeParams(workflowSummary, step);
  }

  @Override
  public List<Tag> injectRuntimeTags(WorkflowSummary workflowSummary, Step step) {
    return jobTemplateManager.loadTags(workflowSummary, step);
  }
}
