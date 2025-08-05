/*
 * Copyright 2024 Netflix, Inc.
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
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowInstanceActionHandler;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.SubworkflowStep;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Subworkflow step runtime. The downstream subworkflow workflow instance id and run id may not
 * match the upstream.
 */
@Slf4j
@AllArgsConstructor
public class SubworkflowStepRuntime implements StepRuntime {
  private static final String SUBWORKFLOW_NAME = StepType.SUBWORKFLOW.getType();
  private static final String SUBWORKFLOW_ID_PARAM_NAME = SUBWORKFLOW_NAME + "_id";
  private static final String SUBWORKFLOW_VERSION_PARAM_NAME = SUBWORKFLOW_NAME + "_version";
  private static final String SUBWORKFLOW_TAG_NAME = Constants.MAESTRO_PREFIX + SUBWORKFLOW_NAME;

  private final WorkflowActionHandler actionHandler;
  private final WorkflowInstanceActionHandler instanceActionHandler;

  private final InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;

  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final MaestroQueueSystem queueSystem;
  private final Set<String> alwaysPassDownParamNames;

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    boolean isStarting =
        runtimeSummary.getArtifacts() == null
            || runtimeSummary.getArtifacts().isEmpty()
            || !runtimeSummary.getArtifacts().containsKey(Artifact.Type.SUBWORKFLOW.key());
    String action = (isStarting ? "start" : "execute");
    try {
      if (isStarting) {
        return runSubworkflowInstance(workflowSummary, step, runtimeSummary);
      } else {
        return trackSubworkflowInstance(step, runtimeSummary);
      }
    } catch (MaestroRetryableError mre) {
      LOG.info(
          "Failed to {} subworkflow {}{}, will retry",
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
          "Failed to {} subworkflow step runtime {}{}, with error",
          action,
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      // todo improve this error handling to gracefully clean up all resources
      return new Result(
          State.FATAL_ERROR,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineDetailsEvent.from(
                  Details.create(
                      e,
                      false,
                      "Failed to " + action + " subworkflow step runtime with an error"))));
    }
  }

  private Result runSubworkflowInstance(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    RunRequest runRequest =
        StepHelper.createInternalWorkflowRunRequest(
            workflowSummary,
            runtimeSummary,
            Collections.singletonList(Tag.create(SUBWORKFLOW_TAG_NAME)),
            createSubworkflowRunParam(workflowSummary, step, runtimeSummary),
            workflowSummary.getIdentity() + runtimeSummary.getIdentity(),
            ((SubworkflowStep) step).getSync());

    if (!instanceStepConcurrencyHandler.addInstance(runRequest)) {
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineLogEvent.info(
                  "Unavailable due to InstanceStepConcurrency and will retry later to launch the subworkflow")));
    }

    try {
      RunResponse runResponse = null;
      // restart from workflow or restart the step
      if (!workflowSummary.isFreshRun() || !runtimeSummary.getStepRetry().isRetryable()) {
        SubworkflowArtifact subworkflowArtifact =
            stepInstanceDao.getLatestSubworkflowArtifact(
                workflowSummary.getWorkflowId(),
                workflowSummary.getWorkflowInstanceId(),
                step.getId());
        if (subworkflowArtifact != null) {
          WorkflowInstance instance =
              instanceDao.getWorkflowInstanceRun(
                  subworkflowArtifact.getSubworkflowId(),
                  subworkflowArtifact.getSubworkflowInstanceId(),
                  subworkflowArtifact.getSubworkflowRunId());

          runRequest.updateForDownstreamIfNeeded(step.getId(), instance);
          runResponse = instanceActionHandler.restartDirectly(instance, runRequest);
          LOG.info(
              "In step runtime {}{}, restarting a subworkflow instance {} from the parent run with response {}",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity(),
              subworkflowArtifact.getIdentity(),
              runResponse);
        }
      }

      if (runResponse == null) {
        String subworkflowId = runtimeSummary.getParams().get(SUBWORKFLOW_ID_PARAM_NAME).asString();
        String subworkflowVersion =
            runtimeSummary.getParams().get(SUBWORKFLOW_VERSION_PARAM_NAME).asString();

        // always reset runRequest to be START_FRESH_NEW_RUN as this is the first run
        runRequest.clearRestartFor(RunPolicy.START_FRESH_NEW_RUN);
        runResponse = actionHandler.start(subworkflowId, subworkflowVersion, runRequest);
        if (runResponse.getStatus() == RunResponse.Status.DUPLICATED) { // deduplication
          WorkflowInstance subworkflowInstance =
              instanceDao.getWorkflowInstanceRunByUuid(
                  subworkflowId, runResponse.getWorkflowUuid());
          runResponse = RunResponse.from(subworkflowInstance, 1);
        }
        LOG.info(
            "In step runtime {}{}, starting a subworkflow instance {}",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity(),
            runResponse);
      }

      SubworkflowArtifact artifact = new SubworkflowArtifact();
      artifact.setSubworkflowId(runResponse.getWorkflowId());
      artifact.setSubworkflowVersionId(runResponse.getWorkflowVersionId());
      artifact.setSubworkflowInstanceId(runResponse.getWorkflowInstanceId());
      artifact.setSubworkflowRunId(runResponse.getWorkflowRunId());
      artifact.setSubworkflowUuid(runResponse.getWorkflowUuid());
      return new Result(
          State.CONTINUE,
          Collections.singletonMap(artifact.getType().key(), artifact),
          Collections.singletonList(
              TimelineLogEvent.info(
                  "Started a subworkflow with uuid: " + runResponse.getWorkflowUuid())));
    } catch (Exception e) {
      instanceStepConcurrencyHandler.removeInstance(
          runRequest.getCorrelationId(),
          runRequest.getInitiator().getDepth(),
          runRequest.getRequestId().toString());
      throw e;
    }
  }

  private Map<String, ParamDefinition> createSubworkflowRunParam(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    Map<String, ParamDefinition> runParams = new LinkedHashMap<>();

    if (!ObjectHelper.valueOrDefault(
        ((SubworkflowStep) step).getExplicitParams(), Defaults.DEFAULT_SUBWORKFLOW_EXPLICIT_FLAG)) {
      // pass all (including users and system defined) workflow level instance params to subworkflow
      workflowSummary.getParams().forEach((k, v) -> runParams.put(k, v.toDefinition()));
    } else {
      // only put all alwaysPassDownParamNames into run params
      alwaysPassDownParamNames.forEach(
          name -> {
            Parameter param = workflowSummary.getParams().get(name);
            if (param != null) {
              runParams.put(name, param.toDefinition());
            }
          });
    }
    // all user defined params in the step definition will be passed to subworkflow
    step.getParams()
        .forEach((k, v) -> runParams.put(k, runtimeSummary.getParams().get(k).toDefinition()));
    // remove subworkflow related params
    runParams.remove(SUBWORKFLOW_ID_PARAM_NAME);
    runParams.remove(SUBWORKFLOW_VERSION_PARAM_NAME);
    return runParams;
  }

  private Result trackSubworkflowInstance(Step step, StepRuntimeSummary runtimeSummary) {
    if (ObjectHelper.valueOrDefault(
        ((SubworkflowStep) step).getSync(), Defaults.DEFAULT_SUBWORKFLOW_SYNC_FLAG)) {
      SubworkflowArtifact artifact =
          runtimeSummary.getArtifacts().get(Artifact.Type.SUBWORKFLOW.key()).asSubworkflow();
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
          state = State.FATAL_ERROR; // no retry for subworkflow
          break;
        case STOPPED:
          state = State.STOPPED;
          break;
        case TIMED_OUT:
          state = State.TIMED_OUT;
          break;
        default:
          throw new MaestroInternalError(
              "Invalid status: %s for subworkflow step %s%s",
              instance.getStatus(), instance.getIdentity(), runtimeSummary.getIdentity());
      }

      artifact.setSubworkflowOverview(instance.getRuntimeOverview());

      TimelineEvent timelineEvent = null;
      if (instance.getStatus().isTerminal()) {
        timelineEvent =
            TimelineLogEvent.info(
                "Step is in %s status because its subworkflow instance is in %s status",
                state, instance.getStatus());
      }

      return new Result(
          state,
          Collections.singletonMap(artifact.getType().key(), artifact),
          timelineEvent == null
              ? Collections.emptyList()
              : Collections.singletonList(timelineEvent));
    } else {
      return new Result(State.DONE, Collections.emptyMap(), Collections.emptyList());
    }
  }

  /**
   * Terminate subworkflow instance. No action is needed here and just monitor if subworkflow
   * instance is stopped. The action related to termination has already been written to DB. The
   * final step instance termination callback will do the cleanup, including the step action
   * deletion.
   */
  @Override
  public Result terminate(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    try {
      if (runtimeSummary.getArtifacts().containsKey(Artifact.Type.SUBWORKFLOW.key())) {
        SubworkflowArtifact artifact =
            runtimeSummary.getArtifacts().get(Artifact.Type.SUBWORKFLOW.key()).asSubworkflow();

        WorkflowInstance.Status status =
            instanceDao.getWorkflowInstanceStatus(
                artifact.getSubworkflowId(),
                artifact.getSubworkflowInstanceId(),
                artifact.getSubworkflowRunId());

        if (status == null) {
          LOG.warn(
              "{}{} is terminating a not-found subworkflow {}, which might be deleted.",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity(),
              artifact.getIdentity());
          return new Result(
              State.STOPPED,
              Collections.emptyMap(),
              Collections.singletonList(
                  TimelineLogEvent.warn(
                      "Cannot find Subworkflow instance (might be deleted): "
                          + artifact.getIdentity())));
        }

        LOG.debug(
            "{}{} is terminating the subworkflow instance {} in a status [{}]",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity(),
            artifact.getIdentity(),
            status);

        if (!status.isTerminal()) {
          tryTerminateQueuedInstanceIfNeeded(artifact, status);
          wakeUpUnderlyingActor(workflowSummary);
          throw new MaestroRetryableError(
              "Termination at subworkflow step %s%s is not done and will retry it.",
              workflowSummary.getIdentity(), runtimeSummary.getIdentity());
        }

        // update with the final subworkflow runtime overview
        WorkflowInstance instance = getWorkflowInstance(artifact);
        artifact.setSubworkflowOverview(instance.getRuntimeOverview());
        return new Result(
            State.STOPPED,
            Collections.singletonMap(artifact.getType().key(), artifact),
            Collections.singletonList(
                TimelineLogEvent.info(
                    "Terminated the running subworkflow instance: " + artifact.getIdentity())));
      } else {
        LOG.debug(
            "Subworkflow step {}{} haven't start a subworkflow instance and do nothing.",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity());
      }
    } catch (MaestroNotFoundException e) {
      LOG.warn("Ignore termination as workflow instance is not stoppable due to", e);
    }
    return Result.of(State.STOPPED);
  }

  /** If the subworkflow instance is queued, then terminate it now. */
  private void tryTerminateQueuedInstanceIfNeeded(
      SubworkflowArtifact artifact, WorkflowInstance.Status status) {
    if (status == WorkflowInstance.Status.CREATED || status == WorkflowInstance.Status.PAUSED) {
      WorkflowInstance toTerminate = getWorkflowInstance(artifact);
      instanceDao.tryTerminateQueuedInstance(
          toTerminate,
          WorkflowInstance.Status.STOPPED,
          "The queued workflow instance is terminated by its upstream subworkflow step.");
    }
  }

  private WorkflowInstance getWorkflowInstance(SubworkflowArtifact artifact) {
    return instanceDao.getWorkflowInstanceRun(
        artifact.getSubworkflowId(),
        artifact.getSubworkflowInstanceId(),
        artifact.getSubworkflowRunId());
  }

  private void wakeUpUnderlyingActor(WorkflowSummary summary) {
    var msg =
        MessageDto.createMessageForWakeUp(
            summary.getWorkflowId(),
            summary.getGroupInfo(),
            Map.of(summary.getWorkflowInstanceId(), summary.getWorkflowRunId()));
    queueSystem.notify(msg);
  }
}
