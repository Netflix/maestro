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
package com.netflix.maestro.engine.handlers;

import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroDryRunException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.WorkflowActionResponse;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.TriggerInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineActionEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.IdHelper;
import com.netflix.maestro.utils.MapHelper;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Workflow actor to implement all workflow actions. */
@Slf4j
@AllArgsConstructor
public class WorkflowActionHandler {
  private final MaestroWorkflowDao workflowDao;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroRunStrategyDao runStrategyDao;
  private final DryRunValidator dryRunValidator;
  private final WorkflowHelper workflowHelper;

  /**
   * Start a workflow instance run. It first created a workflow instance in DB and then put it in a
   * queue to check its run strategy. After run strategy check, the workflow instance then gets
   * launched.
   *
   * @param workflowId workflow id
   * @param version workflow version, `active`, `latest`, or exact version id
   * @param runRequest run request with start configs and overrides
   * @return the response
   */
  public RunResponse start(
      @NotNull String workflowId, @NotNull String version, @NotNull RunRequest runRequest) {
    WorkflowDefinition definition = workflowDao.getWorkflowDefinition(workflowId, version);

    validateRequest(version, definition, runRequest);

    RunProperties runProperties =
        RunProperties.from(
            Checks.notNull(
                definition.getPropertiesSnapshot(),
                "property snapshot cannot be null for workflow: " + workflowId));
    // create and initiate a new instance with overrides and param evaluation
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(
            definition.getWorkflow(),
            definition.getInternalId(),
            definition.getMetadata().getWorkflowVersionId(),
            runProperties,
            runRequest);
    RunStrategy runStrategy = definition.getRunStrategyOrDefault();
    int ret = runStrategyDao.startWithRunStrategy(instance, runStrategy);
    RunResponse response = RunResponse.from(instance, ret);
    LOG.info("Created a workflow instance with response {}", response);
    return response;
  }

  private void validateRequest(
      String version, @NotNull WorkflowDefinition definition, @NotNull RunRequest request) {
    if (request.getInitiator() instanceof TriggerInitiator) {
      Checks.checkTrue(
          definition.getIsActive(),
          "Triggered workflow definition for workflow [%s][%s] must be active.",
          definition.getWorkflow().getId(),
          version);
      if (!((TriggerInitiator) request.getInitiator()).isValid(definition.getTriggerUuids())) {
        throw new MaestroResourceConflictException(
            "Invalid trigger initiator due to mismatch trigger uuid.");
      }
      if ((request.getInitiator().getType() == Initiator.Type.SIGNAL
              && ObjectHelper.valueOrDefault(
                  definition.getPropertiesSnapshot().getSignalTriggerDisabled(), Boolean.FALSE))
          || (request.getInitiator().getType() == Initiator.Type.TIME
              && ObjectHelper.valueOrDefault(
                  definition.getPropertiesSnapshot().getTimeTriggerDisabled(), Boolean.FALSE))) {
        throw new MaestroUnprocessableEntityException(
            "Trigger type [%s] is disabled for the workflow [%s] in the workflow properties.",
            request.getInitiator().getType(), definition.getWorkflow().getId());
      }
    }
  }

  /**
   * Start a batch of new workflow instance runs (i.e. with run_id = 1) asynchronous. It first
   * created workflow instances in DB and then send a job event to run strategy poller, which checks
   * and start queued workflow instances if possible. Note that request list size must fit into a
   * single DB transaction batch limit defined by {@link Constants#START_BATCH_LIMIT}. It will
   * handle the case with duplicate uuids.
   *
   * @param workflowId workflow id
   * @param version workflow version, `active`, `latest`, or exact version id
   * @param requests a list of start requests with start configs and overrides
   * @return the response
   */
  public List<RunResponse> startBatch(
      String workflowId, String version, List<RunRequest> requests) {
    if (ObjectHelper.isCollectionEmptyOrNull(requests)) {
      return Collections.emptyList();
    }
    Checks.checkTrue(
        requests.size() <= Constants.START_BATCH_LIMIT,
        "The size of Requests is greater than the batch limit");
    WorkflowDefinition definition = workflowDao.getWorkflowDefinition(workflowId, version);

    // Fail the whole batch if any request is invalid
    requests.forEach(request -> validateRequest(version, definition, request));

    RunProperties runProperties = RunProperties.from(definition.getPropertiesSnapshot());

    List<WorkflowInstance> instances =
        createWorkflowInstances(
            definition.getWorkflow(),
            definition.getInternalId(),
            definition.getMetadata().getWorkflowVersionId(),
            runProperties,
            requests);
    RunStrategy runStrategy = definition.getRunStrategyOrDefault();
    int[] results = runStrategyDao.startBatchWithRunStrategy(workflowId, runStrategy, instances);
    List<RunResponse> responses = new ArrayList<>();
    int idx = 0;
    for (WorkflowInstance instance : instances) {
      responses.add(RunResponse.from(instance, results[idx]));
      idx++;
    }
    LOG.debug(
        "Created {} of workflow instances for workflow id {} to start",
        requests.size(),
        workflowId);
    return responses;
  }

  /**
   * Run a batch of foreach workflow instances for a given workflow and version. The request list
   * has already been sized by insertBatchLimit in StepRuntimeProperties.Foreach. Note that batch
   * start only supports a fresh new run (run_id=1). Additionally, the instance ids have already
   * been decided to avoid race condition and ensure idempotency. It will bypass run strategy
   * manager as foreach manages its own inline workflow instances.
   *
   * @return the status of start foreach workflow instances.
   */
  public Optional<Details> runForeachBatch(
      Workflow workflow,
      Long internalId,
      long workflowVersionId,
      RunProperties runProperties,
      String foreachStepId,
      ForeachArtifact artifact,
      List<RunRequest> requests,
      List<Long> instanceIds) {
    if (ObjectHelper.isCollectionEmptyOrNull(requests)) {
      return Optional.empty();
    }
    Checks.checkTrue(
        requests.size() == instanceIds.size(),
        "Run request list size [%s] must match instance id list size [%s]",
        requests.size(),
        instanceIds.size());
    List<WorkflowInstance> instances;
    if (artifact.isFreshRun()) {
      instances =
          createStartForeachInstances(
              workflow,
              internalId,
              workflowVersionId,
              artifact.getForeachRunId(),
              runProperties,
              requests,
              instanceIds);
    } else {
      instances =
          createRestartForeachInstances(
              workflow,
              internalId,
              workflowVersionId,
              runProperties,
              foreachStepId,
              artifact,
              requests,
              instanceIds);
    }
    if (ObjectHelper.isCollectionEmptyOrNull(instances)) {
      return Optional.empty();
    }
    return instanceDao.runWorkflowInstances(workflow.getId(), instances);
  }

  private List<WorkflowInstance> createStartForeachInstances(
      Workflow workflow,
      Long internalId,
      long workflowVersionId,
      long workflowRunId,
      RunProperties runProperties,
      List<RunRequest> requests,
      List<Long> instanceIds) {
    List<WorkflowInstance> instances =
        createWorkflowInstances(workflow, internalId, workflowVersionId, runProperties, requests);

    Iterator<Long> instanceId = instanceIds.iterator();
    for (WorkflowInstance instance : instances) {
      instance.setWorkflowInstanceId(instanceId.next());
      instance.setWorkflowRunId(workflowRunId);
    }
    return instances;
  }

  private List<WorkflowInstance> createRestartForeachInstances(
      Workflow workflow,
      Long internalId,
      long workflowVersionId,
      RunProperties runProperties,
      String foreachStepId,
      ForeachArtifact artifact,
      List<RunRequest> requests,
      List<Long> instanceIds) {
    long totalAncestorIterations =
        ObjectHelper.valueOrDefault(artifact.getAncestorIterationCount(), 0L);
    List<WorkflowInstance> instances = new ArrayList<>();

    Iterator<Long> instanceIditerator = instanceIds.iterator();
    for (RunRequest request : requests) {

      long instanceId = instanceIditerator.next();
      if (instanceId > totalAncestorIterations) {
        RunRequest.RunRequestBuilder requestBuilder =
            request.toBuilder().currentPolicy(RunPolicy.RESTART_FROM_BEGINNING);
        if (!isRestartFromInlineRootMode(request, workflow)) {
          requestBuilder.restartConfig(null);
        }
        WorkflowInstance instance =
            workflowHelper.createWorkflowInstance(
                workflow.toBuilder().build(),
                internalId,
                workflowVersionId,
                runProperties,
                requestBuilder.build());
        instance.setWorkflowInstanceId(instanceId);
        instance.setWorkflowRunId(artifact.getForeachRunId());
        instances.add(instance);
      } else {
        WorkflowInstance instance =
            instanceDao.getLatestWorkflowInstanceRun(workflow.getId(), instanceId);
        if (!isRestartFromInlineRootMode(request, workflow)) {
          request.updateForDownstreamIfNeeded(foreachStepId, instance);
        }
        workflowHelper.updateWorkflowInstance(instance, request);
        instance.setWorkflowRunId(artifact.getForeachRunId());
        instances.add(instance);
      }
    }
    return instances;
  }

  public Optional<Details> restartForeachInstance(
      RunRequest request, WorkflowInstance instance, String foreachStepId, long restartRunId) {
    request.updateForDownstreamIfNeeded(foreachStepId, instance);
    workflowHelper.updateWorkflowInstance(instance, request);
    instance.setWorkflowRunId(restartRunId);
    return instanceDao.runWorkflowInstances(
        instance.getWorkflowId(), Collections.singletonList(instance));
  }

  private boolean isRestartFromInlineRootMode(RunRequest request, Workflow workflow) {
    if (request.getInitiator().getType() == Initiator.Type.FOREACH
        && request.getRestartConfig() != null
        && request.getRestartConfig().getRestartPath() != null
        && request.getRestartConfig().getRestartPath().size() == 1
        && request.getRestartConfig().getRestartPath().getFirst().getStepId() == null
        && !MapHelper.isEmptyOrNull(request.getRestartConfig().getStepRestartParams())) {
      UpstreamInitiator.Info info =
          ((ForeachInitiator) request.getInitiator()).getNonInlineParent();
      if (info.getWorkflowId().equals(request.getRestartWorkflowId())
          && info.getInstanceId() == request.getRestartInstanceId()) {
        Set<String> stepIds = request.getRestartConfig().getStepRestartParams().keySet();
        return workflow.getAllStepIds().stream().anyMatch(stepIds::contains);
      }
    }
    return false;
  }

  private List<WorkflowInstance> createWorkflowInstances(
      Workflow workflow,
      Long internalId,
      long workflowVersionId,
      RunProperties runProperties,
      List<RunRequest> requests) {
    return requests.stream()
        .map(
            request ->
                workflowHelper.createWorkflowInstance(
                    workflow.toBuilder().build(),
                    internalId,
                    workflowVersionId,
                    runProperties,
                    request))
        .collect(Collectors.toList());
  }

  /**
   * Validate a workflow definition.
   *
   * @param request workflow create request to validate
   */
  public void validate(WorkflowCreateRequest request, User caller) {
    LOG.debug(
        "validating workflow [{}] for user [{}]", request.getWorkflow().getId(), caller.getName());
    List<String> errors = new ArrayList<>();
    if (request.getWorkflow() == null) {
      errors.add("workflow cannot be null");
    }
    // definition data will be checked by dryRunValidator
    try {
      dryRunValidator.validate(request.getWorkflow(), caller);
    } catch (MaestroDryRunException e) {
      errors.add(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
    }
    if (!errors.isEmpty()) {
      throw new MaestroBadRequestException(errors, "Invalid workflow create request");
    }
  }

  /**
   * Stop all current running workflow instances for the given workflow id.
   *
   * @param workflowId workflow id
   * @return the final timeline log message
   */
  public TimelineEvent stop(String workflowId, User caller) {
    return terminate(workflowId, Actions.WorkflowInstanceAction.STOP, caller);
  }

  /**
   * Kill/fail all current running workflow instances for the given workflow id.
   *
   * @param workflowId workflow id
   * @return the final timeline log message
   */
  public TimelineEvent kill(String workflowId, User caller) {
    return terminate(workflowId, Actions.WorkflowInstanceAction.KILL, caller);
  }

  private TimelineEvent terminate(
      String workflowId, Actions.WorkflowInstanceAction action, User caller) {
    String reason =
        String.format(
            "All workflow instances of workflow id [%s] are terminated to status [%s] by the caller [%s]",
            workflowId, action.getStatus(), caller.getName());
    int stopped = Constants.TERMINATE_BATCH_LIMIT;
    int stoppedQueued = 0;
    while (stopped == Constants.TERMINATE_BATCH_LIMIT) {
      stopped =
          instanceDao.terminateQueuedInstances(
              workflowId, Constants.TERMINATE_BATCH_LIMIT, action.getStatus(), reason);
      stoppedQueued += stopped;
    }

    int stoppedRunning =
        instanceDao.terminateRunningInstances(
            workflowId, Constants.TERMINATE_BATCH_LIMIT, action, caller, reason);

    LOG.info(
        "Terminated [{}] queued instances and terminating [{}] running instances with reason [{}]",
        stoppedQueued,
        stoppedRunning,
        reason);

    return TimelineActionEvent.builder()
        .action(action)
        .author(caller)
        .message(
            "Terminated [%s] queued instances and terminating [%s] running instances",
            stoppedQueued, stoppedRunning)
        .reason(reason)
        .build();
  }

  /**
   * Activate this workflow version.
   *
   * @param workflowId workflow id
   * @param version workflow version (i.e. active, latest, default, or exact version id)
   * @param caller the user making the call
   */
  public WorkflowActionResponse activate(String workflowId, String version, User caller) {
    Checks.notNull(
        caller, "caller cannot be null to activate workflow [%s][%s]", workflowId, version);
    WorkflowVersionUpdateJobEvent jobEvent =
        (WorkflowVersionUpdateJobEvent) workflowDao.activate(workflowId, version, caller);
    LOG.info(jobEvent.getLog());
    TimelineEvent event =
        TimelineActionEvent.builder()
            .action(Actions.WorkflowAction.ACTIVATE)
            .author(caller)
            .message(jobEvent.getLog())
            .build();
    return WorkflowActionResponse.from(workflowId, jobEvent.getCurrentActiveVersion(), event);
  }

  /**
   * Deactivate the workflow with a provided workflow id.
   *
   * @param workflowId workflow id to deactivate
   * @param caller the caller
   * @return action response
   */
  public WorkflowActionResponse deactivate(String workflowId, User caller) {
    Checks.notNull(caller, "caller cannot be null to deactivate workflow [%s]", workflowId);
    String timeline = workflowDao.deactivate(workflowId, caller);
    LOG.info(timeline);
    TimelineEvent event =
        TimelineActionEvent.builder()
            .action(Actions.WorkflowAction.DEACTIVATE)
            .author(caller)
            .message(timeline)
            .build();
    return WorkflowActionResponse.from(workflowId, event);
  }

  /**
   * Unblock the failed workflow instances with a provided workflow id.
   *
   * <p>todo: this might not work well if there are millions of failed instances. If needed, we
   * should rewrite it to keep a column in maestro_workflow to track it than updating the status of
   * each failed instance.
   *
   * @param workflowId workflow id
   */
  public TimelineEvent unblock(String workflowId, User caller) {
    TimelineActionEvent.TimelineActionEventBuilder eventBuilder =
        TimelineActionEvent.builder().author(caller).reason("Unblock workflow [%s]", workflowId);
    if (IdHelper.isInlineWorkflowId(workflowId)) {
      return eventBuilder.message("Unblocked the workflow.").build();
    }
    int totalUnblocked =
        instanceDao.tryUnblockFailedWorkflowInstances(
            workflowId, Constants.UNBLOCK_BATCH_SIZE, eventBuilder.build());
    return eventBuilder
        .message("Unblocked [%s] failed workflow instances.", totalUnblocked)
        .build();
  }
}
