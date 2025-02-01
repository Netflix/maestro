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
package com.netflix.maestro.engine.utils;

import com.netflix.maestro.engine.eval.InstanceWrapper;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.DurationParser;
import com.netflix.maestro.utils.IdHelper;
import com.netflix.maestro.utils.MapHelper;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;

/** Workflow Helper class. */
@AllArgsConstructor
public class WorkflowHelper {
  private final ParamsManager paramsManager;
  private final ParamEvaluator paramEvaluator;
  private final DagTranslator dagTranslator;
  private final MaestroParamExtensionRepo paramExtensionRepo;
  private final MaestroJobEventPublisher publisher;
  private final long maxGroupNum;

  /**
   * Returns workflow id if workflow name is missing and was not provided by the user.
   *
   * @param workflow workflow definition
   * @return workflow name
   */
  public static String getWorkflowNameOrDefault(Workflow workflow) {
    return workflow.getName() != null ? workflow.getName() : workflow.getId();
  }

  /**
   * Create a workflow instance based on the workflow info, e.g. definition, run request, etc.
   *
   * @param workflowDef versioned workflow definition
   * @param internalId internal unique id
   * @param workflowVersionId workflow version id
   * @param runProperties run properties extracted from workflow properties
   * @param runRequest run request with start configs and overrides
   * @return new workflow instance
   */
  public WorkflowInstance createWorkflowInstance(
      Workflow workflowDef,
      Long internalId,
      long workflowVersionId,
      RunProperties runProperties,
      RunRequest runRequest) {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId(workflowDef.getId());
    instance.setInternalId(internalId);
    instance.setWorkflowVersionId(workflowVersionId);
    // latest workflow instance id is unknown, update it later.
    instance.setWorkflowInstanceId(Constants.LATEST_ONE);
    // set correlation id if request contains it, otherwise, update it later inside DAO
    instance.setCorrelationId(runRequest.getCorrelationId());
    instance.setRunProperties(runProperties);
    // set the current max group num for the fresh new workflow instance
    instance.setMaxGroupNum(ObjectHelper.valueOrDefault(runRequest.getMaxGroupNum(), maxGroupNum));
    // it includes runtime params and tags. Its dag is versioned dag.
    Workflow workflow = overrideWorkflowConfig(workflowDef, runRequest);
    instance.setRuntimeWorkflow(workflow);

    // update newly created workflow instance
    updateWorkflowInstance(instance, runRequest);
    return instance;
  }

  /**
   * Update workflow instance for workflow start and restart cases. It does the param evaluation,
   * string interpolation for some fields. It won't update workflow id, version id, correlation id,
   * run properties, runtime workflow. Other than those, it will set all other fields in a workflow
   * instance.
   *
   * <p>same workflow id, version id, run properties, runtime workflow, params, correlation id
   *
   * @param instance a new workflow instance to start or a baseline workflow instance to restart
   * @param runRequest run request to start or restart a workflow instance
   */
  public void updateWorkflowInstance(WorkflowInstance instance, RunRequest runRequest) {
    if (!runRequest.isFreshRun() && instance.getRuntimeDag() != null) {
      // For restart, set the baseline aggregatedInfo using the previous run aggregated info.
      instance.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(instance, true));
    }
    // set run id to the latest but unknown, which will be set later
    instance.setWorkflowRunId(Constants.LATEST_ONE);
    instance.setWorkflowUuid(IdHelper.getOrCreateUuid(runRequest.getRequestId()));
    instance.setExecutionId(null); // clear execution id if set
    instance.setRunConfig(runRequest.toRunConfig());
    instance.setRunParams(runRequest.getRunParams());
    instance.setStepRunParams(runRequest.getStepRunParams());
    instance.setInitiator(runRequest.getInitiator());
    instance.setStatus(WorkflowInstance.Status.CREATED);
    instance.setRequestTime(runRequest.getRequestTime());
    // create time will be set within transaction to ensure the same order as instance id.
    // not set startTime and endTime, modified time will be set by DB automatically
    instance.setRuntimeOverview(null); // always null before running
    instance.setArtifacts(runRequest.getArtifacts());
    // set runtime dag
    instance.setRuntimeDag(dagTranslator.translate(instance));
    // validate run params
    if (runRequest.getStepRunParams() != null) {
      Checks.checkTrue(
          runRequest.getStepRunParams().keySet().stream()
              .allMatch(instance.getRuntimeDag()::containsKey),
          "non-existing step id detected in step param overrides: inputs %s vs dag %s",
          runRequest.getStepRunParams().keySet(),
          instance.getRuntimeDag().keySet());
    }
    // set initial timeline
    instance.setTimeline(
        new Timeline(Collections.singletonList(runRequest.getInitiator().getTimelineEvent())));

    try {
      initiateWorkflowParamsAndProperties(instance, runRequest);
    } catch (RuntimeException e) {
      if (runRequest.isPersistFailedRun()) {
        instance.setStatus(WorkflowInstance.Status.FAILED);
        instance
            .getTimeline()
            .add(
                TimelineDetailsEvent.from(
                    Details.create(e, false, "Failed to initiate workflow params and properties")));
      } else {
        throw e;
      }
    }
  }

  /**
   * Create a workflow summary for the workflow instance.
   *
   * @param instance the workflow instance to be used to build summary
   * @return the workflow summary
   */
  public WorkflowSummary createWorkflowSummaryFromInstance(WorkflowInstance instance) {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId(instance.getWorkflowId());
    summary.setInternalId(instance.getInternalId());
    summary.setWorkflowVersionId(instance.getWorkflowVersionId());
    summary.setWorkflowName(getWorkflowNameOrDefault(instance.getRuntimeWorkflow()));
    summary.setWorkflowInstanceId(instance.getWorkflowInstanceId());
    summary.setCreationTime(instance.getCreateTime());
    summary.setWorkflowRunId(instance.getWorkflowRunId());
    summary.setCorrelationId(instance.getCorrelationId());
    summary.setMaxGroupNum(instance.getMaxGroupNum());
    summary.setWorkflowUuid(instance.getWorkflowUuid());
    if (instance.getRunConfig() != null) {
      summary.setRunPolicy(instance.getRunConfig().getPolicy());
      summary.setRestartConfig(instance.getRunConfig().getRestartConfig());
    }
    summary.setRunProperties(instance.getRunProperties());
    summary.setInitiator(instance.getInitiator());
    summary.setParams(instance.getParams());
    summary.setStepRunParams(instance.getStepRunParams());
    summary.setTags(instance.getRuntimeWorkflow().getTags());
    summary.setRuntimeDag(instance.getRuntimeDag());
    summary.setStepMap(
        instance.getRuntimeWorkflow().getSteps().stream()
            .collect(MapHelper.toListMap(Step::getId, s -> s)));
    summary.setCriticality(instance.getRuntimeWorkflow().getCriticality());
    summary.setInstanceStepConcurrency(instance.getRuntimeWorkflow().getInstanceStepConcurrency());
    return summary;
  }

  /** Override the baseline workflow definition with run request. */
  private Workflow overrideWorkflowConfig(Workflow workflow, RunRequest runRequest) {
    Workflow.WorkflowBuilder builder = workflow.toBuilder();
    if (runRequest.getRuntimeTags() != null) {
      TagList tags = new TagList(null);
      if (workflow.getTags() != null) {
        tags.merge(workflow.getTags().getTags());
      }
      tags.merge(runRequest.getRuntimeTags());
      builder.tags(tags);
    }
    // both cases always inherit upstream instance_step_concurrency settings
    if (runRequest.isSystemInitiatedRun() || runRequest.getInstanceStepConcurrency() != null) {
      builder.instanceStepConcurrency(runRequest.getInstanceStepConcurrency());
    }
    return builder.build();
  }

  // set evaluated param results and run properties.
  private void initiateWorkflowParamsAndProperties(WorkflowInstance instance, RunRequest request) {
    // evaluate workflow params
    Map<String, Parameter> allParams =
        paramsManager.generateMergedWorkflowParams(instance, request);
    paramExtensionRepo.reset(
        Collections.emptyMap(), Collections.emptyMap(), InstanceWrapper.from(instance, request));
    // evaluate wf params during start and restart
    paramEvaluator.evaluateWorkflowParameters(allParams, instance.getWorkflowId());
    paramExtensionRepo.clear();

    // update run properties with string interpolation support
    Workflow workflow = instance.getRuntimeWorkflow();
    RunProperties runProperties = instance.getRunProperties();
    if (runProperties != null && runProperties.getAlerting() != null) {
      runProperties
          .getAlerting()
          .update(p -> paramEvaluator.parseAttribute(p, allParams, workflow.getId(), true));
    }
    if (workflow.getTimeout() != null) { // set parsed timeout
      long timeout =
          DurationParser.getDurationWithParamInMillis(
              workflow.getTimeout(),
              p -> paramEvaluator.parseAttribute(p, allParams, workflow.getId(), false));
      instance.setTimeoutInMillis(timeout);
    }

    instance.setParams(allParams);
  }

  /**
   * Helper method to publish a start workflow event for a given workflow if flag is true and
   * workflow id is valid and is not a foreach inline workflow.
   */
  public void publishStartWorkflowEvent(String workflowId, boolean flag) {
    if (flag
        && workflowId != null
        && !workflowId.isEmpty()
        && !IdHelper.isInlineWorkflowId(workflowId)) {
      publisher.publishOrThrow(StartWorkflowJobEvent.create(workflowId));
    }
  }
}
