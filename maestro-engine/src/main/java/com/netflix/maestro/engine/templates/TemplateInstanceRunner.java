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

import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.IdHelper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 * Launches the inline workflow instances of a template step: the prepare instance of a step
 * preparer and the execution instance. Both instances share the artifact's inline workflow id and
 * run id and differ by instance id. The launch bypasses the run strategy manager, as the template
 * step manages its own instances.
 */
@AllArgsConstructor
public class TemplateInstanceRunner {
  private static final String TEMPLATE_TAG_NAME = Constants.TEMPLATE_INLINE_WORKFLOW_PREFIX;

  private final WorkflowActionHandler actionHandler;
  private final InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;

  /**
   * Build the inline workflow definition for a template step. It has no params of its own; every
   * param reaches the instance through the run params.
   *
   * @param templateWorkflowId the inline workflow id from the artifact
   * @param templateIdentity the parent workflow and template step identity
   * @param step the template step definition
   * @param steps the steps to run
   * @return the inline workflow definition
   */
  public static Workflow createInlineWorkflow(
      String templateWorkflowId, String templateIdentity, Step step, List<Step> steps) {
    return Workflow.builder()
        .id(templateWorkflowId)
        .name(templateIdentity)
        .description("Maestro template inline workflow created by " + templateIdentity)
        .tags(step.getTags())
        .params(Collections.emptyMap())
        .steps(steps)
        .build();
  }

  /**
   * Launch an inline instance. The instance id is the caller's choice and the run id comes from the
   * artifact. With {@code restartPrevious} the previous run of the same instance is restarted under
   * the parent's run policy; otherwise a fresh run is created regardless of the parent's policy.
   *
   * @param workflowSummary parent workflow summary
   * @param runtimeSummary template step runtime summary
   * @param step template step definition
   * @param artifact template artifact of the current run
   * @param instanceId inline instance id to launch
   * @param inlineWorkflow inline workflow definition
   * @param runParams run params for the inline instance
   * @param restartPrevious true to restart the previous run of this instance
   * @return empty when the instance is launched, otherwise the reason to retry later
   */
  public Optional<Details> run(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      Step step,
      TemplateArtifact artifact,
      long instanceId,
      Workflow inlineWorkflow,
      Map<String, ParamDefinition> runParams,
      boolean restartPrevious) {
    RunRequest runRequest =
        StepHelper.createInternalWorkflowRunRequest(
            workflowSummary,
            runtimeSummary,
            Collections.singletonList(Tag.create(TEMPLATE_TAG_NAME)),
            runParams,
            dedupKey(artifact, instanceId),
            null);
    if (!restartPrevious) {
      runRequest.clearRestartFor(RunPolicy.START_FRESH_NEW_RUN);
    }
    if (artifact.getTemplateInstanceId() != null
        && artifact.getTemplateInstanceId() == instanceId) {
      artifact.setTemplateUuid(IdHelper.getOrCreateUuid(runRequest.getRequestId()));
    }

    if (!instanceStepConcurrencyHandler.addInstance(runRequest)) {
      return Optional.of(
          Details.create(
              "Unavailable due to InstanceStepConcurrency and will retry later to launch "
                  + "the template inline workflow instance [%s][%s][%s]",
              artifact.getTemplateWorkflowId(), instanceId, artifact.getTemplateRunId()));
    } // no need to release if exception

    return actionHandler.runTemplateInstance(
        inlineWorkflow,
        workflowSummary.getInternalId(), // inherit parent unique internalId
        workflowSummary.getWorkflowVersionId(), // inherit parent versionId
        workflowSummary.getRunProperties(), // inherit parent run properties
        step.getId(),
        instanceId,
        artifact.getTemplateRunId(),
        runRequest);
  }

  private static String dedupKey(TemplateArtifact artifact, long instanceId) {
    return String.format(
        "[%s][%s][%s]", artifact.getTemplateWorkflowId(), instanceId, artifact.getTemplateRunId());
  }
}
