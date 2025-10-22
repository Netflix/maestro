/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.engine.stepruntime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.kubernetes.KubernetesCommandGenerator;
import com.netflix.maestro.engine.kubernetes.KubernetesJobResult;
import com.netflix.maestro.engine.kubernetes.KubernetesRuntimeExecutor;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.KubernetesArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Kubernetes step runtime. */
@Slf4j
@AllArgsConstructor
public class KubernetesStepRuntime implements StepRuntime {
  private final KubernetesRuntimeExecutor runtimeExecutor;
  private final KubernetesCommandGenerator commandGenerator;
  private final JobTemplateManager jobTemplateManager;
  private final OutputDataManager outputDataManager;
  private final ObjectMapper objectMapper;
  private final MaestroMetrics metrics;

  @Override
  public Result start(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    KubernetesStepContext context =
        new KubernetesStepContext(workflowSummary, runtimeSummary, step);
    try {
      context.setCommand(commandGenerator.generate(context));
      customizePreLaunchCommand(context);

      KubernetesJobResult jobResult = runtimeExecutor.launchJob(context);
      LOG.info("Launched a job with the result: [{}]", jobResult);
      metrics.counter(
          MetricConstants.STEP_LAUNCHED_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          "launch",
          MetricConstants.STATUS_TAG,
          MetricConstants.STATUS_TAG_VALUE_SUCCESS);
      context.setJobResult(jobResult);

      processPostLaunchArtifacts(context);
      return new Result(
          State.CONTINUE, context.getPendingArtifacts(), context.getPendingTimeline());
    } catch (MaestroBadRequestException e) {
      LOG.error(
          "Error starting Kubernetes job for workflow {}{} with command={}",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          context.getCommand(),
          e);
      metrics.counter(
          MetricConstants.STEP_LAUNCHED_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          "bad_request",
          MetricConstants.STATUS_TAG,
          MetricConstants.STATUS_TAG_VALUE_FAILURE);
      var timeline =
          TimelineDetailsEvent.from(
              Details.create(e, true, "Error starting KubernetesStepRuntime"));
      return new Result(
          State.PLATFORM_ERROR, Collections.emptyMap(), Collections.singletonList(timeline));
    }
  }

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    String jobId = getLaunchedJobId(runtimeSummary);
    if (jobId == null) {
      throw new MaestroUnprocessableEntityException(
          "Invalid: JobId is null for %s%s",
          workflowSummary.getIdentity(), runtimeSummary.getIdentity());
    }
    try {
      KubernetesJobResult jobResult = runtimeExecutor.checkJobStatus(jobId);
      KubernetesStepContext context =
          new KubernetesStepContext(workflowSummary, runtimeSummary, step);
      context.setJobResult(jobResult);
      StepRuntime.State jobState = customizeArtifactsForJobStatus(context);
      LOG.debug(
          "Current Job state=[{}] for job {}{}, jobId=[{}]",
          jobState,
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          jobId);
      return new Result(jobState, context.getPendingArtifacts(), context.getPendingTimeline());
    } catch (RuntimeException e) {
      LOG.warn(
          "Error executing Kubernetes job for {}{}, will retry",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
    } catch (IOException e) {
      LOG.error(
          "Error processing the data for {}{}, will fail the job",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      return new Result(
          State.USER_ERROR,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineDetailsEvent.from(
                  Details.create(
                      e, false, "Error processing the output data in KubernetesStepRuntime"))));
    }
    return new Result(State.CONTINUE, Collections.emptyMap(), Collections.emptyList());
  }

  @Override
  public Result terminate(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    String jobId = getLaunchedJobId(runtimeSummary);
    if (jobId != null) {
      KubernetesStepContext context =
          new KubernetesStepContext(workflowSummary, runtimeSummary, null);
      StepRuntime.State finalJobState;
      try {
        KubernetesJobResult jobResult = runtimeExecutor.checkJobStatus(jobId);
        // poll before terminate to see status in case job completed
        if (jobResult.jobStatus().isTerminal()) {
          LOG.info(
              "Terminate, found Kubernetes job already finished: {}{}, jobId=[{}] state=[{}]",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity(),
              jobId,
              jobResult.jobStatus());
          context
              .getPendingTimeline()
              .add(
                  TimelineLogEvent.info(
                      "Kubernetes Job completion detected during Terminate, jobId=" + jobId));
          finalJobState = jobResult.jobStatus();
        } else {
          LOG.info(
              "Terminating Kubernetes job for workflow due to runtime terminate called: {}{} jobId=[{}]",
              workflowSummary.getIdentity(),
              runtimeSummary.getIdentity(),
              jobId);
          runtimeExecutor.terminateJob(jobId);
          finalJobState = StepRuntime.State.STOPPED;
          context
              .getPendingTimeline()
              .add(TimelineLogEvent.info("Terminated Kubernetes job for jobId=" + jobId));
        }
      } catch (RuntimeException e) {
        throw new MaestroRetryableError(e, "Error terminating Kubernetes job for jobId=" + jobId);
      }

      LOG.info(
          "Final Job state=[{}] for job {}{}, jobId=[{}]",
          finalJobState,
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          jobId);
      return new Result(State.STOPPED, context.getPendingArtifacts(), context.getPendingTimeline());
    } else {
      return new Result(
          State.STOPPED,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineLogEvent.info("Job terminating, no kubernetes job found")));
    }
  }

  /** Hook method for customizing Kubernetes command. */
  protected void customizePreLaunchCommand(KubernetesStepContext context) {}

  /** Hook method for customizing artifacts after job is returned. */
  protected void processPostLaunchArtifacts(KubernetesStepContext context) {
    KubernetesArtifact artifact = new KubernetesArtifact();
    artifact.setJobId(context.getJobResult().jobId());
    artifact.setExecutionScript(context.getCommand().getEntrypoint());
    artifact.setCommand(context.getCommand());
    context.getPendingArtifacts().put(Artifact.Type.KUBERNETES.key(), artifact);
  }

  private String getLaunchedJobId(StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary.getArtifacts().containsKey(Artifact.Type.KUBERNETES.key())) {
      KubernetesArtifact artifact =
          runtimeSummary.getArtifacts().get(Artifact.Type.KUBERNETES.key()).asKubernetes();
      return artifact.getJobId();
    }
    return null;
  }

  private StepRuntime.State customizeArtifactsForJobStatus(KubernetesStepContext context)
      throws IOException {
    if (context.getJobResult().jobStatus().isTerminal()) {
      KubernetesArtifact artifact = context.getKubernetesArtifact();
      if (artifact != null) {
        // download logs
        artifact.setExecutionOutput(runtimeExecutor.getJobLog(artifact.getJobId()));
        context.getPendingArtifacts().put(Artifact.Type.KUBERNETES.key(), artifact);
        // download outputs
        String outputStr = runtimeExecutor.getJobOutput(artifact.getJobId());
        if (outputStr != null) {
          OutputData outputData = objectMapper.readValue(outputStr, OutputData.class);
          if (outputData.isNotEmpty()) {
            outputData.setExternalJobId(artifact.getJobId());
            outputData.setExternalJobType(context.getRuntimeSummary().getType());
            if (outputData.getCreateTime() == null) {
              outputData.setCreateTime(System.currentTimeMillis());
            }
            outputData.setModifyTime(System.currentTimeMillis());
            outputData.setWorkflowId(context.getWorkflowSummary().getWorkflowId());
            outputDataManager.saveOutputData(outputData);
          }
        }
      }
    }
    return context.getJobResult().jobStatus();
  }

  @Override
  public Map<String, ParamDefinition> injectRuntimeParams(
      WorkflowSummary workflowSummary, Step step) {
    String version = getJobTemplateVersion(workflowSummary, step);
    Map<String, ParamDefinition> allParams = jobTemplateManager.loadRuntimeParams(step, version);
    // merge workflow level params into template schema introduced params.
    jobTemplateManager.mergeWorkflowParamsIntoSchemaParams(allParams, workflowSummary.getParams());
    return Map.copyOf(allParams);
  }

  private String getJobTemplateVersion(WorkflowSummary workflowSummary, Step step) {
    // subtype version is used for job template versioning if present.
    String version = ((TypedStep) step).getSubTypeVersion();
    if (version == null
        && workflowSummary.getParams() != null
        && workflowSummary.getParams().containsKey(Constants.JOB_TEMPLATE_VERSION_PARAM)) {
      version =
          workflowSummary
              .getParams()
              .get(Constants.JOB_TEMPLATE_VERSION_PARAM)
              .getEvaluatedResultString();
    }
    if (version == null || version.isEmpty()) {
      version = Constants.DEFAULT_JOB_TEMPLATE_VERSION;
    }
    return version;
  }

  @Override
  public List<Tag> injectRuntimeTags(WorkflowSummary workflowSummary, Step step) {
    String version = getJobTemplateVersion(workflowSummary, step);
    return List.copyOf(jobTemplateManager.loadTags(step, version));
  }
}
