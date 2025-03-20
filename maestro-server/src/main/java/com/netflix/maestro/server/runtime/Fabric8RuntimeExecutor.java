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
package com.netflix.maestro.server.runtime;

import com.netflix.maestro.engine.kubernetes.KubernetesJobResult;
import com.netflix.maestro.engine.kubernetes.KubernetesRuntimeExecutor;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Implementation of KubernetesRuntimeExecutor using Fabric8 Kubernetes client. */
@Slf4j
public class Fabric8RuntimeExecutor implements KubernetesRuntimeExecutor {
  private static final String NAMESPACE = "default";
  private static final int MAX_LOG_LINES = 128;
  private static final int MAX_LOG_SIZE = 10 * 1024;
  private static final int DEFAULT_JOB_TTL_IN_SECS = 24 * 60 * 60;
  private static final String DEFAULT_CONTAINER_NAME = "maestro-job";

  private final KubernetesClient client;

  /** Constructor. */
  public Fabric8RuntimeExecutor() {
    this.client =
        new KubernetesClientBuilder()
            .withConfig(new ConfigBuilder().withAutoConfigure().build())
            .build();
  }

  @Override
  public KubernetesJobResult launchJob(KubernetesStepContext context) {
    try {
      Job job = buildJob(context);
      client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

      String jobId = job.getMetadata().getName();
      StepRuntime.State state = deriveState(job.getStatus());
      LOG.info("Job [{}] created with a status [{}]", jobId, state);
      return new KubernetesJobResult(jobId, state);
    } catch (KubernetesClientException e) {
      throw new MaestroBadRequestException(
          e, "Error creating Kubernetes job for " + context.getIdentity());
    }
  }

  @Override
  public KubernetesJobResult checkJobStatus(String jobId) {
    Job job = client.batch().v1().jobs().inNamespace(NAMESPACE).withName(jobId).get();
    return new KubernetesJobResult(jobId, deriveState(job.getStatus()));
  }

  @Override
  public String getJobLog(String jobId) {
    try {
      String log =
          client
              .batch()
              .v1()
              .jobs()
              .inNamespace(NAMESPACE)
              .withName(jobId)
              .tailingLines(MAX_LOG_LINES)
              .getLog();
      return log.length() > MAX_LOG_SIZE ? log.substring(log.length() - MAX_LOG_SIZE) : log;
    } catch (KubernetesClientException e) {
      LOG.warn("Unable to fetch the log and ignore it due to the error:", e);
      return "n/a";
    }
  }

  @Override
  public void terminateJob(String jobId) {
    client.batch().v1().jobs().inNamespace(NAMESPACE).withName(jobId).delete();
  }

  /** Translate Kubernetes Job status to StepRuntime.State. */
  private StepRuntime.State deriveState(JobStatus status) {
    if (status == null) {
      return StepRuntime.State.CONTINUE;
    } else if (status.getActive() != null && status.getActive() > 0) {
      return StepRuntime.State.CONTINUE;
    } else if (status.getFailed() != null && status.getFailed() > 0) {
      return StepRuntime.State.USER_ERROR;
    } else if (status.getSucceeded() != null && status.getSucceeded() > 0) {
      return StepRuntime.State.DONE;
    } else {
      return StepRuntime.State.CONTINUE;
    }
  }

  /** Build Kubernetes Job object from KubernetesStepContext. */
  private Job buildJob(KubernetesStepContext context) {
    KubernetesCommand command = context.getCommand();
    List<EnvVar> envVars =
        command.getEnv().entrySet().stream()
            .map(
                entry ->
                    new EnvVarBuilder()
                        .withName(entry.getKey())
                        .withValue(entry.getValue())
                        .build())
            .toList();

    ResourceRequirementsBuilder resourceBuilder = new ResourceRequirementsBuilder();
    setLimitIfNotNull(resourceBuilder, "cpu", command.getCpu());
    setLimitIfNotNull(resourceBuilder, "memory", command.getMemory());
    setLimitIfNotNull(resourceBuilder, "ephemeral-storage", command.getDisk());
    setLimitIfNotNull(resourceBuilder, "nvidia.com/gpu", command.getGpu());

    return new JobBuilder()
        .withApiVersion("batch/v1")
        .withNewMetadata()
        .withName(command.getJobDeduplicationKey())
        .withLabels(Collections.singletonMap("dedupKey", command.getJobDeduplicationKey()))
        .withAnnotations(context.getMetadata())
        .endMetadata()
        .withNewSpec()
        .withNewTemplate()
        .withNewSpec()
        .addNewContainer()
        .withResources(resourceBuilder.build())
        .withName(DEFAULT_CONTAINER_NAME)
        .withImage(command.getImage())
        .withCommand("/bin/sh", "-c")
        .withArgs(command.getEntrypoint())
        .withEnv(envVars)
        .endContainer()
        .withRestartPolicy("Never")
        .endSpec()
        .endTemplate()
        .withBackoffLimit(0)
        .withTtlSecondsAfterFinished(DEFAULT_JOB_TTL_IN_SECS)
        .endSpec()
        .build();
  }

  private void setLimitIfNotNull(ResourceRequirementsBuilder builder, String key, String value) {
    if (value != null) {
      builder.addToLimits(key, new Quantity(value));
    }
  }
}
