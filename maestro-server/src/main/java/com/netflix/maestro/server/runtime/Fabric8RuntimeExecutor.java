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
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of KubernetesRuntimeExecutor using Fabric8 Kubernetes client.
 *
 * <p>todo improve the error handling
 */
@Slf4j
@AllArgsConstructor
public class Fabric8RuntimeExecutor implements KubernetesRuntimeExecutor {
  private static final String NAMESPACE = "default";
  private static final int MAX_LOG_LINES = 128;
  private static final int MAX_LOG_SIZE = 10 * 1024;
  private static final String DEFAULT_CONTAINER_NAME = "maestro-job";
  private static final String KUBERNETES_FAILURE_PHASE = "failed";
  private static final String KUBERNETES_SUCCESS_PHASE = "succeeded";
  private static final String MAESTRO_OUTPUT_START_ENV = "MAESTRO_OUTPUT_START";
  private static final String MAESTRO_OUTPUT_START_TOKEN = "MAESTRO_OUTPUT_START_TOKEN";
  private static final String MAESTRO_OUTPUT_END_ENV = "MAESTRO_OUTPUT_END";
  private static final String MAESTRO_OUTPUT_END_TOKEN = "MAESTRO_OUTPUT_END_TOKEN";

  private final KubernetesClient client;

  @Override
  public KubernetesJobResult launchJob(KubernetesStepContext context) {
    try {
      Pod pod = buildPod(context);
      client.pods().inNamespace(NAMESPACE).resource(pod).create();

      String jobId = pod.getMetadata().getName();
      StepRuntime.State state = deriveState(pod.getStatus());
      LOG.info("Job [{}] created with a status [{}]", jobId, state);
      return new KubernetesJobResult(jobId, state);
    } catch (KubernetesClientException e) {
      throw new MaestroBadRequestException(
          e, "Error creating Kubernetes job for " + context.getIdentity());
    }
  }

  @Override
  public KubernetesJobResult checkJobStatus(String jobId) {
    Pod pod = client.pods().inNamespace(NAMESPACE).withName(jobId).get();
    return new KubernetesJobResult(jobId, deriveState(pod.getStatus()));
  }

  @Override
  public String getJobLog(String jobId) {
    try {
      String log =
          client.pods().inNamespace(NAMESPACE).withName(jobId).tailingLines(MAX_LOG_LINES).getLog();
      return log.length() > MAX_LOG_SIZE ? log.substring(log.length() - MAX_LOG_SIZE) : log;
    } catch (KubernetesClientException e) {
      LOG.warn("Unable to fetch the log and ignore it due to the error:", e);
      return "n/a";
    }
  }

  @Override
  public String getJobOutput(String jobId) {
    String log = client.pods().inNamespace(NAMESPACE).withName(jobId).getLog();
    if (log == null) {
      return null;
    }
    int start = log.lastIndexOf(MAESTRO_OUTPUT_START_TOKEN) + MAESTRO_OUTPUT_START_TOKEN.length();
    int end = log.lastIndexOf(MAESTRO_OUTPUT_END_TOKEN);
    if (start < 0 || end < 0 || start >= end) {
      return null;
    }
    return log.substring(start, end);
  }

  @Override
  public void terminateJob(String jobId) {
    client.pods().inNamespace(NAMESPACE).withName(jobId).delete();
  }

  /** Translate Kubernetes Job status to StepRuntime.State. */
  private StepRuntime.State deriveState(PodStatus status) {
    if (status == null || status.getPhase() == null) {
      return StepRuntime.State.CONTINUE;
    } else {
      return switch (status.getPhase().toLowerCase(Locale.US)) {
        case KUBERNETES_SUCCESS_PHASE -> StepRuntime.State.DONE;
        case KUBERNETES_FAILURE_PHASE -> StepRuntime.State.USER_ERROR;
        default -> StepRuntime.State.CONTINUE;
      };
    }
  }

  /** Build Kubernetes Pod object from KubernetesStepContext. */
  private Pod buildPod(KubernetesStepContext context) {
    KubernetesCommand command = context.getCommand();
    List<EnvVar> envVars = buildEnvs(command);

    ResourceRequirementsBuilder resourceBuilder = new ResourceRequirementsBuilder();
    setLimitIfNotNull(resourceBuilder, "cpu", command.getCpu());
    setLimitIfNotNull(resourceBuilder, "memory", command.getMemory());
    setLimitIfNotNull(resourceBuilder, "ephemeral-storage", command.getDisk());
    setLimitIfNotNull(resourceBuilder, "nvidia.com/gpu", command.getGpu());

    return new PodBuilder()
        .withNewMetadata()
        .withName(command.getJobDeduplicationKey())
        .withLabels(Collections.singletonMap("dedupKey", command.getJobDeduplicationKey()))
        .withAnnotations(context.getMetadata())
        .endMetadata()
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
        .build();
  }

  private List<EnvVar> buildEnvs(KubernetesCommand command) {
    List<EnvVar> envVars =
        command.getEnv().entrySet().stream()
            .map(
                entry ->
                    new EnvVarBuilder()
                        .withName(entry.getKey())
                        .withValue(entry.getValue())
                        .build())
            .collect(Collectors.toList());
    // add maestro system envs for the output support.
    envVars.add(
        new EnvVarBuilder()
            .withName(MAESTRO_OUTPUT_START_ENV)
            .withValue(MAESTRO_OUTPUT_START_TOKEN)
            .build());
    envVars.add(
        new EnvVarBuilder()
            .withName(MAESTRO_OUTPUT_END_ENV)
            .withValue(MAESTRO_OUTPUT_END_TOKEN)
            .build());
    return envVars;
  }

  private void setLimitIfNotNull(ResourceRequirementsBuilder builder, String key, String value) {
    if (value != null) {
      builder.addToLimits(key, new Quantity(value));
    }
  }
}
