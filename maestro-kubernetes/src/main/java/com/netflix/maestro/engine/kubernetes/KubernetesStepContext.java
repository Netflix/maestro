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
package com.netflix.maestro.engine.kubernetes;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.KubernetesArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import com.netflix.maestro.models.timeline.TimelineEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/** Kubernetes step context. */
@RequiredArgsConstructor
@Setter
@Getter
public class KubernetesStepContext {
  private final WorkflowSummary workflowSummary;
  private final StepRuntimeSummary runtimeSummary;
  @Nullable private final Step step;
  private final Map<String, Artifact> pendingArtifacts = new LinkedHashMap<>();
  private final List<TimelineEvent> pendingTimeline = new ArrayList<>();

  private KubernetesCommand command;
  private KubernetesJobResult jobResult;

  /** Get the Kubernetes artifact. */
  public KubernetesArtifact getKubernetesArtifact() {
    if (runtimeSummary.getArtifacts().containsKey(Artifact.Type.KUBERNETES.key())) {
      return runtimeSummary.getArtifacts().get(Artifact.Type.KUBERNETES.key()).asKubernetes();
    }
    return null;
  }

  /** Get the identity of the step context. */
  public String getIdentity() {
    return workflowSummary.getIdentity() + runtimeSummary.getIdentity();
  }

  /** Get the metadata of the step context. */
  public Map<String, String> getMetadata() {
    return Map.of(
        "orchestrator", "maestro",
        "workflow_identity", workflowSummary.getIdentity(),
        "step_identity", runtimeSummary.getIdentity());
  }
}
