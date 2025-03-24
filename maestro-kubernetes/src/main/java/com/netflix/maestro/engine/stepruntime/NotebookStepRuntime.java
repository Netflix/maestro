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
import com.netflix.maestro.engine.kubernetes.KubernetesCommandGenerator;
import com.netflix.maestro.engine.kubernetes.KubernetesRuntimeExecutor;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.notebook.PapermillCommand;
import com.netflix.maestro.engine.notebook.PapermillEntrypointBuilder;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.NotebookArtifact;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import lombok.extern.slf4j.Slf4j;

/** Jupyter Notebook step runtime to execute notebook using papermill. */
@Slf4j
public class NotebookStepRuntime extends KubernetesStepRuntime {
  private final PapermillEntrypointBuilder entrypointBuilder;

  /** Constructor. */
  public NotebookStepRuntime(
      KubernetesRuntimeExecutor runtimeExecutor,
      KubernetesCommandGenerator commandGenerator,
      OutputDataManager outputDataManager,
      ObjectMapper objectMapper,
      MaestroMetrics metrics,
      PapermillEntrypointBuilder entrypointBuilder) {
    super(runtimeExecutor, commandGenerator, outputDataManager, objectMapper, metrics);
    this.entrypointBuilder = entrypointBuilder;
  }

  @Override
  protected void customizePreLaunchCommand(KubernetesStepContext context) {
    super.customizePreLaunchCommand(context);

    PapermillCommand papermillCommand = entrypointBuilder.generatePapermillRuntime(context);

    KubernetesCommand originalCommand = context.getCommand();
    context.setCommand(
        originalCommand.toBuilder().entrypoint(papermillCommand.entrypoint()).build());

    NotebookArtifact notebookArtifact = new NotebookArtifact();
    notebookArtifact.setOutputPath(papermillCommand.outputPath());
    context.getPendingArtifacts().put(Artifact.Type.NOTEBOOK.key(), notebookArtifact);
  }
}
