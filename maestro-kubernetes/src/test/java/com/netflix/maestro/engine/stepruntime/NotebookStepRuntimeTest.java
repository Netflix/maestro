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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.kubernetes.KubernetesCommandGenerator;
import com.netflix.maestro.engine.kubernetes.KubernetesRuntimeExecutor;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.notebook.PapermillCommand;
import com.netflix.maestro.engine.notebook.PapermillEntrypointBuilder;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import org.junit.Test;

public class NotebookStepRuntimeTest extends MaestroBaseTest {
  @Test
  public void testCustomizePreLaunchCommand() {
    PapermillEntrypointBuilder entrypointBuilder = mock(PapermillEntrypointBuilder.class);
    NotebookStepRuntime notebookStepRuntime =
        new NotebookStepRuntime(
            mock(KubernetesRuntimeExecutor.class),
            mock(KubernetesCommandGenerator.class),
            mock(JobTemplateManager.class),
            mock(OutputDataManager.class),
            MAPPER,
            mock(MaestroMetrics.class),
            entrypointBuilder);
    KubernetesStepContext context =
        new KubernetesStepContext(
            new WorkflowSummary(), StepRuntimeSummary.builder().build(), null);
    context.setCommand(KubernetesCommand.builder().build());
    when(entrypointBuilder.generatePapermillRuntime(context))
        .thenReturn(new PapermillCommand("inputPath", "outputPath", "entrypoint"));

    notebookStepRuntime.customizePreLaunchCommand(context);

    assertEquals("entrypoint", context.getCommand().getEntrypoint());
    assertTrue(context.getPendingArtifacts().containsKey(Artifact.Type.NOTEBOOK.key()));
    assertEquals(
        "outputPath",
        context
            .getPendingArtifacts()
            .get(Artifact.Type.NOTEBOOK.key())
            .asNotebook()
            .getOutputPath());
  }
}
