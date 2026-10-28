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

import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.stepruntime.JobTemplate;

/**
 * Everything a {@link TemplatePreparer} reads to prepare a template step. The artifact is the one
 * the runtime persists for this step; a preparer that launches a prepare instance records it there.
 *
 * @param workflowSummary the parent workflow instance summary, read-only
 * @param step the template step definition
 * @param runtimeSummary the template step runtime summary with its evaluated params, read-only
 * @param definition the registered job template definition of the step's sub type
 * @param artifact the template artifact of the current run
 * @param previousArtifact the template artifact of the previous step attempt, with the expansion
 *     and the inline instance that failed, so that a preparer can prepare the retry differently;
 *     null on the first attempt
 */
public record PrepareContext(
    WorkflowSummary workflowSummary,
    Step step,
    StepRuntimeSummary runtimeSummary,
    JobTemplate.Definition definition,
    TemplateArtifact artifact,
    TemplateArtifact previousArtifact) {

  /** Context of a first attempt, without a previous artifact. */
  public PrepareContext(
      WorkflowSummary workflowSummary,
      Step step,
      StepRuntimeSummary runtimeSummary,
      JobTemplate.Definition definition,
      TemplateArtifact artifact) {
    this(workflowSummary, step, runtimeSummary, definition, artifact, null);
  }
}
