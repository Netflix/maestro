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
package com.netflix.maestro.engine.notebook;

import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/** Build Jupyter Notebook launch command from passed notebook parameters. */
@AllArgsConstructor
public class PapermillEntrypointBuilder {
  private final NotebookParamsBuilder paramsBuilder;

  /**
   * Generate Papermill properties for notebooks executions.
   *
   * @return the generated papermill command object
   */
  public PapermillCommand generatePapermillRuntime(KubernetesStepContext context) {
    StepRuntimeSummary runtimeSummary = context.getRuntimeSummary();
    MapParameter notebookParams = getAndValidateNotebookParams(runtimeSummary);

    String inputPath = buildInputPath(notebookParams);
    String outputPath =
        notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_OUTPUT_PARAM).asString();
    String extraArgs = buildExtraArguments(notebookParams);
    String entrypoint =
        String.format(
            "%s %s %s %s %s %s %s",
            notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_INIT_COMMAND).asString(),
            NotebookConstants.PAPERMILL_COMMAND,
            quoteBashCommandArg(inputPath),
            quoteBashCommandArg(outputPath),
            buildInputArgs(context),
            NotebookConstants.LOG_OUTPUT_ARG,
            extraArgs);

    String inputArg = quoteBashCommandArg(inputPath);
    String gitCloneCmd = buildGitCloneCommand(notebookParams, inputArg);
    if (gitCloneCmd != null && !gitCloneCmd.isEmpty()) {
      entrypoint =
          String.format(
              "%s %s %s \"$(basename %s)\" %s %s %s %s",
              notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_INIT_COMMAND).asString(),
              gitCloneCmd,
              NotebookConstants.PAPERMILL_COMMAND,
              inputArg,
              quoteBashCommandArg(outputPath),
              buildInputArgs(context),
              NotebookConstants.LOG_OUTPUT_ARG,
              extraArgs);
    }

    return new PapermillCommand(inputPath, outputPath, entrypoint);
  }

  /**
   * Get and validate Notebook parameters.
   *
   * @param stepSummary step runtime summary
   * @return Kubernetes parameters
   */
  private MapParameter getAndValidateNotebookParams(StepRuntimeSummary stepSummary) {
    Checks.notNull(stepSummary.getParams(), "params must be present");
    Checks.notNull(
        stepSummary.getParams().get(NotebookConstants.NOTEBOOK_KEY),
        "notebook params must be present");
    return stepSummary.getParams().get(NotebookConstants.NOTEBOOK_KEY).asMapParam();
  }

  // Extra validation should be done within workflow definition.
  private String buildGitCloneCommand(MapParameter notebookParams, String inputArg) {
    if (notebookParams.containsParam(NotebookConstants.NOTEBOOK_GIT_REPO)) {
      String gitCmd;
      if (notebookParams.containsParam(NotebookConstants.NOTEBOOK_GIT_SUBMODULES)
          && notebookParams
              .getEvaluatedParam(NotebookConstants.NOTEBOOK_GIT_SUBMODULES)
              .asBoolean()) {
        gitCmd = "git clone --recurse-submodules";
      } else {
        gitCmd = "git clone";
      }
      if (notebookParams.containsParam(NotebookConstants.NOTEBOOK_GIT_BRANCH)) {
        return String.format(
            "%s -b %s %s %s && cd \"%s/$(dirname %s)\" &&",
            gitCmd,
            notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_GIT_BRANCH).asString(),
            notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_GIT_REPO).asString(),
            NotebookConstants.NOTEBOOK_GIT_CLONE_FOLDER,
            NotebookConstants.NOTEBOOK_GIT_CLONE_FOLDER,
            inputArg);
      }
      if (notebookParams.containsParam(NotebookConstants.NOTEBOOK_GIT_COMMIT)) {
        return String.format(
            "%s %s %s && cd \"%s/$(dirname %s)\" && git checkout %s &&",
            gitCmd,
            notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_GIT_REPO).asString(),
            NotebookConstants.NOTEBOOK_GIT_CLONE_FOLDER,
            NotebookConstants.NOTEBOOK_GIT_CLONE_FOLDER,
            inputArg,
            notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_GIT_COMMIT).asString());
      }
      return String.format(
          "%s %s %s && cd \"%s/$(dirname %s)\" &&",
          gitCmd,
          notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_GIT_REPO).asString(),
          NotebookConstants.NOTEBOOK_GIT_CLONE_FOLDER,
          NotebookConstants.NOTEBOOK_GIT_CLONE_FOLDER,
          inputArg);
    }
    return "";
  }

  private String buildInputPath(MapParameter notebookParams) {
    return notebookParams.getEvaluatedParam(NotebookConstants.NOTEBOOK_INPUT_PARAM).asString();
  }

  /** Build extra papermill arguments. */
  private String buildExtraArguments(MapParameter notebookParams) {
    List<String> extraArgs = new ArrayList<>();
    if (notebookParams.containsParam(NotebookConstants.PAPERMILL_ARG_PARAM)) {
      String[] userArgs =
          notebookParams.getEvaluatedParam(NotebookConstants.PAPERMILL_ARG_PARAM).asStringArray();
      List<String> userArgsList = Arrays.stream(userArgs).map(String::trim).toList();
      extraArgs.addAll(userArgsList);
    }
    if (!extraArgs.contains(NotebookConstants.START_TIMEOUT_ARG)) {
      extraArgs.add(NotebookConstants.START_TIMEOUT_ARG);
      extraArgs.add(Long.toString(NotebookConstants.START_TIMEOUT_DEFAULT));
    }
    return extraArgs.stream()
        .map(this::quoteBashCommandArg)
        .collect(Collectors.joining(NotebookConstants.PARAM_DELIMITER));
  }

  private String quoteBashCommandArg(String arg) {
    return String.format("'%s'", arg.replaceAll("'", "'\\\\''"));
  }

  private String buildInputArgs(KubernetesStepContext context) {
    String args =
        paramsBuilder.buildNotebookParams(
            context.getWorkflowSummary(), context.getRuntimeSummary(), context.getStep());
    return String.format("-y %s", quoteBashCommandArg(args));
  }
}
