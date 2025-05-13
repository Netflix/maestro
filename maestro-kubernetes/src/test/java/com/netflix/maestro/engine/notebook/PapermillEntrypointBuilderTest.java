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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.eval.ExprEvaluator;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.properties.SelProperties;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PapermillEntrypointBuilderTest extends MaestroBaseTest {
  private static ParamEvaluator EVALUATOR;

  private PapermillEntrypointBuilder papermillEntrypointBuilder;
  private StepRuntimeSummary stepRuntimeSummary;
  private KubernetesStepContext context;

  @BeforeClass
  public static void init() {
    var props = new SelProperties();
    props.setThreadNum(3);
    props.setTimeoutMillis(120000);
    props.setStackLimit(128);
    props.setLoopLimit(10000);
    props.setArrayLimit(10000);
    props.setLengthLimit(10000);
    props.setVisitLimit(100000000L);
    props.setMemoryLimit(10000000L);
    ExprEvaluator evaluator =
        new ExprEvaluator(props, new MaestroParamExtensionRepo(null, null, MAPPER));
    evaluator.postConstruct();
    EVALUATOR = new ParamEvaluator(evaluator, MaestroBaseTest.MAPPER);
  }

  @Before
  public void before() throws IOException {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowRunId(1);
    workflowSummary.setWorkflowId("MyWorkflow");
    Map<String, Parameter> workflowParams = new HashMap<>();
    workflowSummary.setParams(workflowParams);
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.create("propsuser"));
    workflowSummary.setRunProperties(runProperties);

    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-notebook.json", WorkflowDefinition.class);
    Workflow workflow = definition.getWorkflow();
    Map<String, Parameter> params = toParameters(workflow.getParams());
    EVALUATOR.evaluateWorkflowParameters(params, workflow.getId());
    stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .params(params)
            .stepInstanceId(2)
            .stepName("mystep")
            .stepInstanceUuid("asdfa-12311-12311")
            .type(StepType.NOTEBOOK)
            .subType("SparkJob")
            .build();
    context = new KubernetesStepContext(workflowSummary, stepRuntimeSummary, null);
    papermillEntrypointBuilder = new PapermillEntrypointBuilder(new NotebookParamsBuilder(MAPPER));
  }

  @Test
  public void testShouldDefaultTimeout() {
    deleteNotebookParam(NotebookConstants.PAPERMILL_ARG_PARAM);
    String entrypoint = papermillEntrypointBuilder.generatePapermillRuntime(context).entrypoint();
    assertTrue(entrypoint.contains("'--start_timeout' '600'"));
  }

  @Test
  public void testShouldAllowOverrideTimeout() {
    String entrypoint = papermillEntrypointBuilder.generatePapermillRuntime(context).entrypoint();
    assertTrue(entrypoint.contains("'--start_timeout' '100'"));
  }

  @Test
  public void testShouldHaveLogOutput() {
    String entrypoint = papermillEntrypointBuilder.generatePapermillRuntime(context).entrypoint();
    assertTrue(entrypoint.contains("--log-output"));
  }

  @Test
  public void testShouldContainParams() {
    String entrypoint = papermillEntrypointBuilder.generatePapermillRuntime(context).entrypoint();
    assertTrue(entrypoint.contains("-y '{\"RUN_TS\":1607746121804,\"DSL_DEFAULT_TZ\":\"UTC\""));
  }

  @Test
  public void testProvidedInputPath() {
    String entrypoint = papermillEntrypointBuilder.generatePapermillRuntime(context).entrypoint();
    assertTrue(entrypoint.contains("papermill '/some/notebook.ipynb'"));
  }

  @Test
  public void testWithOutputPath() {
    PapermillCommand papermillCommand =
        papermillEntrypointBuilder.generatePapermillRuntime(context);
    String entrypoint = papermillCommand.entrypoint();
    assertTrue(entrypoint.contains("'s3://path/someplace/nice.ipynb'"));
    assertFalse(papermillCommand.outputPath().contains("'s3://path/someplace/nice.ipynb'"));
    assertTrue(papermillCommand.outputPath().contains("s3://path/someplace/nice.ipynb"));
  }

  @Test
  public void testGeneratedEntryPointWithoutGitRepo() {
    PapermillCommand papermillCommand =
        papermillEntrypointBuilder.generatePapermillRuntime(context);
    String entrypoint = papermillCommand.entrypoint();
    assertTrue(entrypoint.startsWith(" papermill "));
  }

  @Test
  public void testGeneratedEntryPointWithGitRepo() {
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_REPO,
            ParamDefinition.buildParamDefinition(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo"));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo");

    PapermillCommand papermillCommand =
        papermillEntrypointBuilder.generatePapermillRuntime(context);
    String entrypoint = papermillCommand.entrypoint();
    assertTrue(
        entrypoint.startsWith(
            " git clone my_repo maestro_notebook_git_repo && "
                + "cd \"maestro_notebook_git_repo/$(dirname '/some/notebook.ipynb')\" && "
                + "papermill \"$(basename '/some/notebook.ipynb')\" "));
  }

  @Test
  public void testGeneratedEntryPointWithGitRepoAndGitBranch() {
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_REPO,
            ParamDefinition.buildParamDefinition(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo"));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo");
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_BRANCH,
            ParamDefinition.buildParamDefinition(
                NotebookConstants.NOTEBOOK_GIT_BRANCH, "my_branch"));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_BRANCH, "my_branch");

    PapermillCommand papermillCommand =
        papermillEntrypointBuilder.generatePapermillRuntime(context);
    String entrypoint = papermillCommand.entrypoint();
    assertTrue(
        entrypoint.startsWith(
            " git clone -b my_branch my_repo maestro_notebook_git_repo && "
                + "cd \"maestro_notebook_git_repo/$(dirname '/some/notebook.ipynb')\" && "
                + "papermill \"$(basename '/some/notebook.ipynb')\" "));
  }

  @Test
  public void testGeneratedEntryPointWithGitRepoAndGitCommit() {
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_REPO,
            ParamDefinition.buildParamDefinition(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo"));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo");
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_COMMIT,
            ParamDefinition.buildParamDefinition(
                NotebookConstants.NOTEBOOK_GIT_COMMIT, "my_commit"));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_COMMIT, "my_commit");

    PapermillCommand papermillCommand =
        papermillEntrypointBuilder.generatePapermillRuntime(context);
    String entrypoint = papermillCommand.entrypoint();
    assertTrue(
        entrypoint.startsWith(
            " git clone my_repo maestro_notebook_git_repo && "
                + "cd \"maestro_notebook_git_repo/$(dirname '/some/notebook.ipynb')\" && "
                + "git checkout my_commit && papermill \"$(basename '/some/notebook.ipynb')\" "));
  }

  @Test
  public void testGeneratedEntryPointWithGitRepoAndGitSubmodules() {
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_REPO,
            ParamDefinition.buildParamDefinition(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo"));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_REPO, "my_repo");
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .put(
            NotebookConstants.NOTEBOOK_GIT_SUBMODULES,
            ParamDefinition.buildParamDefinition(NotebookConstants.NOTEBOOK_GIT_SUBMODULES, true));
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .put(NotebookConstants.NOTEBOOK_GIT_SUBMODULES, true);

    PapermillCommand papermillCommand =
        papermillEntrypointBuilder.generatePapermillRuntime(context);
    String entrypoint = papermillCommand.entrypoint();
    assertTrue(
        entrypoint.startsWith(
            " git clone --recurse-submodules my_repo maestro_notebook_git_repo && "
                + "cd \"maestro_notebook_git_repo/$(dirname '/some/notebook.ipynb')\" && "
                + "papermill \"$(basename '/some/notebook.ipynb')\" "));
  }

  private void deleteNotebookParam(String key) {
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getValue()
        .remove(key);
    stepRuntimeSummary
        .getParams()
        .get(NotebookConstants.NOTEBOOK_KEY)
        .asMapParam()
        .getEvaluatedResult()
        .remove(key);
  }
}
