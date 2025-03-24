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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.definition.Criticality;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class NotebookParamsBuilderTest extends MaestroBaseTest {
  private NotebookParamsBuilder notebookParamsBuilder;
  private WorkflowSummary workflowSummary;
  private StepRuntimeSummary stepRuntimeSummary;
  private Map<String, Parameter> params;
  private Map<String, Parameter> workflowParams;

  @Before
  public void before() {
    notebookParamsBuilder = new NotebookParamsBuilder(MAPPER);
    workflowParams = new LinkedHashMap<>();
    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowRunId(2);
    workflowSummary.setWorkflowId("MyWorkflow");
    workflowSummary.setWorkflowInstanceId(123);
    RunProperties runProperties = new RunProperties();
    runProperties.setOwner(User.create("propsuser"));
    workflowSummary.setRunProperties(runProperties);
    workflowParams.put("owner", buildParam("owner", "paramuser"));
    workflowSummary.setParams(workflowParams);
    workflowSummary.setCriticality(Criticality.MEDIUM);
    params = new LinkedHashMap<>();
    Map<String, Object> noteParamsEvaluatedResult = new LinkedHashMap<>();
    noteParamsEvaluatedResult.put(NotebookConstants.NOTEBOOK_INPUT_PARAM, "owner/mynotebook.ipynb");
    noteParamsEvaluatedResult.put("run_id", "MyWorkflow_123_2_mystepid_3");
    MapParameter notebookParams =
        MapParameter.builder()
            .name("notebook")
            .value(
                twoItemMap(
                    NotebookConstants.NOTEBOOK_INPUT_PARAM,
                    StringParamDefinition.builder().value("owner/mynotebook.ipynb").build(),
                    "run_id",
                    StringParamDefinition.builder().value("MyWorkflow_123_2_mystepid_3").build()))
            .evaluatedResult(noteParamsEvaluatedResult)
            .evaluatedTime(System.currentTimeMillis())
            .build();
    params.put(NotebookConstants.NOTEBOOK_KEY, notebookParams);
    stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .params(params)
            .stepInstanceId(2)
            .stepName("mystep")
            .stepId("mystepid")
            .stepInstanceUuid("asdfa-12311-12311")
            .stepAttemptId(3)
            .type(StepType.NOTEBOOK)
            .build();
  }

  @Test
  public void testBuildNotebookParams() {
    workflowParams.put("k1", buildParam("k1", "efg"));
    params.put("k2", buildParam("k2", 123));
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"owner\":\"paramuser\",\"k1\":\"efg\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"k2\":123,\"criticality\":\"medium\",\"attempt_number\":2}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithExcludedParams() {
    workflowParams.put("workflow_id", buildParam("workflow_id", "efg"));
    params.put("step_attempt_id", buildParam("step_attempt_id", 123));
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"owner\":\"paramuser\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"criticality\":\"medium\",\"attempt_number\":2}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithOverloadedRunParams() {
    params.put("run_id", buildParam("run_id", "my_run_id"));
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"owner\":\"paramuser\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"run_id\":\"my_run_id\",\"criticality\":\"medium\",\"attempt_number\":2}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithBackupPropsUser() {
    workflowParams.remove("owner");
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"criticality\":\"medium\",\"attempt_number\":2,\"owner\":\"propsuser\"}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithConflictingWorkflowAndStepParams() {
    workflowParams.put("k1", buildParam("k1", "workflowvalue"));
    params.put("k1", buildParam("k1", "stepvalue"));
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"owner\":\"paramuser\",\"k1\":\"stepvalue\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"criticality\":\"medium\",\"attempt_number\":2}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithNullCriticality() {
    workflowSummary.setCriticality(null);
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"owner\":\"paramuser\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"attempt_number\":2}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithStepTimeoutSpecified() {
    TypedStep step = new TypedStep();
    step.setTimeout(ParsableLong.of("1 day 1 hour"));
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, step);
    assertEquals(
        "{\"owner\":\"paramuser\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"StepTimeout\":\"1 day 1 hour\",\"criticality\":\"medium\",\"attempt_number\":2}",
        notebookParams);
  }

  @Test
  public void testBuildNotebookParamsWithPapermillJsonParamTransform() {
    workflowParams.put("p01", buildParam("p01", new long[] {1L, 2L, 3L}));
    workflowParams.put("p02", buildParam("p02", "None"));
    workflowParams.put("p03", buildParam("p03", "true"));
    workflowParams.put("p04", buildParam("p04", "True"));
    workflowParams.put("p05", buildParam("p05", "01"));
    workflowParams.put("p06", buildParam("p06", "infinity0"));
    workflowParams.put("p07", buildParam("p07", singletonMap("foo", "bar")));
    workflowParams.put("p08", buildParam("p08", Collections.emptyMap()));
    workflowParams.put("p09", buildParam("p09", new boolean[0]));
    workflowParams.put("p10", buildParam("p10", true));
    workflowParams.put("p11", buildParam("p11", 12.3));
    workflowParams.put("p12", buildParam("p12", "123.45"));
    workflowParams.put("p13", buildParam("p13", new double[] {1.2, 34.5}));
    workflowParams.put("p14", buildParam("p14", 123));
    workflowParams.put("p15", buildParam("p15", "foo"));
    String notebookParams =
        notebookParamsBuilder.buildNotebookParams(workflowSummary, stepRuntimeSummary, null);
    assertEquals(
        "{\"owner\":\"paramuser\",\"p01\":[1,2,3],\"p02\":\"None\",\"p03\":\"true\",\"p04\":\"True\",\"p05\":\"01\",\"p06\":\"infinity0\",\"p07\":{\"foo\":\"bar\"},\"p08\":{},\"p09\":[],\"p10\":true,\"p11\":12.3,\"p12\":\"123.45\",\"p13\":[1.2,34.5],\"p14\":123,\"p15\":\"foo\",\"notebook\":{\"input_path\":\"owner/mynotebook.ipynb\",\"run_id\":\"MyWorkflow_123_2_mystepid_3\"},\"criticality\":\"medium\",\"attempt_number\":2}",
        notebookParams);
  }
}
