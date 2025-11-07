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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.http.HttpRuntimeExecutor;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.HttpArtifact;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParameter;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class HttpStepRuntimeTest extends MaestroBaseTest {
  @Mock private HttpRuntimeExecutor runtimeExecutor;
  @Mock private JobTemplateManager jobTemplateManager;
  @Mock private OutputDataManager outputDataManager;
  @Mock private ParamEvaluator paramEvaluator;
  @Mock private HttpResponse<String> httpResponse;

  private HttpStepRuntime stepRuntime;
  private WorkflowSummary workflowSummary;
  private StepRuntimeSummary runtimeSummary;

  @Before
  public void setUp() throws Exception {
    stepRuntime =
        new HttpStepRuntime(
            runtimeExecutor, jobTemplateManager, outputDataManager, paramEvaluator, MAPPER);

    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("test-wf");
    workflowSummary.setParams(Collections.emptyMap());

    runtimeSummary =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);

    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put(
        "http",
        MapParameter.builder()
            .evaluatedResult(Map.of("url", "https://test.example/api/test", "method", "GET"))
            .evaluatedTime(System.currentTimeMillis())
            .build());
    params.put(
        "state_expr",
        buildParam(
            "state_expr", "if (status_code == 200) return \"DONE\"; else return \"USER_ERROR\";"));
    runtimeSummary.getParams().clear();
    runtimeSummary.getParams().putAll(params);

    when(runtimeExecutor.execute(any())).thenReturn(httpResponse);
  }

  @Test
  public void testExecuteWithDoneState() throws Exception {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"result\":\"test result\"}");
    Mockito.doAnswer(
            invocation -> {
              StringParameter param = invocation.getArgument(3);
              param.setEvaluatedResult("DONE");
              return null;
            })
        .when(paramEvaluator)
        .parseStepParameter(any(), any(), any(), any(), any());

    StepRuntime.Result result = stepRuntime.execute(workflowSummary, null, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    assertEquals(1, result.artifacts().size());
    HttpArtifact artifact = result.artifacts().get(Artifact.Type.HTTP.key()).asHttp();
    assertNotNull(artifact);
    assertEquals(200, artifact.getStatusCode());
    assertEquals("{\"result\":\"test result\"}", artifact.getBody());
    assertEquals("SUCCESS", artifact.getStatus());
    assertNotNull(artifact.getRequest());
    assertEquals("https://test.example/api/test", artifact.getRequest().getUrl());
    assertEquals("GET", artifact.getRequest().getMethod());

    assertEquals(3, result.timeline().size());
    verify(runtimeExecutor, times(1)).execute(any());
    verify(paramEvaluator, times(1)).parseStepParameter(any(), any(), any(), any(), any());
    ArgumentCaptor<OutputData> outputCaptor = ArgumentCaptor.forClass(OutputData.class);
    verify(outputDataManager, times(1)).saveOutputData(outputCaptor.capture());
    OutputData output = outputCaptor.getValue();
    assertEquals("test-wf", output.getWorkflowId());
    assertTrue(output.getArtifacts().isEmpty());
    assertEquals(2, output.getParams().size());
    assertEquals(200L, output.getParams().get("status_code").asLong().longValue());
    assertEquals(
        "{\"result\":\"test result\"}", output.getParams().get("response_body").asString());
  }

  @Test
  public void testExecuteWithErrorState() {
    when(httpResponse.statusCode()).thenReturn(404);
    when(httpResponse.body()).thenReturn("{\"result\":\"Not found error\"}");
    Mockito.doAnswer(
            invocation -> {
              StringParameter param = invocation.getArgument(3);
              param.setEvaluatedResult("FATAL_ERROR");
              return null;
            })
        .when(paramEvaluator)
        .parseStepParameter(any(), any(), any(), any(), any());

    StepRuntime.Result result = stepRuntime.execute(workflowSummary, null, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    HttpArtifact artifact = result.artifacts().get(Artifact.Type.HTTP.key()).asHttp();
    assertEquals(404, artifact.getStatusCode());
    assertEquals("FAILED", artifact.getStatus());
  }

  @Test
  public void testExecuteWithIOException() throws Exception {
    when(runtimeExecutor.execute(any())).thenThrow(new IOException("IO error"));

    StepRuntime.Result result = stepRuntime.execute(workflowSummary, null, runtimeSummary);

    assertEquals(StepRuntime.State.PLATFORM_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
    assertEquals(
        "IOException: IO error", result.timeline().getFirst().asDetails().getErrors().getFirst());
  }

  @Test
  public void testExecuteWithInterruptedException() throws Exception {
    when(runtimeExecutor.execute(any())).thenThrow(new InterruptedException("interrupted"));

    StepRuntime.Result result = stepRuntime.execute(workflowSummary, null, runtimeSummary);

    assertEquals(StepRuntime.State.PLATFORM_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
    assertEquals(
        "InterruptedException: interrupted",
        result.timeline().getFirst().asDetails().getErrors().getFirst());
  }

  @Test
  public void testExecuteWithRetryableError() throws IOException, InterruptedException {
    when(runtimeExecutor.execute(any())).thenThrow(new MaestroRetryableError("retryable error"));

    StepRuntime.Result result = stepRuntime.execute(workflowSummary, null, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
    assertEquals("retryable error", result.timeline().getFirst().asDetails().getMessage());
  }

  @Test
  public void testExecuteWithOtherExceptions() {
    runtimeSummary.getParams().clear();
    StepRuntime.Result result = stepRuntime.execute(workflowSummary, null, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
    assertEquals(
        "Failed to execute http step runtime with an error",
        result.timeline().getFirst().asDetails().getMessage());
    assertEquals(
        "NullPointerException: http params must be present",
        result.timeline().getFirst().asDetails().getErrors().getFirst());
  }

  @Test
  public void testInjectRuntimeParams() {
    when(jobTemplateManager.loadRuntimeParams(eq(workflowSummary), any()))
        .thenReturn(Map.of("foo", buildParam("foo", "bar").toDefinition()));

    Map<String, ParamDefinition> params =
        stepRuntime.injectRuntimeParams(workflowSummary, new TypedStep());

    assertEquals(1, params.size());
    assertEquals("bar", params.get("foo").asStringParamDef().getValue());
    verify(jobTemplateManager, times(1)).loadRuntimeParams(any(), any());
  }

  @Test
  public void testInjectRuntimeTags() {
    when(jobTemplateManager.loadTags(eq(workflowSummary), any()))
        .thenReturn(List.of(Tag.create("test-tag")));

    List<Tag> tags = stepRuntime.injectRuntimeTags(workflowSummary, new TypedStep());

    assertEquals(1, tags.size());
    assertEquals("test-tag", tags.getFirst().getName());
    verify(jobTemplateManager, times(1)).loadTags(any(), any());
  }
}
