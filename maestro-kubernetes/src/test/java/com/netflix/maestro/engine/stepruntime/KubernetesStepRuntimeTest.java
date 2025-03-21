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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.kubernetes.KubernetesCommandGenerator;
import com.netflix.maestro.engine.kubernetes.KubernetesJobResult;
import com.netflix.maestro.engine.kubernetes.KubernetesRuntimeExecutor;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.KubernetesArtifact;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests for {@link KubernetesStepRuntime}. */
public class KubernetesStepRuntimeTest extends MaestroBaseTest {
  private KubernetesRuntimeExecutor runtimeExecutor;
  private OutputDataManager outputDataManager;
  private MaestroMetricRepo metricRepo;
  private KubernetesStepRuntime stepRuntime;
  private StepRuntimeSummary runtimeSummary;

  @Before
  public void setUp() throws Exception {
    runtimeExecutor = Mockito.mock(KubernetesRuntimeExecutor.class);
    outputDataManager = Mockito.mock(OutputDataManager.class);
    KubernetesCommandGenerator commandGenerator = new KubernetesCommandGenerator(MAPPER);
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    stepRuntime =
        new KubernetesStepRuntime(
            runtimeExecutor, commandGenerator, outputDataManager, MAPPER, metricRepo);

    runtimeSummary =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);
    runtimeSummary
        .getParams()
        .put(
            "kubernetes",
            MapParameter.builder()
                .evaluatedResult(
                    Map.of(
                        "app_name", "test-app",
                        "cpu", "0.5",
                        "disk", "1G",
                        "gpu", "0",
                        "memory", "1G",
                        "image", "test-image",
                        "entrypoint", "test-entrypoint",
                        "env",
                            Map.of(
                                "key1", "value1",
                                "key2", "value2"),
                        "job_deduplication_key", "job_deduplication_key",
                        "owner_email", "owner_email"))
                .evaluatedTime(12345L)
                .build());
  }

  @Test
  public void testStart() {
    when(runtimeExecutor.launchJob(Mockito.any()))
        .thenReturn(new KubernetesJobResult("job_deduplication_key", StepRuntime.State.CONTINUE));
    StepRuntime.Result res = stepRuntime.start(new WorkflowSummary(), null, runtimeSummary);
    assertEquals(StepRuntime.State.CONTINUE, res.getState());
    assertEquals(1, res.getArtifacts().size());
    assertTrue(res.getTimeline().isEmpty());
    var artifact = res.getArtifacts().get(Artifact.Type.KUBERNETES.key()).asKubernetes();
    assertEquals("job_deduplication_key", artifact.getJobId());
    assertNull(artifact.getExecutionOutput());
    assertEquals("test-entrypoint", artifact.getExecutionScript());
    var contextCaptor = ArgumentCaptor.forClass(KubernetesStepContext.class);
    Mockito.verify(runtimeExecutor, times(1)).launchJob(contextCaptor.capture());
    var context = contextCaptor.getValue();
    assertEquals(
        new KubernetesJobResult("job_deduplication_key", StepRuntime.State.CONTINUE),
        context.getJobResult());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.STEP_LAUNCHED_METRIC,
                KubernetesStepRuntime.class,
                MetricConstants.TYPE_TAG,
                "launch",
                MetricConstants.STATUS_TAG,
                MetricConstants.STATUS_TAG_VALUE_SUCCESS)
            .count());
  }

  @Test
  public void testFailureOnStart() {
    when(runtimeExecutor.launchJob(Mockito.any()))
        .thenThrow(new MaestroBadRequestException(List.of(), "test-error"));
    StepRuntime.Result res = stepRuntime.start(new WorkflowSummary(), null, runtimeSummary);
    assertEquals(StepRuntime.State.PLATFORM_ERROR, res.getState());
    assertTrue(res.getArtifacts().isEmpty());
    assertEquals(1, res.getTimeline().size());
    assertEquals("Error starting KubernetesStepRuntime", res.getTimeline().getFirst().getMessage());
    assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.STEP_LAUNCHED_METRIC,
                KubernetesStepRuntime.class,
                MetricConstants.TYPE_TAG,
                "bad_request",
                MetricConstants.STATUS_TAG,
                MetricConstants.STATUS_TAG_VALUE_FAILURE)
            .count());
  }

  @Test
  public void testExecute() {
    var ka = new KubernetesArtifact();
    ka.setJobId("job_deduplication_key");
    runtimeSummary.getArtifacts().put(Artifact.Type.KUBERNETES.key(), ka);
    when(runtimeExecutor.getJobLog("job_deduplication_key")).thenReturn("test-log");
    when(runtimeExecutor.checkJobStatus("job_deduplication_key"))
        .thenReturn(new KubernetesJobResult("job_deduplication_key", StepRuntime.State.DONE));
    StepRuntime.Result res = stepRuntime.execute(new WorkflowSummary(), null, runtimeSummary);
    assertEquals(StepRuntime.State.DONE, res.getState());
    assertEquals(1, res.getArtifacts().size());
    assertTrue(res.getTimeline().isEmpty());
    var artifact = res.getArtifacts().get(Artifact.Type.KUBERNETES.key()).asKubernetes();
    assertEquals("job_deduplication_key", artifact.getJobId());
    assertEquals("test-log", artifact.getExecutionOutput());
    Mockito.verify(runtimeExecutor, times(1)).checkJobStatus("job_deduplication_key");
    Mockito.verify(runtimeExecutor, times(1)).getJobLog("job_deduplication_key");
    Mockito.verify(runtimeExecutor, times(1)).getJobOutput("job_deduplication_key");
  }

  @Test
  public void testExecuteWithOutput() {
    String sampleOutput =
        "{\"params\":{\"foo\": {\"value\":\"bar\",\"type\":\"STRING\",\"evaluated_result\":\"bar\",\"evaluated_time\":1}}}";
    var ka = new KubernetesArtifact();
    ka.setJobId("job_deduplication_key");
    runtimeSummary.getArtifacts().put(Artifact.Type.KUBERNETES.key(), ka);
    when(runtimeExecutor.getJobLog("job_deduplication_key")).thenReturn("test-log");
    when(runtimeExecutor.checkJobStatus("job_deduplication_key"))
        .thenReturn(new KubernetesJobResult("job_deduplication_key", StepRuntime.State.DONE));
    when(runtimeExecutor.getJobOutput("job_deduplication_key")).thenReturn(sampleOutput);
    StepRuntime.Result res = stepRuntime.execute(new WorkflowSummary(), null, runtimeSummary);
    assertEquals(StepRuntime.State.DONE, res.getState());
    assertEquals(1, res.getArtifacts().size());
    assertTrue(res.getTimeline().isEmpty());
    var artifact = res.getArtifacts().get(Artifact.Type.KUBERNETES.key()).asKubernetes();
    assertEquals("job_deduplication_key", artifact.getJobId());
    Mockito.verify(runtimeExecutor, times(1)).checkJobStatus("job_deduplication_key");
    Mockito.verify(runtimeExecutor, times(1)).getJobLog("job_deduplication_key");
    Mockito.verify(runtimeExecutor, times(1)).getJobOutput("job_deduplication_key");
    Mockito.verify(outputDataManager, times(1)).saveOutputData(Mockito.any());
  }

  @Test
  public void testFailureOnExecute() {
    when(runtimeExecutor.checkJobStatus("job_deduplication_key"))
        .thenThrow(new RuntimeException("test-error"));
    var ka = new KubernetesArtifact();
    ka.setJobId("job_deduplication_key");
    runtimeSummary.getArtifacts().put(Artifact.Type.KUBERNETES.key(), ka);

    StepRuntime.Result res = stepRuntime.execute(new WorkflowSummary(), null, runtimeSummary);
    assertEquals(StepRuntime.State.CONTINUE, res.getState());
    assertTrue(res.getArtifacts().isEmpty());
    assertTrue(res.getTimeline().isEmpty());
  }

  @Test
  public void testIOExceptionOnExecute() {
    when(runtimeExecutor.checkJobStatus("job_deduplication_key"))
        .thenReturn(new KubernetesJobResult("job_deduplication_key", StepRuntime.State.DONE));
    when(runtimeExecutor.getJobOutput("job_deduplication_key")).thenReturn("test-output");
    var ka = new KubernetesArtifact();
    ka.setJobId("job_deduplication_key");
    runtimeSummary.getArtifacts().put(Artifact.Type.KUBERNETES.key(), ka);

    StepRuntime.Result res = stepRuntime.execute(new WorkflowSummary(), null, runtimeSummary);
    assertEquals(StepRuntime.State.USER_ERROR, res.getState());
    assertTrue(res.getArtifacts().isEmpty());
    assertEquals(1, res.getTimeline().size());
    assertEquals(
        "Error processing the output data in KubernetesStepRuntime",
        res.getTimeline().getFirst().getMessage());
  }

  @Test
  public void testThrowOnExecute() {
    AssertHelper.assertThrows(
        "invalid case that kubernetes job id is missing",
        MaestroUnprocessableEntityException.class,
        "Invalid: JobId is null",
        () -> stepRuntime.execute(new WorkflowSummary(), null, runtimeSummary));
  }

  @Test
  public void testTerminate() {
    var ka = new KubernetesArtifact();
    ka.setJobId("job_deduplication_key");
    runtimeSummary.getArtifacts().put(Artifact.Type.KUBERNETES.key(), ka);
    when(runtimeExecutor.checkJobStatus("job_deduplication_key"))
        .thenReturn(new KubernetesJobResult("job_deduplication_key", StepRuntime.State.CONTINUE));
    StepRuntime.Result res = stepRuntime.terminate(new WorkflowSummary(), runtimeSummary);
    assertEquals(StepRuntime.State.STOPPED, res.getState());
    assertTrue(res.getArtifacts().isEmpty());
    assertEquals(1, res.getTimeline().size());
    assertEquals(
        "Terminated Kubernetes job for jobId=job_deduplication_key",
        res.getTimeline().getFirst().getMessage());
    Mockito.verify(runtimeExecutor, times(1)).checkJobStatus("job_deduplication_key");
    Mockito.verify(runtimeExecutor, times(1)).terminateJob("job_deduplication_key");
  }

  @Test
  public void testNoJobOnTerminate() {
    StepRuntime.Result res = stepRuntime.terminate(new WorkflowSummary(), runtimeSummary);
    assertEquals(StepRuntime.State.STOPPED, res.getState());
    assertTrue(res.getArtifacts().isEmpty());
    assertEquals(1, res.getTimeline().size());
    assertEquals(
        "Job terminating, no kubernetes job found", res.getTimeline().getFirst().getMessage());
  }

  @Test
  public void testThrowOnTerminate() {
    var ka = new KubernetesArtifact();
    ka.setJobId("job_deduplication_key");
    runtimeSummary.getArtifacts().put(Artifact.Type.KUBERNETES.key(), ka);
    when(runtimeExecutor.checkJobStatus("job_deduplication_key"))
        .thenThrow(new RuntimeException("test-error"));

    AssertHelper.assertThrows(
        "Invalid case throwing an exception",
        MaestroRetryableError.class,
        "Error terminating Kubernetes job for jobId",
        () -> stepRuntime.terminate(new WorkflowSummary(), runtimeSummary));
  }
}
