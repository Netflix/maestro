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
package com.netflix.maestro.engine.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

public class ExtensionStepRuntimeTest extends MaestroEngineBaseTest {
  @Mock private JobTemplateManager jobTemplateManager;
  @Mock private StepRuntime sparkRuntime;

  private final ExtensionStepRuntimeRegistry registry = new ExtensionStepRuntimeRegistry();
  private final WorkflowSummary workflowSummary = new WorkflowSummary();
  private final StepRuntimeSummary runtimeSummary =
      StepRuntimeSummary.builder()
          .stepId("test")
          .stepAttemptId(1)
          .stepInstanceUuid("uuid")
          .stepName("testExtension")
          .stepInstanceId(1)
          .type(StepType.EXTENSION)
          .subType("spark")
          .build();
  private ExtensionStepRuntime runtime;
  private TypedStep step;

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Before
  public void setUp() {
    registry.register("spark", sparkRuntime);
    runtime = new ExtensionStepRuntime(registry, jobTemplateManager);
    step = new TypedStep();
    step.setId("test");
    step.setType(StepType.EXTENSION);
    step.setSubType("spark");
    step.setSubTypeVersion("v2");
  }

  @Test
  public void testDelegateToRegisteredSubType() {
    var startResult = StepRuntime.Result.of(StepRuntime.State.CONTINUE);
    var executeResult = StepRuntime.Result.of(StepRuntime.State.DONE, 3000L);
    when(sparkRuntime.start(workflowSummary, step, runtimeSummary)).thenReturn(startResult);
    when(sparkRuntime.execute(workflowSummary, step, runtimeSummary)).thenReturn(executeResult);

    assertSame(startResult, runtime.start(workflowSummary, step, runtimeSummary));
    assertSame(executeResult, runtime.execute(workflowSummary, step, runtimeSummary));
    verify(jobTemplateManager, never()).loadAncestorJobTypes(any(), anyString(), any());
  }

  @Test
  public void testDelegateToJobTemplateAncestor() {
    step.setSubType("my-spark-job");
    when(jobTemplateManager.loadAncestorJobTypes(workflowSummary, "my-spark-job", "v2"))
        .thenReturn(List.of("team-spark-base", "spark"));
    var result = StepRuntime.Result.of(StepRuntime.State.CONTINUE);
    when(sparkRuntime.start(workflowSummary, step, runtimeSummary)).thenReturn(result);

    assertSame(result, runtime.start(workflowSummary, step, runtimeSummary));
  }

  @Test
  public void testTerminateUsesRuntimeSummarySubType() {
    var result = StepRuntime.Result.of(StepRuntime.State.STOPPED);
    when(sparkRuntime.terminate(workflowSummary, runtimeSummary)).thenReturn(result);

    assertSame(result, runtime.terminate(workflowSummary, runtimeSummary));
    verify(jobTemplateManager, never()).loadAncestorJobTypes(any(), anyString(), any());
  }

  @Test
  public void testTerminateResolvesAncestorsWithRuntimeSummarySubTypeVersion() {
    StepRuntimeSummary derivedSummary =
        StepRuntimeSummary.builder()
            .stepId("test")
            .stepAttemptId(1)
            .stepInstanceUuid("uuid")
            .stepName("testExtension")
            .stepInstanceId(1)
            .type(StepType.EXTENSION)
            .subType("my-spark-job")
            .subTypeVersion("v2")
            .build();
    when(jobTemplateManager.loadAncestorJobTypes(workflowSummary, "my-spark-job", "v2"))
        .thenReturn(List.of("spark"));
    var result = StepRuntime.Result.of(StepRuntime.State.STOPPED);
    when(sparkRuntime.terminate(workflowSummary, derivedSummary)).thenReturn(result);

    assertSame(result, runtime.terminate(workflowSummary, derivedSummary));
    verify(jobTemplateManager).loadAncestorJobTypes(workflowSummary, "my-spark-job", "v2");
  }

  @Test
  public void testInjectRuntimeParamsAndTags() {
    Map<String, ParamDefinition> params =
        Map.of("spark_conf", buildParam("spark_conf", "x").toDefinition());
    List<Tag> tags = List.of(Tag.create("spark"));
    when(sparkRuntime.injectRuntimeParams(workflowSummary, step)).thenReturn(params);
    when(sparkRuntime.injectRuntimeTags(workflowSummary, step)).thenReturn(tags);

    assertEquals(params, runtime.injectRuntimeParams(workflowSummary, step));
    assertEquals(tags, runtime.injectRuntimeTags(workflowSummary, step));
  }

  @Test
  public void testUnregisteredSubType() {
    step.setSubType("flink");
    when(jobTemplateManager.loadAncestorJobTypes(workflowSummary, "flink", "v2"))
        .thenReturn(List.of("streaming-base"));

    AssertHelper.assertThrows(
        "unregistered sub type should throw",
        NullPointerException.class,
        "Cannot find an extension step runtime for sub type [flink] or its job template ancestors",
        () -> runtime.start(workflowSummary, step, runtimeSummary));
  }

  @Test
  public void testMissingSubType() {
    step.setSubType(null);
    AssertHelper.assertThrows(
        "missing sub type should throw",
        NullPointerException.class,
        "Extension step must define a sub type",
        () -> runtime.execute(workflowSummary, step, runtimeSummary));
  }
}
