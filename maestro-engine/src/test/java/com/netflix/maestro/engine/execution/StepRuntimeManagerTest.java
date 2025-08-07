/*
 * Copyright 2024 Netflix, Inc.
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
package com.netflix.maestro.engine.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.params.DefaultParamManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.DefaultArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringMapParamDefinition;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class StepRuntimeManagerTest extends MaestroEngineBaseTest {

  private WorkflowSummary workflowSummary;
  private StepRuntimeSummary runtimeSummary;
  private StepRuntimeManager runtimeManager;
  private final AtomicInteger count = new AtomicInteger(0);
  private final Artifact artifact = DefaultArtifact.create("foo", "bar");
  private @Mock DefaultParamManager defaultParamManager;

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Before
  public void setUp() {
    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("abc");
    workflowSummary.setWorkflowInstanceId(123);
    runtimeSummary =
        StepRuntimeSummary.builder().stepId("step1").stepInstanceUuid("uuid123").build();
    defaultParamManager = Mockito.mock(DefaultParamManager.class);
    ParamsManager paramsManager = new ParamsManager(defaultParamManager);
    Map<StepType, StepRuntime> stepRuntimeMap =
        Collections.singletonMap(
            StepType.NOOP,
            new StepRuntime() {
              @Override
              public Result start(
                  WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
                // runtime summary change won't be saved.
                runtimeSummary.addTimeline(TimelineLogEvent.info("hello world"));
                if (count.get() < 1) {
                  return new Result(
                      State.DONE, Collections.singletonMap("test-artifact", artifact), null);
                } else {
                  return Result.of(State.USER_ERROR);
                }
              }

              @Override
              public Result execute(
                  WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
                runtimeSummary.addTimeline(TimelineLogEvent.info("hello world"));
                if (count.get() < 1) {
                  return new Result(
                      State.DONE, Collections.singletonMap("test-artifact", artifact), null);
                } else {
                  return Result.of(State.PLATFORM_ERROR);
                }
              }

              @Override
              public Result terminate(
                  WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
                return new Result(
                    State.STOPPED,
                    Collections.singletonMap("test-artifact", artifact),
                    Collections.singletonList(TimelineLogEvent.info("test termination")));
              }

              @Override
              public Map<String, ParamDefinition> injectRuntimeParams(
                  WorkflowSummary workflowSummary, Step step) {
                return threeItemMap(
                    "test-param",
                    ParamDefinition.buildParamDefinition("test-param", "from-inject"),
                    "injected-param",
                    ParamDefinition.buildParamDefinition("injected-param", "from-inject"),
                    "injected-step-field",
                    ParamDefinition.buildParamDefinition(
                        "injected-step-field", step.getType().toString()));
              }

              @Override
              public List<Tag> injectRuntimeTags() {
                return Collections.singletonList(Tag.create("test"));
              }
            });
    runtimeManager =
        new StepRuntimeManager(stepRuntimeMap, MAPPER, paramsManager, metricRepo, null);
  }

  @Test
  public void testStart() {
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .type(StepType.NOOP)
            .stepRetry(StepInstance.StepRetry.from(Defaults.DEFAULT_RETRY_POLICY))
            .build();
    summary.setPendingAction(StepAction.builder().build());
    assertNotNull(summary.getPendingAction());
    boolean ret = runtimeManager.start(workflowSummary, null, summary);
    assertTrue(ret);
    assertEquals(StepInstance.Status.RUNNING, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getExecuteTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(1, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.NOT_CREATED, summary.getPendingRecords().getFirst().getOldStatus());
    assertEquals(
        StepInstance.Status.RUNNING, summary.getPendingRecords().getFirst().getNewStatus());
    assertEquals(artifact, summary.getArtifacts().get("test-artifact"));
    assertTrue(summary.getTimeline().isEmpty());
    // The pending action should have been cleared after passing it to step runtime
    assertNull(summary.getPendingAction());
  }

  @Test
  public void testStartFailure() {
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setErrorRetryLimit(1);
    stepRetry.setPlatformRetryLimit(1);
    stepRetry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    count.incrementAndGet();
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder().type(StepType.NOOP).stepRetry(stepRetry).build();
    boolean ret = runtimeManager.start(workflowSummary, null, summary);
    assertFalse(ret);
    assertEquals(StepInstance.Status.USER_FAILED, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getEndTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(1, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.NOT_CREATED, summary.getPendingRecords().getFirst().getOldStatus());
    assertEquals(
        StepInstance.Status.USER_FAILED, summary.getPendingRecords().getFirst().getNewStatus());
    assertTrue(summary.getArtifacts().isEmpty());

    stepRetry.incrementByStatus(StepInstance.Status.USER_FAILED);
    ret = runtimeManager.start(workflowSummary, null, summary);
    assertFalse(ret);
    assertEquals(StepInstance.Status.FATALLY_FAILED, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getEndTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(2, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.USER_FAILED, summary.getPendingRecords().get(1).getOldStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED, summary.getPendingRecords().get(1).getNewStatus());
    assertTrue(summary.getArtifacts().isEmpty());
  }

  @Test
  public void testExecute() {
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .type(StepType.NOOP)
            .stepRetry(StepInstance.StepRetry.from(Defaults.DEFAULT_RETRY_POLICY))
            .build();
    summary.setPendingAction(StepAction.builder().build());
    assertNotNull(summary.getPendingAction());
    boolean ret = runtimeManager.execute(workflowSummary, null, summary);
    assertTrue(ret);
    assertEquals(StepInstance.Status.FINISHING, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getFinishTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(1, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.NOT_CREATED, summary.getPendingRecords().getFirst().getOldStatus());
    assertEquals(
        StepInstance.Status.FINISHING, summary.getPendingRecords().getFirst().getNewStatus());
    assertEquals(artifact, summary.getArtifacts().get("test-artifact"));
    assertTrue(summary.getTimeline().isEmpty());
    // The pending action should have been cleared after passing it to step runtime
    assertNull(summary.getPendingAction());
  }

  @Test
  public void testExecuteFailure() {
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setErrorRetryLimit(1);
    stepRetry.setPlatformRetryLimit(1);
    stepRetry.incrementByStatus(StepInstance.Status.USER_FAILED);
    count.incrementAndGet();
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder().type(StepType.NOOP).stepRetry(stepRetry).build();
    boolean ret = runtimeManager.execute(workflowSummary, null, summary);
    assertFalse(ret);
    assertEquals(StepInstance.Status.PLATFORM_FAILED, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getEndTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(1, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.NOT_CREATED, summary.getPendingRecords().getFirst().getOldStatus());
    assertEquals(
        StepInstance.Status.PLATFORM_FAILED, summary.getPendingRecords().getFirst().getNewStatus());
    assertTrue(summary.getArtifacts().isEmpty());

    stepRetry.incrementByStatus(StepInstance.Status.PLATFORM_FAILED);
    ret = runtimeManager.execute(workflowSummary, null, summary);
    assertFalse(ret);
    assertEquals(StepInstance.Status.FATALLY_FAILED, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getEndTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(2, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.PLATFORM_FAILED, summary.getPendingRecords().get(1).getOldStatus());
    assertEquals(
        StepInstance.Status.FATALLY_FAILED, summary.getPendingRecords().get(1).getNewStatus());
    assertTrue(summary.getArtifacts().isEmpty());
  }

  @Test
  public void testTerminate() {
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .type(StepType.NOOP)
            .stepRetry(StepInstance.StepRetry.from(Defaults.DEFAULT_RETRY_POLICY))
            .build();
    summary.setPendingAction(StepAction.builder().build());
    assertNotNull(summary.getPendingAction());
    runtimeManager.terminate(workflowSummary, summary, StepInstance.Status.STOPPED);
    assertEquals(StepInstance.Status.STOPPED, summary.getRuntimeState().getStatus());
    assertNotNull(summary.getRuntimeState().getEndTime());
    assertNotNull(summary.getRuntimeState().getModifyTime());
    assertEquals(1, summary.getPendingRecords().size());
    assertEquals(
        StepInstance.Status.NOT_CREATED, summary.getPendingRecords().getFirst().getOldStatus());
    assertEquals(
        StepInstance.Status.STOPPED, summary.getPendingRecords().getFirst().getNewStatus());
    assertEquals(artifact, summary.getArtifacts().get("test-artifact"));
    assertEquals(1, summary.getTimeline().getTimelineEvents().size());
    assertEquals(
        "test termination", summary.getTimeline().getTimelineEvents().getFirst().getMessage());
    // The pending action should have been cleared after passing it to step runtime
    assertNull(summary.getPendingAction());
  }

  @Test
  public void testGetAllParams() {
    when(defaultParamManager.getDefaultStepParams())
        .thenReturn(
            twoItemMap(
                "default1", ParamDefinition.buildParamDefinition("default1", "d1"),
                "default2", ParamDefinition.buildParamDefinition("default2", "d2")));
    TypedStep testStep = new TypedStep();
    testStep.setId("step1");
    testStep.setParams(
        twoItemMap(
            "foo", ParamDefinition.buildParamDefinition("foo", "bar"),
            "test-param", ParamDefinition.buildParamDefinition("test-param", "hello")));
    testStep.setType(StepType.NOOP);
    testStep.setId("step1");
    Map<String, Parameter> params =
        runtimeManager.getAllParams(testStep, workflowSummary, runtimeSummary);
    assertTrue(params.size() >= 4);
    assertEquals("bar", params.get("foo").getValue());
    assertEquals("hello", params.get("test-param").getValue());
    assertEquals("d1", params.get("default1").getValue());
    assertEquals("d2", params.get("default2").getValue());
  }

  @Test
  public void testGetAllParamsOverrideOrder() {
    when(this.defaultParamManager.getDefaultStepParams())
        .thenReturn(
            twoItemMap(
                "foo",
                ParamDefinition.buildParamDefinition("foo", "from-default"),
                "test-param",
                ParamDefinition.buildParamDefinition("test-param", "from-other-default")));
    TypedStep testStep = new TypedStep();
    testStep.setParams(
        twoItemMap(
            "foo", ParamDefinition.buildParamDefinition("foo", "bar-from-step"),
            "test-param", ParamDefinition.buildParamDefinition("test-param", "hello-from-step")));
    testStep.setType(StepType.NOOP);
    testStep.setId("step1");
    Map<String, Parameter> params =
        runtimeManager.getAllParams(testStep, workflowSummary, runtimeSummary);
    assertTrue(params.size() >= 3);
    assertEquals("bar-from-step", params.get("foo").getValue());
    assertEquals("hello-from-step", params.get("test-param").getValue());
    assertEquals("from-inject", params.get("injected-param").getValue());
    assertEquals("NOOP", params.get("injected-step-field").getValue());
  }

  @Test
  public void testMergeNestedParamMap() {
    when(this.defaultParamManager.getDefaultStepParams())
        .thenReturn(
            Map.of(
                "nested-default-new",
                MapParamDefinition.builder()
                    .name("nested-default-new")
                    .value(
                        singletonMap(
                            "default-new",
                            ParamDefinition.buildParamDefinition("default-new", "from-default")))
                    .build(),
                "nested-common",
                MapParamDefinition.builder()
                    .name("nested-common")
                    .value(
                        new HashMap<>(
                            Map.of(
                                "default-param",
                                ParamDefinition.buildParamDefinition(
                                    "default-param", "from-default"),
                                "common-param",
                                ParamDefinition.buildParamDefinition(
                                    "common-param", "from-default"),
                                "double-nested-common",
                                MapParamDefinition.builder()
                                    .name("nested-common")
                                    .value(
                                        twoItemMap(
                                            "default-param",
                                            ParamDefinition.buildParamDefinition(
                                                "default-param", "from-default"),
                                            "common-param",
                                            ParamDefinition.buildParamDefinition(
                                                "common-param", "from-default")))
                                    .build(),
                                "double-nested-common-string",
                                StringMapParamDefinition.builder()
                                    .name("nested-common")
                                    .value(
                                        twoItemMap(
                                            "default-param", "from-default",
                                            "common-param", "from-default"))
                                    .build())))
                    .build(),
                "foo",
                ParamDefinition.buildParamDefinition("foo", "some-default"),
                "test-param",
                ParamDefinition.buildParamDefinition("test-param", "some-other-default")));
    TypedStep testStep = new TypedStep();
    testStep.setParams(
        Map.of(
            "nested-step-new",
            MapParamDefinition.builder()
                .name("nested-step-new")
                .value(
                    singletonMap(
                        "step-new", ParamDefinition.buildParamDefinition("step-new", "from-step")))
                .build(),
            "nested-common",
            MapParamDefinition.builder()
                .name("nested-common")
                .value(
                    new HashMap<>(
                        Map.of(
                            "step-param",
                            ParamDefinition.buildParamDefinition("step-param", "from-step"),
                            "common-param",
                            ParamDefinition.buildParamDefinition("common-param", "from-step"),
                            "double-nested-common",
                            MapParamDefinition.builder()
                                .name("nested-common")
                                .value(
                                    twoItemMap(
                                        "step-param",
                                        ParamDefinition.buildParamDefinition(
                                            "default-param", "from-step"),
                                        "common-param",
                                        ParamDefinition.buildParamDefinition(
                                            "common-param", "from-step")))
                                .build(),
                            "double-nested-common-string",
                            StringMapParamDefinition.builder()
                                .name("nested-common")
                                .value(
                                    twoItemMap(
                                        "step-param", "from-step",
                                        "common-param", "from-step"))
                                .build())))
                .build(),
            "foo",
            ParamDefinition.buildParamDefinition("foo", "bar"),
            "test-param",
            ParamDefinition.buildParamDefinition("test-param", "hello")));
    testStep.setType(StepType.NOOP);
    testStep.setId("step1");
    Map<String, Parameter> params =
        runtimeManager.getAllParams(testStep, workflowSummary, runtimeSummary);
    assertEquals("bar", params.get("foo").getValue());
    assertEquals("hello", params.get("test-param").getValue());
    assertEquals(
        "from-step",
        ((MapParameter) params.get("nested-step-new")).getValue().get("step-new").getValue());
    assertEquals(
        "from-default",
        ((MapParameter) params.get("nested-default-new")).getValue().get("default-new").getValue());
    assertEquals(
        "from-default",
        ((MapParameter) params.get("nested-common")).getValue().get("default-param").getValue());
    assertEquals(
        "from-step",
        ((MapParameter) params.get("nested-common")).getValue().get("step-param").getValue());
    assertEquals(
        "from-step",
        ((MapParameter) params.get("nested-common")).getValue().get("common-param").getValue());

    MapParamDefinition nestedMap =
        (MapParamDefinition)
            ((MapParameter) params.get("nested-common")).getValue().get("double-nested-common");
    assertEquals("from-default", nestedMap.getValue().get("default-param").getValue());
    assertEquals("from-step", nestedMap.getValue().get("step-param").getValue());
    assertEquals("from-step", nestedMap.getValue().get("common-param").getValue());

    StringMapParamDefinition nestedStringMap =
        (StringMapParamDefinition)
            ((MapParameter) params.get("nested-common"))
                .getValue()
                .get("double-nested-common-string");
    assertEquals("from-default", nestedStringMap.getValue().get("default-param"));
    assertEquals("from-step", nestedStringMap.getValue().get("step-param"));
    assertEquals("from-step", nestedStringMap.getValue().get("common-param"));
  }

  @Test
  public void testMergeNestedStringMap() {
    when(this.defaultParamManager.getDefaultStepParams())
        .thenReturn(
            Map.of(
                "nested-default-new",
                StringMapParamDefinition.builder()
                    .name("nested-default-new")
                    .value(singletonMap("default-new", "from-default"))
                    .build(),
                "nested-common",
                StringMapParamDefinition.builder()
                    .name("nested-common")
                    .value(
                        twoItemMap(
                            "default-param", "from-default",
                            "common-param", "from-default"))
                    .build(),
                "foo",
                ParamDefinition.buildParamDefinition("foo", "some-default"),
                "test-param",
                ParamDefinition.buildParamDefinition("test-param", "some-other-default")));
    TypedStep testStep = new TypedStep();
    testStep.setParams(
        Map.of(
            "nested-step-new",
            StringMapParamDefinition.builder()
                .name("nested-step-new")
                .value(singletonMap("step-new", "from-step"))
                .build(),
            "nested-common",
            StringMapParamDefinition.builder()
                .name("nested-common")
                .value(
                    twoItemMap(
                        "step-param", "from-step",
                        "common-param", "from-step"))
                .build(),
            "foo",
            ParamDefinition.buildParamDefinition("foo", "bar"),
            "test-param",
            ParamDefinition.buildParamDefinition("test-param", "hello")));
    testStep.setType(StepType.NOOP);
    testStep.setId("step1");
    Map<String, Parameter> params =
        runtimeManager.getAllParams(testStep, workflowSummary, runtimeSummary);
    assertEquals("bar", params.get("foo").getValue());
    assertEquals("hello", params.get("test-param").getValue());
    assertEquals(
        "from-step", params.get("nested-step-new").asStringMapParam().getValue().get("step-new"));
    assertEquals(
        "from-default",
        params.get("nested-default-new").asStringMapParam().getValue().get("default-new"));
    assertEquals(
        "from-default",
        params.get("nested-common").asStringMapParam().getValue().get("default-param"));
    assertEquals(
        "from-step", params.get("nested-common").asStringMapParam().getValue().get("step-param"));
    assertEquals(
        "from-step", params.get("nested-common").asStringMapParam().getValue().get("common-param"));
  }

  @Test(expected = MaestroValidationException.class)
  public void testMergeNestedParamMismatchTypes() {
    when(this.defaultParamManager.getDefaultStepParams())
        .thenReturn(
            Collections.singletonMap(
                "nested-mismatch", ParamDefinition.buildParamDefinition("foo", "bar")));
    TypedStep testStep = new TypedStep();
    testStep.setParams(
        singletonMap(
            "nested-mismatch",
            MapParamDefinition.builder()
                .name("nested-mismatch")
                .value(
                    Collections.singletonMap(
                        "default-new",
                        ParamDefinition.buildParamDefinition("default-new", "from-default")))
                .build()));
    testStep.setType(StepType.NOOP);
    testStep.setId("step1");
    runtimeManager.getAllParams(testStep, workflowSummary, runtimeSummary);
  }
}
