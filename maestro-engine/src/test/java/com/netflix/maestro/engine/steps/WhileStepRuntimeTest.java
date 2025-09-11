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
package com.netflix.maestro.engine.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.models.artifact.Artifact.Type;
import com.netflix.maestro.models.artifact.WhileArtifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.WhileStep;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.queue.MaestroQueueSystem;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class WhileStepRuntimeTest extends MaestroEngineBaseTest {
  private static final String WORKFLOW_ID = "fake_workflow_id";
  private static final long INSTANCE_ID = 1L;
  private static final long RUN_ID = 1L;
  private static final String STEP_ID = "fake_step_id";
  private static final long STEP_ATTEMPT_ID = 1L;

  @Mock private WorkflowActionHandler workflowActionHandler;
  @Mock private MaestroWorkflowInstanceDao workflowInstanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;
  @Mock private ParamEvaluator paramEvaluator;

  private WorkflowSummary workflowSummary;
  private WhileStepRuntime whileStepRuntime;

  @Before
  public void setup() {
    whileStepRuntime =
        new WhileStepRuntime(
            workflowActionHandler,
            workflowInstanceDao,
            stepInstanceDao,
            queueSystem,
            instanceStepConcurrencyHandler,
            paramEvaluator);

    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId(WORKFLOW_ID);
    workflowSummary.setWorkflowInstanceId(INSTANCE_ID);
    workflowSummary.setWorkflowRunId(RUN_ID);
    workflowSummary.setInitiator(new ManualInitiator());
    workflowSummary.setInternalId(12345L);
    workflowSummary.setRunPolicy(RunPolicy.START_FRESH_NEW_RUN);
    workflowSummary.setParams(new HashMap<>());
  }

  @Test
  public void testStartFreshRun() {
    WhileStep step = createWhileStep("count < 3", Collections.emptyList());
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary();

    StepRuntime.Result result = whileStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    assertNotNull(result.artifacts().get(Type.WHILE.key()));
    WhileArtifact artifact = result.artifacts().get(Type.WHILE.key()).asWhile();

    assertEquals(1L, artifact.getFirstIteration());
    assertEquals(0L, artifact.getLastIteration());
    assertEquals(RUN_ID, artifact.getRunId());
    assertEquals(1L, artifact.getLoopRunId());
    assertTrue(artifact.isFreshRun());
    assertNotNull(artifact.getLoopWorkflowId());
    assertNotNull(artifact.getLoopIdentity());
    assertNotNull(artifact.getRollup());
    assertNotNull(artifact.getLoopParamValues());
  }

  @Test
  public void testStartWithPreviousArtifact() {
    WhileStep step = createWhileStep("count < 3", Collections.emptyList());
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary();
    runtimeSummary.getStepRetry().setRetryable(false);
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);

    WhileArtifact prevArtifact = new WhileArtifact();
    prevArtifact.setLoopRunId(2L);
    prevArtifact.setLastIteration(5L);
    prevArtifact.setRollup(new WorkflowRollupOverview());
    prevArtifact.setLoopParamValues(new LinkedHashMap<>());

    when(stepInstanceDao.getLatestWhileArtifact(WORKFLOW_ID, INSTANCE_ID, STEP_ID))
        .thenReturn(prevArtifact);

    StepRuntime.Result result = whileStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    WhileArtifact artifact = result.artifacts().get(Type.WHILE.key()).asWhile();

    assertEquals(3L, artifact.getLoopRunId()); // incremented
    assertEquals(5L, artifact.getFirstIteration()); // restart from incomplete
    assertEquals(4L, artifact.getLastIteration()); // last iteration - 1
  }

  @Test
  public void testTerminateWithoutArtifact() {
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary();

    StepRuntime.Result result = whileStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertTrue(result.artifacts().isEmpty());
  }

  private WhileStep createWhileStep(
      String condition, java.util.List<com.netflix.maestro.models.definition.Step> steps) {
    WhileStep step = new WhileStep();
    step.setId(STEP_ID);
    step.setCondition(condition);
    step.setSteps(steps);
    step.setParams(new HashMap<>());
    return step;
  }

  private StepRuntimeSummary createStepRuntimeSummary() {
    Map<String, Parameter> params = new HashMap<>();

    Map<String, Object> loopParamValues = new LinkedHashMap<>();
    loopParamValues.put("count", 1);

    MapParameter loopParams =
        MapParameter.builder()
            .evaluatedResult(loopParamValues)
            .evaluatedTime(System.currentTimeMillis())
            .build();
    params.put("loop_params", loopParams);

    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);

    return StepRuntimeSummary.builder()
        .stepId(STEP_ID)
        .stepAttemptId(STEP_ATTEMPT_ID)
        .type(StepType.WHILE)
        .artifacts(new HashMap<>())
        .params(params)
        .stepRetry(stepRetry)
        .build();
  }
}
