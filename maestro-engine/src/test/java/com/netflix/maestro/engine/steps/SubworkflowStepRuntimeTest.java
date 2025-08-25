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
package com.netflix.maestro.engine.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowInstanceActionHandler;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.SubworkflowStep;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class SubworkflowStepRuntimeTest extends MaestroBaseTest {

  private SubworkflowStepRuntime subworkflowStepRuntime;

  @Mock private WorkflowActionHandler workflowActionHandler;
  @Mock private WorkflowInstanceActionHandler instanceActionHandler;
  @Mock private InstanceStepConcurrencyHandler concurrencyHandler;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private SubworkflowStep step;

  @Before
  public void setUp() {
    subworkflowStepRuntime =
        new SubworkflowStepRuntime(
            workflowActionHandler,
            instanceActionHandler,
            concurrencyHandler,
            instanceDao,
            stepInstanceDao,
            queueSystem,
            Collections.emptySet());
  }

  @Test
  public void testSubworkflowLaunchDuplicateInstance() {
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("test-workflow");
    workflowSummary.setWorkflowInstanceId(1L);
    workflowSummary.setWorkflowRunId(1L);
    workflowSummary.setRunPolicy(null);
    workflowSummary.setParams(Collections.emptyMap());
    workflowSummary.setRunPolicy(RunPolicy.START_FRESH_NEW_RUN);

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-step")
            .params(
                Map.of(
                    "subworkflow_id",
                    StringParameter.builder()
                        .evaluatedTime(12345L)
                        .evaluatedResult("sub-workflow")
                        .build(),
                    "subworkflow_version",
                    StringParameter.builder().evaluatedTime(12345L).evaluatedResult("1").build()))
            .type(StepType.SUBWORKFLOW)
            .stepRetry(StepInstance.StepRetry.from(null))
            .build();

    String subworkflowUuid = "existing-uuid-123";
    RunResponse duplicatedResponse =
        RunResponse.builder()
            .status(RunResponse.Status.DUPLICATED)
            .workflowUuid(subworkflowUuid)
            .build();

    WorkflowInstance duplicatedInstance = new WorkflowInstance();
    duplicatedInstance.setWorkflowId("sub-workflow");
    duplicatedInstance.setWorkflowVersionId(1L);
    duplicatedInstance.setWorkflowInstanceId(100L);
    duplicatedInstance.setWorkflowRunId(1L);
    duplicatedInstance.setWorkflowUuid(subworkflowUuid);
    var initiator = new SubworkflowInitiator();
    initiator.setAncestors(List.of(new UpstreamInitiator.Info()));
    duplicatedInstance.setInitiator(initiator);

    when(concurrencyHandler.addInstance(any(RunRequest.class))).thenReturn(true);
    when(workflowActionHandler.start(anyString(), anyString(), any(RunRequest.class)))
        .thenReturn(duplicatedResponse);
    when(instanceDao.getWorkflowInstanceRunByUuid(anyString(), anyString()))
        .thenReturn(duplicatedInstance);

    StepRuntime.Result result =
        subworkflowStepRuntime.execute(workflowSummary, step, runtimeSummary);

    verify(instanceDao, times(1)).getWorkflowInstanceRunByUuid("sub-workflow", "existing-uuid-123");
    assertEquals(StepRuntime.State.CONTINUE, result.getState());
    assertNotNull(result.getArtifacts());

    SubworkflowArtifact artifact =
        (SubworkflowArtifact) result.getArtifacts().get(Artifact.Type.SUBWORKFLOW.key());
    assertNotNull(artifact);
    assertEquals("sub-workflow", artifact.getSubworkflowId());
    assertEquals(1L, artifact.getSubworkflowVersionId());
    assertEquals(100L, artifact.getSubworkflowInstanceId());
    assertEquals(1L, artifact.getSubworkflowRunId());
    assertEquals(subworkflowUuid, artifact.getSubworkflowUuid());

    assertEquals(1, result.getTimeline().size());
    TimelineLogEvent logEvent = (TimelineLogEvent) result.getTimeline().getFirst();
    assertEquals("Started a subworkflow with uuid: " + subworkflowUuid, logEvent.getMessage());
  }
}
