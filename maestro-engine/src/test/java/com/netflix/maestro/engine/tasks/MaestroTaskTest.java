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
package com.netflix.maestro.engine.tasks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaestroTaskTest extends MaestroEngineBaseTest {

  private MaestroTask maestroTask;

  @Before
  public void setup() {
    maestroTask = mock(MaestroTask.class);
    doCallRealMethod().when(maestroTask).updateRetryDelayTimeToTimeline(any());
    when(maestroTask.isStepSkipped(any(), any())).thenCallRealMethod();
  }

  @Test
  public void testUpdateRetryDelayTimeToTimeline() {
    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.USER_FAILED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    RetryPolicy.FixedBackoff fixedBackoff =
        RetryPolicy.FixedBackoff.builder()
            .errorRetryBackoffInSecs(100L)
            .platformRetryBackoffInSecs(200L)
            .build();
    stepRetry.setBackoff(fixedBackoff);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();

    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    List<TimelineEvent> timelineEvents = timeline.getTimelineEvents();
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [1m 40s]"));

    RetryPolicy.ExponentialBackoff exponentialBackoff =
        RetryPolicy.ExponentialBackoff.builder()
            .errorRetryExponent(2)
            .errorRetryLimitInSecs(600L)
            .errorRetryBackoffInSecs(100L)
            .platformRetryBackoffInSecs(200L)
            .build();
    stepRetry.setBackoff(exponentialBackoff);
    stepRetry.setErrorRetries(6);
    timelineEvents.clear();
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [10m]"));

    timelineEvents.clear();
    runtimeState.setStatus(StepInstance.Status.PAUSED);
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents).isEmpty();
  }

  @Test
  public void testUpdateTimeoutRetryDelayTimeToTimeline() {
    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.TIMEOUT_FAILED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    RetryPolicy.FixedBackoff fixedBackoff =
        RetryPolicy.FixedBackoff.builder().timeoutRetryBackoffInSecs(200L).build();
    stepRetry.setBackoff(fixedBackoff);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();

    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    List<TimelineEvent> timelineEvents = timeline.getTimelineEvents();
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [3m 20s]"));

    RetryPolicy.ExponentialBackoff exponentialBackoff =
        RetryPolicy.ExponentialBackoff.builder()
            .timeoutRetryExponent(2)
            .timeoutRetryLimitInSecs(600L)
            .timeoutRetryBackoffInSecs(100L)
            .build();
    stepRetry.setBackoff(exponentialBackoff);
    stepRetry.setTimeoutRetries(6);
    timelineEvents.clear();
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents)
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Retrying task in [10m]"));

    timelineEvents.clear();
    runtimeState.setStatus(StepInstance.Status.PAUSED);
    maestroTask.updateRetryDelayTimeToTimeline(runtimeSummary);
    assertThat(timelineEvents).isEmpty();
  }

  @Test
  public void testIsStepSkipped() {
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowId("test-workflow");
    summary.setWorkflowInstanceId(1L);
    summary.setWorkflowRunId(1L);

    StepRuntimeState runtimeState = new StepRuntimeState();
    runtimeState.setStatus(StepInstance.Status.USER_FAILED);
    Timeline timeline = new Timeline(new ArrayList<>());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("step1")
            .timeline(timeline)
            .runtimeState(runtimeState)
            .stepRetry(stepRetry)
            .build();
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(RestartConfig.builder().build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .addRestartNode("test-workflow", 2, "step1")
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .skipSteps(Collections.singleton("step2"))
            .stepRestartParams(Collections.singletonMap("step2", Collections.emptyMap()))
            .build());
    Assert.assertFalse(maestroTask.isStepSkipped(summary, runtimeSummary));

    summary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .skipSteps(Collections.singleton("step1"))
            .stepRestartParams(Collections.singletonMap("step1", Collections.emptyMap()))
            .build());
    Assert.assertTrue(maestroTask.isStepSkipped(summary, runtimeSummary));
    assertThat(timeline.getTimelineEvents())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Step is skipped because of a user skip action."));
    Assert.assertTrue(stepRetry.isRetryable());
    Assert.assertEquals(DbOperation.UPSERT, runtimeSummary.getDbOperation());
    Assert.assertEquals(StepInstance.Status.SKIPPED, runtimeState.getStatus());
    assertThat(runtimeSummary.getPendingRecords())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("eventTime")
        .contains(
            StepInstanceUpdateJobEvent.createRecord(
                StepInstance.Status.USER_FAILED, StepInstance.Status.SKIPPED, 0L));

    summary.setRestartConfig(null);
    runtimeSummary.getPendingRecords().clear();
    runtimeState.setStatus(StepInstance.Status.USER_FAILED);
    runtimeSummary.setRestartConfig(
        RestartConfig.builder()
            .addRestartNode("test-workflow", 1, "step1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .skipSteps(Collections.singleton("step1"))
            .stepRestartParams(Collections.singletonMap("step1", Collections.emptyMap()))
            .build());
    Assert.assertTrue(maestroTask.isStepSkipped(summary, runtimeSummary));
    assertThat(timeline.getTimelineEvents())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp")
        .contains(TimelineLogEvent.info("Step is skipped because of a user skip action."));
    Assert.assertTrue(stepRetry.isRetryable());
    Assert.assertEquals(DbOperation.UPSERT, runtimeSummary.getDbOperation());
    Assert.assertEquals(StepInstance.Status.SKIPPED, runtimeState.getStatus());
    assertThat(runtimeSummary.getPendingRecords())
        .hasSize(1)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("eventTime")
        .contains(
            StepInstanceUpdateJobEvent.createRecord(
                StepInstance.Status.USER_FAILED, StepInstance.Status.SKIPPED, 0L));
  }
}
