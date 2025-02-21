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
package com.netflix.maestro.timetrigger.messageprocessors;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.timetrigger.Constants;
import com.netflix.maestro.timetrigger.models.PlannedTimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import com.netflix.maestro.timetrigger.properties.TimeTriggerProperties;
import com.netflix.maestro.timetrigger.utils.MaestroWorkflowLauncher;
import com.netflix.maestro.timetrigger.utils.TimeTriggerExecutionPlanner;
import com.netflix.spectator.api.DefaultRegistry;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TimeTriggerExecutionProcessorTest extends MaestroBaseTest {
  private TimeTriggerProducer triggerProducer;
  private TimeTriggerProperties props;
  private TimeTriggerExecution message;
  private MaestroWorkflowLauncher workflowLauncher;
  private TimeTriggerExecutionPlanner executionPlanner;

  private TimeTriggerExecutionProcessor processor;

  @Before
  public void setUp() {
    triggerProducer = Mockito.mock(TimeTriggerProducer.class);
    workflowLauncher = Mockito.mock(MaestroWorkflowLauncher.class);
    executionPlanner = Mockito.mock(TimeTriggerExecutionPlanner.class);
    props = new TimeTriggerProperties();
    props.setTriggerBatchSize(10);
    props.setMaxTriggersPerMessage(100);
    processor =
        new TimeTriggerExecutionProcessor(
            triggerProducer,
            workflowLauncher,
            executionPlanner,
            props,
            new MaestroMetricRepo(new DefaultRegistry()));

    CronTimeTrigger trigger = new CronTimeTrigger();
    trigger.setCron("*/5 * * * *");
    message =
        TimeTriggerExecution.builder()
            .timeTriggersWithWatermarks(
                List.of(
                    TimeTriggerWithWatermark.builder()
                        .timeTrigger(trigger)
                        .lastTriggerTimestamp(System.currentTimeMillis())
                        .build()))
            .workflowId("wfid")
            .workflowVersion(Constants.TIME_TRIGGER_WORKFLOW_VERSION)
            .build();

    when(executionPlanner.calculateEarliestExecutionDate(any(), any()))
        .thenReturn(Optional.of(new Date()));
    when(executionPlanner.constructNextExecution(any(), any()))
        .thenReturn(TimeTriggerExecution.builder().build());
  }

  /** Enqueue future execution with no current tasks to trigger. */
  @Test
  public void testEnqueueFutureExecutionNoTriggers() {
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any()))
        .thenReturn(new ArrayList<>());
    processor.process(() -> message);
    Mockito.verify(triggerProducer, Mockito.times(1)).push(any(), anyInt());
    Mockito.verifyNoInteractions(workflowLauncher);
  }

  /** Trigger tasks when there are ones to run, and enqueue next message. */
  @Test
  public void testTriggerWorkflows() {
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    when(workflowLauncher.startWorkflowBatchRuns(any(), any(), any(), any()))
        .thenReturn(List.of(RunResponse.builder().build()));
    processor.process(() -> message);
    Mockito.verify(triggerProducer, Mockito.times(1)).push(any(), anyInt());
    Mockito.verify(workflowLauncher, Mockito.times(1))
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());
  }

  /** Enqueue future workflow tasks larger than batch size to trigger. */
  @Test
  public void testTriggerWorkflowsInBatches() {
    props.setTriggerBatchSize(2);
    List<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      plannedList.add(new PlannedTimeTriggerExecution(null, null));
    }
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    when(workflowLauncher.startWorkflowBatchRuns(any(), any(), any(), any()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build()));
    processor.process(() -> message);
    Mockito.verify(triggerProducer, Mockito.times(1)).push(any(), anyInt());
    Mockito.verify(workflowLauncher, Mockito.times(6))
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());
  }

  /** Enqueue future workflow tasks larger than total trigger size. */
  @Test
  public void testTriggerWorkflowsMoreThanMax() {
    props.setTriggerBatchSize(2);
    props.setMaxTriggersPerMessage(7);
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      plannedList.add(new PlannedTimeTriggerExecution(null, null));
    }
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    when(workflowLauncher.startWorkflowBatchRuns(any(), any(), any(), any()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build(), RunResponse.builder().build()))
        .thenReturn(List.of(RunResponse.builder().build()));
    processor.process(() -> message);
    Mockito.verify(triggerProducer, Mockito.times(1)).push(any(), anyInt());
    Mockito.verify(workflowLauncher, Mockito.times(4))
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());
  }

  /**
   * Enqueue future workflow tasks but with large delay. It should get reduced to max delay amount.
   */
  @Test
  public void testEnqueueFutureWorkflowWithLongDelay() {
    props.setMaxDelay(60 * 15);
    props.setMaxJitter(60);
    when(executionPlanner.calculateEarliestExecutionDate(any(), any()))
        .thenReturn(Optional.of(Date.from(ZonedDateTime.now().plusYears(1).toInstant())));
    ArgumentCaptor<Integer> durationCapture = ArgumentCaptor.forClass(Integer.class);
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    when(workflowLauncher.startWorkflowBatchRuns(any(), any(), any(), any()))
        .thenReturn(List.of(RunResponse.builder().build()));
    processor.process(() -> message);
    Mockito.verify(triggerProducer, Mockito.times(1)).push(any(), durationCapture.capture());
    Mockito.verify(workflowLauncher, Mockito.times(1))
        .startWorkflowBatchRuns(any(), anyString(), any(), any());
    assertTrue(durationCapture.getValue() < props.getMaxDelay());
  }

  /** Test situation when workflow has no executions left. */
  @Test
  public void testEnqueueWithNoExecutionLeft() {
    props.setMaxDelay(60 * 15);
    props.setMaxJitter(60);
    when(executionPlanner.calculateEarliestExecutionDate(any(), any()))
        .thenReturn(Optional.empty());
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    when(workflowLauncher.startWorkflowBatchRuns(any(), any(), any(), any()))
        .thenReturn(List.of(RunResponse.builder().build()));
    processor.process(() -> message);
    Mockito.verify(workflowLauncher, Mockito.times(1))
        .startWorkflowBatchRuns(any(), anyString(), any(), any());
    Mockito.verifyNoInteractions(triggerProducer);
  }

  /** Test workflow not found error when triggering. */
  @Test
  public void testWorkflowNotFoundErrorOnTrigger() {
    List<String> expectedMessages =
        List.of(
            "Workflow [wfid] has not been created yet or has been deleted.",
            "Cannot find an active version for workflow [wfid]");
    for (String errorMessage : expectedMessages) {
      ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
      plannedList.add(new PlannedTimeTriggerExecution(null, null));
      when(executionPlanner.calculatePlannedExecutions(any(), any(), any()))
          .thenReturn(plannedList);
      Mockito.doThrow(new MaestroNotFoundException(errorMessage))
          .when(workflowLauncher)
          .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());
      processor.process(() -> message);
      Mockito.verifyNoInteractions(triggerProducer);
    }
  }

  /** Test unknown 404 not found error when triggering. */
  @Test
  public void testWorkflowNotFoundUnknownErrorOnTrigger() {
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    Mockito.doThrow(new MaestroNotFoundException("Unknown Not Found"))
        .when(workflowLauncher)
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());

    AssertHelper.assertThrows(
        "should throw exception for 404 unknown error",
        MaestroRetryableError.class,
        "Unknown 404 not found error for",
        () -> processor.process(() -> message));
    Mockito.verifyNoInteractions(triggerProducer);
  }

  /** Test connection error when triggering. */
  @Test
  public void testConnectionError() {
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    Mockito.doThrow(
            new MaestroRuntimeException(
                MaestroRuntimeException.Code.INTERNAL_ERROR, "Connection refused"))
        .when(workflowLauncher)
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());

    AssertHelper.assertThrows(
        "should throw exception for 400",
        MaestroRuntimeException.class,
        "Connection refused",
        () -> processor.process(() -> message));

    Mockito.verifyNoInteractions(triggerProducer);
  }

  /** Test trigger UUID expired error (service should return 409 - conflict). */
  @Test
  public void testTriggerExpiredError() {
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    Mockito.doThrow(new MaestroResourceConflictException("TriggerUUID Expired"))
        .when(workflowLauncher)
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());

    processor.process(() -> message);
    Mockito.verifyNoInteractions(triggerProducer);
  }

  /** Test trigger disabled (service should return 422 - unprocessable). */
  @Test
  public void testTriggerDisabledError() {
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    Mockito.doThrow(
            new MaestroUnprocessableEntityException(
                "Trigger type [TIME] is disabled for "
                    + "the workflow [bla] in the workflow properties."))
        .when(workflowLauncher)
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());

    processor.process(() -> message);
    Mockito.verifyNoInteractions(triggerProducer);
  }

  /** Test trigger unknown 422. */
  @Test
  public void testUnknown422Error() {
    ArrayList<PlannedTimeTriggerExecution> plannedList = new ArrayList<>();
    plannedList.add(new PlannedTimeTriggerExecution(null, null));
    when(executionPlanner.calculatePlannedExecutions(any(), any(), any())).thenReturn(plannedList);
    Mockito.doThrow(new MaestroUnprocessableEntityException("Some other exception message"))
        .when(workflowLauncher)
        .startWorkflowBatchRuns(any(), Mockito.eq("ACTIVE"), any(), any());

    AssertHelper.assertThrows(
        "should throw exception for unknown 422",
        MaestroRetryableError.class,
        "Unknown 422 unprocessable for workflow [wfid][ACTIVE]",
        () -> processor.process(() -> message));

    Mockito.verifyNoInteractions(triggerProducer);
  }
}
