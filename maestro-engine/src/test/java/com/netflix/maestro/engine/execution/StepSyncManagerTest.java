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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import java.util.Collections;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StepSyncManagerTest extends MaestroEngineBaseTest {

  private final MaestroStepInstanceDao instanceDao = mock(MaestroStepInstanceDao.class);
  private final StepInstance instance = mock(StepInstance.class);
  private final WorkflowSummary workflowSummary = mock(WorkflowSummary.class);
  private final StepSyncManager syncManager = new StepSyncManager(instanceDao);

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Test
  public void testInsertSync() {
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.INSERT)
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertFalse(details.isPresent());
    verify(instanceDao, times(1)).insertOrUpsertStepInstance(instance, false, null);
  }

  @Test
  public void testUpsertSync() {
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.UPSERT)
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertFalse(details.isPresent());
    verify(instanceDao, times(1)).insertOrUpsertStepInstance(instance, true, null);
  }

  @Test
  public void testUpdateSync() {
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.UPDATE)
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertFalse(details.isPresent());
    verify(instanceDao, times(1)).updateStepInstance(workflowSummary, stepRuntimeSummary, null);
  }

  @Test
  public void testInvalidDbOperation() {
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.DELETE)
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertTrue(details.isPresent());
    assertEquals("Failed to sync a Maestro step state change", details.get().getMessage());
    assertFalse(details.get().getErrors().isEmpty());
    assertEquals(
        "MaestroInternalError: Invalid DB operation: DELETE for step instance [test-summary][2]",
        details.get().getErrors().get(0));
  }

  @Test
  public void testInsertPendingRecords() {
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.INSERT)
            .pendingRecords(
                Collections.singletonList(
                    mock(StepInstanceUpdateJobEvent.StepInstancePendingRecord.class)))
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertFalse(details.isPresent());
    var eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(instanceDao, times(1))
        .insertOrUpsertStepInstance(eq(instance), eq(false), eventCaptor.capture());
    assertEquals(StepInstanceUpdateJobEvent.class, eventCaptor.getValue().getClass());
  }

  @Test
  public void testUpdatePendingRecords() {
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.UPDATE)
            .pendingRecords(
                Collections.singletonList(
                    mock(StepInstanceUpdateJobEvent.StepInstancePendingRecord.class)))
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertFalse(details.isPresent());
    var eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(instanceDao, times(1))
        .updateStepInstance(eq(workflowSummary), eq(stepRuntimeSummary), eventCaptor.capture());
    assertEquals(NotificationJobEvent.class, eventCaptor.getValue().getClass());
  }

  @Test
  public void testSyncFailure() {
    doThrow(new RuntimeException("test error"))
        .when(instanceDao)
        .updateStepInstance(any(), any(), any());
    StepRuntimeSummary stepRuntimeSummary =
        StepRuntimeSummary.builder()
            .stepId("test-summary")
            .stepAttemptId(2)
            .stepInstanceId(1)
            .dbOperation(DbOperation.UPDATE)
            .pendingRecords(
                Collections.singletonList(
                    mock(StepInstanceUpdateJobEvent.StepInstancePendingRecord.class)))
            .build();
    Optional<Details> details = syncManager.sync(instance, workflowSummary, stepRuntimeSummary);
    assertTrue(details.isPresent());
    assertEquals("Failed to sync a Maestro step state change", details.get().getMessage());
  }
}
