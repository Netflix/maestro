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
package com.netflix.maestro.engine.messageprocessors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.processors.DeleteWorkflowJobProcessor;
import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.definition.User;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class DeleteWorkflowJobProcessorTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowDeletionDao deletionDao;
  @Mock private MaestroNotificationPublisher eventClient;

  private final String workflowId = "sample-minimal-wf";
  private DeleteWorkflowJobProcessor processor;
  private DeleteWorkflowJobEvent jobEvent;

  @Before
  public void before() throws Exception {
    processor = new DeleteWorkflowJobProcessor(deletionDao, eventClient, "test");
    jobEvent = DeleteWorkflowJobEvent.create(workflowId, 12345L, User.create("tester"));
  }

  @Test
  public void testStartDeleteWorkflow() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L)).thenReturn(true);
    processor.process(() -> jobEvent);
    verify(eventClient, times(1)).send(any());
    verify(deletionDao, times(1)).deleteWorkflowData(eq(workflowId), eq(12345L), anyLong());
  }

  @Test
  public void testDeleteWorkflowInLateStage() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345)).thenReturn(false);
    processor.process(() -> jobEvent);
    verify(eventClient, times(0)).send(any());
    verify(deletionDao, times(1)).deleteWorkflowData(eq(workflowId), eq(12345L), anyLong());
  }

  @Test
  public void testRetryableError() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L))
        .thenThrow(new MaestroRetryableError("test"));
    AssertHelper.assertThrows(
        "Will retry MaestroRetryableError",
        MaestroRetryableError.class,
        "test",
        () -> processor.process(() -> jobEvent));
  }

  @Test
  public void testNonRetryableError() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L))
        .thenThrow(new MaestroNotFoundException("test"));
    processor.process(() -> jobEvent); // no error
  }

  @Test
  public void testUnexpectedError() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L))
        .thenThrow(new IllegalArgumentException("test"));
    AssertHelper.assertThrows(
        "Will retry unexpected error",
        MaestroRetryableError.class,
        "Failed to delete a workflow and will retry the deletion",
        () -> processor.process(() -> jobEvent));
  }
}
