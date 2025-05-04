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
package com.netflix.maestro.engine.processors;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.queue.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class DeleteWorkflowJobEventProcessorTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowDeletionDao deletionDao;

  private final String workflowId = "sample-minimal-wf";
  private DeleteWorkflowJobEventProcessor processor;
  private DeleteWorkflowJobEvent jobEvent;

  @Before
  public void before() throws Exception {
    processor = new DeleteWorkflowJobEventProcessor(deletionDao, "test");
    jobEvent = DeleteWorkflowJobEvent.create(workflowId, 12345L, User.create("tester"));
  }

  @Test
  public void testStartDeleteWorkflow() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L)).thenReturn(true);
    var actual = processor.process(jobEvent);
    Assert.assertTrue(actual.isPresent());
    Assert.assertEquals(jobEvent, ((NotificationJobEvent) actual.get()).getEvent());
    verify(deletionDao, times(1)).deleteWorkflowData(eq(workflowId), eq(12345L), anyLong());
  }

  @Test
  public void testDeleteWorkflowInLateStage() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345)).thenReturn(false);
    Assert.assertTrue(processor.process(jobEvent).isEmpty());
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
        () -> processor.process(jobEvent));
  }

  @Test
  public void testNonRetryableError() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L))
        .thenThrow(new MaestroNotFoundException("test"));
    Assert.assertTrue(processor.process(jobEvent).isEmpty()); // no error
  }

  @Test
  public void testUnexpectedError() {
    when(deletionDao.isDeletionInitialized(workflowId, 12345L))
        .thenThrow(new IllegalArgumentException("test"));
    AssertHelper.assertThrows(
        "Will retry unexpected error",
        MaestroRetryableError.class,
        "Failed to delete a workflow and will retry the deletion",
        () -> processor.process(jobEvent));
  }
}
