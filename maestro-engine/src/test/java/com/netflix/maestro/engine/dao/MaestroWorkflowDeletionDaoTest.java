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
package com.netflix.maestro.engine.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;

import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.DeleteWorkflowJobEvent;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class MaestroWorkflowDeletionDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_WORKFLOW_ID1 = "sample-active-wf-with-props";

  @Mock private MaestroQueueSystem queueSystem;

  private MaestroWorkflowDeletionDao deletionDao;
  private MaestroWorkflowDao workflowDao;

  @Before
  public void setUp() {
    deletionDao = new MaestroWorkflowDeletionDao(DATA_SOURCE, MAPPER, CONFIG, metricRepo);
    workflowDao =
        new MaestroWorkflowDao(
            DATA_SOURCE,
            MAPPER,
            CONFIG,
            queueSystem,
            mock(TriggerSubscriptionClient.class),
            metricRepo);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflow(DATA_SOURCE, TEST_WORKFLOW_ID1);
    reset(queueSystem);
  }

  @Test
  public void testIsDeletionInProgress() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertFalse(deletionDao.isDeletionInProgress(TEST_WORKFLOW_ID1));
    workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester"));
    assertTrue(deletionDao.isDeletionInProgress(TEST_WORKFLOW_ID1));
  }

  @Test
  public void testDeleteWorkflowData() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    reset(queueSystem);
    ArgumentCaptor<DeleteWorkflowJobEvent> argumentCaptor =
        ArgumentCaptor.forClass(DeleteWorkflowJobEvent.class);
    workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester"));
    Mockito.verify(queueSystem, times(1)).enqueue(any(), argumentCaptor.capture());
    Mockito.verify(queueSystem, times(1)).notify(any());

    DeleteWorkflowJobEvent deleteWorkflowJobEvent = argumentCaptor.getValue();
    assertEquals(TEST_WORKFLOW_ID1, deleteWorkflowJobEvent.getWorkflowId());
    assertEquals("tester", deleteWorkflowJobEvent.getAuthor().getName());

    deletionDao.deleteWorkflowData(
        TEST_WORKFLOW_ID1, deleteWorkflowJobEvent.getInternalId(), TimeUnit.MINUTES.toNanos(1));
    assertFalse(deletionDao.isDeletionInProgress(TEST_WORKFLOW_ID1));
  }
}
