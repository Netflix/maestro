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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.TemplateInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.utils.IdHelper;
import java.util.Collections;
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
  private MaestroWorkflowInstanceDao instanceDao;
  private MaestroStepInstanceDao stepDao;
  private String templateInlineWorkflowId;

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
    instanceDao =
        new MaestroWorkflowInstanceDao(DATA_SOURCE, MAPPER, CONFIG, queueSystem, metricRepo);
    stepDao = new MaestroStepInstanceDao(DATA_SOURCE, MAPPER, CONFIG, queueSystem, metricRepo);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflow(DATA_SOURCE, TEST_WORKFLOW_ID1);
    if (templateInlineWorkflowId != null) {
      MaestroTestHelper.removeWorkflowInstance(DATA_SOURCE, templateInlineWorkflowId, 1);
    }
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

  @Test
  public void testDeleteWorkflowDataWithTemplateInlineInstances() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    reset(queueSystem);
    ArgumentCaptor<DeleteWorkflowJobEvent> argumentCaptor =
        ArgumentCaptor.forClass(DeleteWorkflowJobEvent.class);
    workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester"));
    Mockito.verify(queueSystem, times(1)).enqueue(any(), argumentCaptor.capture());
    long internalId = argumentCaptor.getValue().getInternalId();
    templateInlineWorkflowId =
        IdHelper.getInlineWorkflowPrefixId(internalId, StepType.TEMPLATE) + "11_abc";

    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId(templateInlineWorkflowId);
    instance.setWorkflowInstanceId(1L);
    instance.setWorkflowRunId(1L);
    TemplateInitiator initiator = new TemplateInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId(TEST_WORKFLOW_ID1);
    initiator.setAncestors(Collections.singletonList(parent));
    instance.setInitiator(initiator);
    instanceDao.runWorkflowInstances(templateInlineWorkflowId, Collections.singletonList(instance));
    StepInstance si =
        loadObject("fixtures/instances/sample-step-instance-running.json", StepInstance.class);
    si.setWorkflowId(templateInlineWorkflowId);
    stepDao.insertOrUpsertStepInstance(si, false, null);
    assertEquals(
        WorkflowInstance.Status.CREATED,
        instanceDao.getWorkflowInstanceStatus(templateInlineWorkflowId, 1L, 1L));
    assertEquals(
        "job1", stepDao.getStepInstance(templateInlineWorkflowId, 1, 1, "job1", "1").getStepId());

    deletionDao.deleteWorkflowData(TEST_WORKFLOW_ID1, internalId, TimeUnit.MINUTES.toNanos(1));

    assertFalse(deletionDao.isDeletionInProgress(TEST_WORKFLOW_ID1));
    assertNull(instanceDao.getWorkflowInstanceStatus(templateInlineWorkflowId, 1L, 1L));
    AssertHelper.assertThrows(
        "template inline step instance is deleted with the parent workflow",
        MaestroNotFoundException.class,
        "not found",
        () -> stepDao.getStepInstance(templateInlineWorkflowId, 1, 1, "job1", "1"));
  }
}
