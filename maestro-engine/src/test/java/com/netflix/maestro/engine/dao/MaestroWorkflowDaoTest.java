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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.db.PropertiesUpdate;
import com.netflix.maestro.engine.db.PropertiesUpdate.Type;
import com.netflix.maestro.engine.dto.MaestroWorkflow;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.exceptions.InvalidWorkflowVersionException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroPreconditionFailedException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.WorkflowOverviewResponse;
import com.netflix.maestro.models.definition.Metadata;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.WorkflowTimeline;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.models.trigger.SignalTrigger;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.utils.IdHelper;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class MaestroWorkflowDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_WORKFLOW_ID1 = "sample-active-wf-with-props";
  private static final String TEST_WORKFLOW_ID2 = "sample-minimal-wf";
  private static final String TEST_WORKFLOW_ID3 = "sample-active-wf-with-signal-triggers";
  private static final String TEST_WORKFLOW_ID4 = "sample-active-wf-with-time-triggers";
  private static final String TEST_WORKFLOW_ID5 = "sample-active-wf-with-step-dependencies";
  private static final String TEST_WORKFLOW_ID6 = "sample-active-wf-with-triggers";
  private static final String TEST_WORKFLOW_ID7 = "sample-active-wf-with-output-signals";
  private static final String TEST_WORKFLOW_ID8 = "sample-minimal-wf-with-auth-metadata";
  private static final String TEST_WORKFLOW_ID9 = "sample-minimal-wf-with-tags";
  private static final String TEST_INLINE_WORKFLOW_ID1 =
      Constants.FOREACH_INLINE_WORKFLOW_PREFIX + TEST_WORKFLOW_ID1;
  private static final PropertiesUpdate PROPERTIES_UPDATE =
      new PropertiesUpdate(Type.UPDATE_PROPERTIES);
  private static final PropertiesUpdate PROPERTIES_UPDATE_ADD_TAG =
      new PropertiesUpdate(Type.ADD_WORKFLOW_TAG);
  private static final PropertiesUpdate PROPERTIES_UPDATE_DELETE_TAG =
      new PropertiesUpdate(Type.DELETE_WORKFLOW_TAG);

  private MaestroWorkflowDao workflowDao;
  private MaestroQueueSystem queueSystem;
  private TriggerSubscriptionClient triggerClient;
  private MaestroWorkflowInstanceDao instanceDao;
  private MaestroRunStrategyDao runStrategyDao;

  @Before
  public void setUp() {
    queueSystem = mock(MaestroQueueSystem.class);
    triggerClient = mock(TriggerSubscriptionClient.class);
    workflowDao =
        new MaestroWorkflowDao(dataSource, MAPPER, config, queueSystem, triggerClient, metricRepo);
    instanceDao =
        new MaestroWorkflowInstanceDao(dataSource, MAPPER, config, queueSystem, metricRepo);
    runStrategyDao = new MaestroRunStrategyDao(dataSource, MAPPER, config, queueSystem, metricRepo);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID1);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID2);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID3);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID4);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID5);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID6);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID7);
    MaestroTestHelper.removeWorkflow(dataSource, TEST_WORKFLOW_ID8);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID1, 1);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID1, 2);
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_INLINE_WORKFLOW_ID1, 1);
  }

  @Test
  public void testAddWorkflowDefinition() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertEquals(wfd, definition);
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    verify(triggerClient, times(0)).upsertTriggerSubscription(any(), any(), any(), any());
  }

  @Test
  public void testAddBackDeletedWorkflowDef() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId(TEST_WORKFLOW_ID1);
    runStrategyDao.startWithRunStrategy(instance, RunStrategy.create("SEQUENTIAL"));
    MaestroTestHelper.deleteWorkflow(dataSource, TEST_WORKFLOW_ID1);
    WorkflowDefinition wfdAnother = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(
        wfdAnother, wfdAnother.getPropertiesSnapshot().extractProperties());
    WorkflowOverviewResponse response = workflowDao.getWorkflowOverview(TEST_WORKFLOW_ID1);
    assertEquals(Long.valueOf(1), response.getLatestInstanceId());
  }

  @Test
  public void testAddWorkflowDefinitionWithOutputSignals() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID7);
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertEquals(wfd, definition);
    assertNotNull(wfd.getInternalId());
    var step1Signals =
        definition.getWorkflow().getSteps().getFirst().getSignalOutputs().definitions();
    assertEquals(2, step1Signals.size());
    assertEquals("dummy/test/signal1", step1Signals.getFirst().getName());
    assertEquals(
        1, step1Signals.getFirst().getParams().get("p1").asLongParamDef().getValue().longValue());
    assertEquals("aaa", step1Signals.get(1).getName());
    assertEquals("auu+1", step1Signals.get(1).getParams().get("p2").getExpression());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
  }

  @Test
  public void testAddInvalidWorkflowDefinition() throws Exception {
    WorkflowDefinition wfd = loadWorkflow("sample-active-wf-without-props");
    AssertHelper.assertThrows(
        "initial version must contains properties info, e.g. owner",
        IllegalArgumentException.class,
        "workflow properties and also owner must be set when creating the first version of a workflow",
        () -> workflowDao.addWorkflowDefinition(wfd, null));
  }

  @Test
  public void testAddWorkflowDefinitionWithTriggerUpdates() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID6);
    List<TimeTrigger> timeTriggers = wfd.getWorkflow().getTimeTriggers();
    List<SignalTrigger> signalTriggers = wfd.getWorkflow().getSignalTriggers();

    // push an initial active version with triggers, and then call upsert
    testWorkflowUpdate(wfd, true, null, null, false, true, 1);

    // push an active version without triggers, and then no upsert call
    wfd.setWorkflow(wfd.getWorkflow().toBuilder().timeTriggers(null).signalTriggers(null).build());
    testWorkflowUpdate(wfd, true, null, null, true, false, 0);

    // push an active version with any triggers, and then call trigger upsert
    wfd.setWorkflow(
        wfd.getWorkflow().toBuilder().timeTriggers(timeTriggers).signalTriggers(null).build());
    testWorkflowUpdate(wfd, true, null, null, false, true, 1);
    testWorkflowUpdate(wfd, true, null, true, false, false, 1);
    testWorkflowUpdate(wfd, true, false, true, false, false, 1);
    // push an active version with disabled triggers, and then no upsert call
    testWorkflowUpdate(wfd, true, true, false, true, false, 0);

    // push an active version with any triggers, and then call trigger upsert
    wfd.setWorkflow(
        wfd.getWorkflow().toBuilder().timeTriggers(null).signalTriggers(signalTriggers).build());
    testWorkflowUpdate(wfd, true, false, false, false, true, 1);
    testWorkflowUpdate(wfd, true, true, false, false, false, 1);
    // push an active version with disabled triggers, and then no upsert call
    testWorkflowUpdate(wfd, true, false, true, true, false, 0);

    // push an active version with any triggers, and then call trigger upsert
    wfd.setWorkflow(
        wfd.getWorkflow().toBuilder()
            .timeTriggers(timeTriggers)
            .signalTriggers(signalTriggers)
            .build());
    testWorkflowUpdate(wfd, true, false, false, false, true, 1);
    testWorkflowUpdate(wfd, true, true, false, false, false, 1);
    testWorkflowUpdate(wfd, true, false, true, false, false, 1);
    // push an active version with disabled triggers, and then no upsert call
    testWorkflowUpdate(wfd, true, true, true, true, false, 0);

    // push an inactive version with triggers, and then call upsert if an active one exists
    testWorkflowUpdate(wfd, false, false, false, false, true, 1);

    // push an active version with triggers disabled, and then no upsert call
    testWorkflowUpdate(wfd, true, true, true, true, false, 0);
  }

  @Test
  public void testAddWorkflowDefinitionWithoutTriggerUpdates() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID6);
    // push an initial inactive version with triggers, and then no upsert call
    testWorkflowUpdate(wfd, false, null, null, true, true, 0);

    // push another inactive one with triggers, and then no upsert call
    testWorkflowUpdate(wfd, false, null, null, true, true, 0);
  }

  private void testWorkflowUpdate(
      WorkflowDefinition wfd,
      boolean active,
      Boolean timeTriggerDisabled,
      Boolean signalTriggerDisabled,
      boolean isCurrentNull,
      boolean isPreviousNull,
      int triggerCalled)
      throws SQLException {
    wfd.getMetadata().setCreateTime(wfd.getMetadata().getCreateTime() + 1);
    wfd.setIsActive(active);
    wfd.setPropertiesSnapshot(
        wfd.getPropertiesSnapshot().toBuilder()
            .timeTriggerDisabled(timeTriggerDisabled)
            .signalTriggerDisabled(signalTriggerDisabled)
            .build());
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);
    assertEquals(active, definition.getIsActive());

    verifyTriggerUpdate(isCurrentNull, isPreviousNull, triggerCalled);
  }

  private void verifyTriggerUpdate(boolean isCurrentNull, boolean isPreviousNull, int triggerCalled)
      throws SQLException {
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    verify(triggerClient, times(triggerCalled))
        .upsertTriggerSubscription(any(), any(), any(), any());
    verify(triggerClient, times(triggerCalled))
        .upsertTriggerSubscription(
            any(),
            any(),
            isCurrentNull ? eq(null) : any(TriggerUuids.class),
            isPreviousNull ? eq(null) : any(TriggerUuids.class));
    reset(triggerClient);
    reset(queueSystem);
  }

  @Test
  public void testUpdateWorkflowProperties() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID2);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    MaestroWorkflow maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID2);
    assertEquals("tester", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    assertEquals(1L, maestroWorkflow.getLatestVersionId().longValue());
    PropertiesSnapshot newSnapshot =
        workflowDao.updateWorkflowProperties(
            TEST_WORKFLOW_ID2, User.create("test"), new Properties(), PROPERTIES_UPDATE);
    assertEquals(
        wfd.getPropertiesSnapshot().toBuilder()
            .createTime(newSnapshot.getCreateTime())
            .author(User.create("test"))
            .build(),
        newSnapshot);
    verify(queueSystem, times(2)).enqueue(any(), any());
    verify(queueSystem, times(2)).notify(any());
    maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID2);
    assertEquals("tester", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    Properties props = new Properties();
    props.setOwner(User.create("another-owner"));
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID2, User.create("test"), props, PROPERTIES_UPDATE);
    verify(queueSystem, times(3)).enqueue(any(), any());
    verify(queueSystem, times(3)).notify(any());
    maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID2);
    assertEquals("another-owner", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    assertEquals(wfd, workflowDao.addWorkflowDefinition(wfd, null));
    verify(triggerClient, times(0)).upsertTriggerSubscription(any(), any(), any(), any());
  }

  @Test
  public void testAddTagToWorkflowProperties() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID2);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    MaestroWorkflow maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID2);
    assertEquals("tester", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    assertEquals(1L, maestroWorkflow.getLatestVersionId().longValue());
    Tag tagToBeAdded = Tag.create("test-add-tag");
    Properties props = new Properties();
    props.setTags(new TagList(Collections.singletonList(tagToBeAdded)));
    PropertiesSnapshot newSnapshot =
        workflowDao.updateWorkflowProperties(
            TEST_WORKFLOW_ID2, User.create("test"), props, PROPERTIES_UPDATE_ADD_TAG);
    assertEquals(
        wfd.getPropertiesSnapshot().toBuilder()
            .createTime(newSnapshot.getCreateTime())
            .author(User.create("test"))
            .tags(new TagList(Collections.singletonList(tagToBeAdded)))
            .build(),
        newSnapshot);
    verify(queueSystem, times(2)).enqueue(any(), any());
    verify(queueSystem, times(2)).notify(any());
    verify(triggerClient, times(0)).upsertTriggerSubscription(any(), any(), any(), any());
  }

  @Test
  public void testUpdateTagInWorkflowProperties() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID2);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    MaestroWorkflow maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID2);
    assertEquals("tester", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    assertEquals(1L, maestroWorkflow.getLatestVersionId().longValue());

    Tag tagToBeAdded1 = Tag.create("test-update-tag-1");
    tagToBeAdded1.setNamespace(Tag.Namespace.NOTEBOOK_TEMPLATE);
    tagToBeAdded1.addAttribute("description", "description 1");

    // add two tags
    Tag tagToBeAdded2 = Tag.create("test-update-tag-2");
    tagToBeAdded2.setNamespace(Tag.Namespace.NOTEBOOK_TEMPLATE);
    tagToBeAdded2.setPermit(1);
    tagToBeAdded2.addAttribute("description", "description 2");

    Properties propsForFirstAdd = new Properties();
    propsForFirstAdd.setTags(new TagList(Collections.singletonList(tagToBeAdded1)));
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID2, User.create("test"), propsForFirstAdd, PROPERTIES_UPDATE_ADD_TAG);

    Properties propsForSecondAdd = new Properties();
    propsForSecondAdd.setTags(new TagList(Collections.singletonList(tagToBeAdded2)));
    PropertiesSnapshot newSnapshot =
        workflowDao.updateWorkflowProperties(
            TEST_WORKFLOW_ID2, User.create("test"), propsForSecondAdd, PROPERTIES_UPDATE_ADD_TAG);

    assertEquals(
        wfd.getPropertiesSnapshot().toBuilder()
            .createTime(newSnapshot.getCreateTime())
            .author(User.create("test"))
            .tags(
                new TagList(
                    Stream.of(tagToBeAdded1, tagToBeAdded2)
                        .collect(Collectors.toCollection(ArrayList::new))))
            .build(),
        newSnapshot);

    // update the second tag
    Tag newVersionOfTag = Tag.create("test-update-tag-2");
    newVersionOfTag.setNamespace(Tag.Namespace.PLATFORM);
    newVersionOfTag.setPermit(2);
    newVersionOfTag.addAttribute("description", "description updated");

    Properties props = new Properties();
    props.setTags(new TagList(Collections.singletonList(newVersionOfTag)));
    PropertiesSnapshot newSnapshotAfterUpdate =
        workflowDao.updateWorkflowProperties(
            TEST_WORKFLOW_ID2, User.create("test"), props, PROPERTIES_UPDATE_ADD_TAG);
    assertEquals(
        wfd.getPropertiesSnapshot().toBuilder()
            .createTime(newSnapshotAfterUpdate.getCreateTime())
            .author(User.create("test"))
            .tags(
                new TagList(
                    Stream.of(tagToBeAdded1, newVersionOfTag)
                        .collect(Collectors.toCollection(ArrayList::new))))
            .build(),
        newSnapshotAfterUpdate);
  }

  @Test
  public void testDeleteTagToWorkflowProperties() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID9);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    MaestroWorkflow maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID9);
    assertEquals("tester", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    assertEquals(1L, maestroWorkflow.getLatestVersionId().longValue());
    assertEquals(1, maestroWorkflow.getPropertiesSnapshot().getTags().getTags().size());
    assertEquals(
        "some-workflow-tag",
        maestroWorkflow.getPropertiesSnapshot().getTags().getTags().getFirst().getName());
    Tag tagToBeDeleted = Tag.create("some-workflow-tag");
    Properties props = new Properties();
    props.setTags(new TagList(Collections.singletonList(tagToBeDeleted)));
    PropertiesSnapshot newSnapshot =
        workflowDao.updateWorkflowProperties(
            TEST_WORKFLOW_ID9, User.create("test"), props, PROPERTIES_UPDATE_DELETE_TAG);
    assertEquals(
        wfd.getPropertiesSnapshot().toBuilder()
            .createTime(newSnapshot.getCreateTime())
            .author(User.create("test"))
            .tags(new TagList(null))
            .build(),
        newSnapshot);
    verify(queueSystem, times(2)).enqueue(any(), any());
    verify(queueSystem, times(2)).notify(any());
    verify(triggerClient, times(0)).upsertTriggerSubscription(any(), any(), any(), any());
  }

  @Test
  public void testInvalidWorkflowProperties() {
    AssertHelper.assertThrows(
        "cannot push a empty properties change for any workflow",
        NullPointerException.class,
        "properties changes to apply cannot be null for workflow",
        () ->
            workflowDao.updateWorkflowProperties(
                TEST_WORKFLOW_ID2, User.create("test"), null, PROPERTIES_UPDATE));

    AssertHelper.assertThrows(
        "cannot push a properties change for non-existing workflow",
        NullPointerException.class,
        "Cannot update workflow properties while the workflow",
        () ->
            workflowDao.updateWorkflowProperties(
                TEST_WORKFLOW_ID2, User.create("test"), new Properties(), PROPERTIES_UPDATE));
  }

  @Test
  public void testUpdateWorkflowPropertiesWithTriggerUpdates() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID6);
    // push an initial inactive version with triggers, and then no upsert call
    testWorkflowUpdate(wfd, false, null, null, true, true, 0);

    Properties props = new Properties();
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties without an active version, and then no upsert call
    verifyTriggerUpdate(true, true, 0);

    // push an active version with triggers, and then call upsert
    testWorkflowUpdate(wfd, true, null, null, false, true, 1);
    MaestroWorkflow maestroWorkflow = workflowDao.getMaestroWorkflow(TEST_WORKFLOW_ID6);
    assertEquals("tester", maestroWorkflow.getPropertiesSnapshot().getOwner().getName());
    assertEquals(2L, maestroWorkflow.getLatestVersionId().longValue());

    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties with existing triggers, and then call upsert
    verifyTriggerUpdate(false, false, 1);

    props.setTimeTriggerDisabled(true);
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties with any trigger enabled, and then call upsert
    verifyTriggerUpdate(false, false, 1);

    props.setSignalTriggerDisabled(true);
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties with all triggers disabled, and then no upsert call
    verifyTriggerUpdate(true, false, 0);

    props = new Properties();
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties still with all triggers disabled, and then no upsert
    verifyTriggerUpdate(true, true, 0);

    props.setTimeTriggerDisabled(false);
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties with any trigger enabled, and then call upsert
    verifyTriggerUpdate(false, true, 1);

    props.setSignalTriggerDisabled(false);
    workflowDao.updateWorkflowProperties(
        TEST_WORKFLOW_ID6, User.create("test"), props, PROPERTIES_UPDATE);
    // update properties still with all triggers disabled, and then call upsert
    verifyTriggerUpdate(false, false, 1);
  }

  @Test
  public void testWorkflowWithSignalTriggerSubscriptions() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID3);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    Mockito.verify(triggerClient, Mockito.times(1))
        .upsertTriggerSubscription(
            any(), workflowCaptor.capture(), Mockito.eq(wfd.getTriggerUuids()), Mockito.eq(null));
    Workflow capturedWorkflow = workflowCaptor.getValue();
    Assert.assertNotNull(capturedWorkflow);
    Assert.assertEquals("sample-active-wf-with-signal-triggers", capturedWorkflow.getId());
    Assert.assertEquals(
        wfd.getWorkflow().getSignalTriggers(), capturedWorkflow.getSignalTriggers());
  }

  @Test
  public void testWorkflowWithStepSignalDependencies() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID5);
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);
  }

  @Test
  public void testWorkflowWithSignalSubscriptionsFailsOnSignalService() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID3);
    Mockito.doThrow(
            new MaestroRuntimeException(
                MaestroRuntimeException.Code.INTERNAL_ERROR, "test error message"))
        .when(triggerClient)
        .upsertTriggerSubscription(
            any(),
            Mockito.eq(wfd.getWorkflow()),
            Mockito.eq(wfd.getTriggerUuids()),
            Mockito.eq(null));
    AssertHelper.assertThrows(
        "expects mockito test error",
        MaestroRuntimeException.class,
        "test error message",
        () ->
            workflowDao.addWorkflowDefinition(
                wfd, wfd.getPropertiesSnapshot().extractProperties()));
  }

  @Test
  public void testGetWorkflowDefinition() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    assertEquals(TEST_WORKFLOW_ID1, wfd.getWorkflow().getId());
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    WorkflowDefinition definition =
        workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "latest");
    assertNotNull(definition.getInternalId());
    assertEquals(wfd, definition);
    assertEquals(TEST_WORKFLOW_ID1, definition.getMetadata().getWorkflowId());
    assertEquals(1L, definition.getMetadata().getWorkflowVersionId().longValue());

    definition = workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "default");
    assertNotNull(definition.getInternalId());
    assertEquals(wfd, definition);
    assertEquals(TEST_WORKFLOW_ID1, definition.getMetadata().getWorkflowId());
    assertEquals(1L, definition.getMetadata().getWorkflowVersionId().longValue());

    definition = workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "active");
    assertNotNull(definition.getInternalId());
    assertEquals(wfd, definition);
    assertEquals(TEST_WORKFLOW_ID1, definition.getMetadata().getWorkflowId());
    assertEquals(1L, definition.getMetadata().getWorkflowVersionId().longValue());

    definition = workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "1");
    assertNotNull(definition.getInternalId());
    assertEquals(wfd, definition);
    assertEquals(TEST_WORKFLOW_ID1, definition.getMetadata().getWorkflowId());
    assertEquals(1L, definition.getMetadata().getWorkflowVersionId().longValue());
  }

  @Test
  public void testGetWorkflowDefinitionWithErrors() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    wfd.setIsActive(false);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());

    AssertHelper.assertThrows(
        "error to get invalid workflow definition",
        InvalidWorkflowVersionException.class,
        "Invalid workflow version [0]",
        () -> workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "0"));

    AssertHelper.assertThrows(
        "error to get invalid workflow definition",
        MaestroNotFoundException.class,
        "Cannot find an active version for workflow",
        () -> workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "active"));

    AssertHelper.assertThrows(
        "error to get invalid workflow definition",
        InvalidWorkflowVersionException.class,
        "Invalid workflow version [-1]",
        () -> workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "-1"));

    AssertHelper.assertThrows(
        "error to get invalid workflow definition",
        MaestroNotFoundException.class,
        "Cannot find workflow [sample-active-wf-with-props] with version [10]",
        () -> workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "10"));

    AssertHelper.assertThrows(
        "error to get invalid workflow definition",
        MaestroNotFoundException.class,
        "Workflow [not-existing] has not been created",
        () -> workflowDao.getWorkflowDefinition("not-existing", "1"));
  }

  @Test
  public void testGetWorkflowDefinitionWithTimeTrigger() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID4);
    assertEquals(TEST_WORKFLOW_ID4, wfd.getWorkflow().getId());
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    WorkflowDefinition definition =
        workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "latest");

    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    Mockito.verify(triggerClient, Mockito.times(1))
        .upsertTriggerSubscription(
            any(), workflowCaptor.capture(), Mockito.eq(wfd.getTriggerUuids()), Mockito.eq(null));
    Workflow capturedWorkflow = workflowCaptor.getValue();
    Assert.assertNotNull(capturedWorkflow);
    Assert.assertEquals("sample-active-wf-with-time-triggers", capturedWorkflow.getId());
    Assert.assertEquals(wfd.getWorkflow().getTimeTriggers(), capturedWorkflow.getTimeTriggers());
    assertNotNull(definition.getInternalId());
    assertEquals(wfd, definition);
    assertEquals(6, wfd.getWorkflow().getTimeTriggers().size());
  }

  @Test
  public void testTimeTriggerUUIDWithFuzzyCron() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID4);
    List<TimeTrigger> triggers = wfd.getWorkflow().getTimeTriggers();
    assertEquals(6, triggers.size());

    TriggerUuids uuids1 = IdHelper.toTriggerUuids(wfd.getWorkflow());
    ((CronTimeTrigger) triggers.get(2)).setFuzzyMaxDelay(null);

    TriggerUuids uuids2 = IdHelper.toTriggerUuids(wfd.getWorkflow());

    assertNotSame(uuids1.getTimeTriggerUuid(), uuids2.getTimeTriggerUuid());
  }

  @Test
  public void testWorkflowWithWorkflowTimeSubscriptionsFailsOnCronService() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID4);
    Mockito.doThrow(
            new MaestroRuntimeException(
                MaestroRuntimeException.Code.INTERNAL_ERROR, "test error message"))
        .when(triggerClient)
        .upsertTriggerSubscription(any(), Mockito.eq(wfd.getWorkflow()), any(), any());
    AssertHelper.assertThrows(
        "expects mockito test error",
        MaestroRuntimeException.class,
        "test error message",
        () ->
            workflowDao.addWorkflowDefinition(
                wfd, wfd.getPropertiesSnapshot().extractProperties()));
  }

  @Test
  public void testGetWorkflowDefinitionWithTriggers() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID6);
    assertEquals(TEST_WORKFLOW_ID6, wfd.getWorkflow().getId());
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);

    Mockito.verify(triggerClient, Mockito.times(1))
        .upsertTriggerSubscription(
            any(), workflowCaptor.capture(), Mockito.eq(wfd.getTriggerUuids()), Mockito.eq(null));
    Mockito.verify(triggerClient, Mockito.times(1))
        .upsertTriggerSubscription(
            any(), workflowCaptor.capture(), Mockito.eq(wfd.getTriggerUuids()), Mockito.eq(null));
    Workflow capturedWorkflow = workflowCaptor.getValue();
    Assert.assertNotNull(capturedWorkflow);
    Assert.assertEquals("sample-active-wf-with-triggers", capturedWorkflow.getId());
    Assert.assertEquals(wfd.getWorkflow().getTimeTriggers(), capturedWorkflow.getTimeTriggers());
    Assert.assertEquals(
        wfd.getWorkflow().getSignalTriggers(), capturedWorkflow.getSignalTriggers());
    assertEquals(1, wfd.getWorkflow().getSignalTriggers().size());
    assertEquals(4, wfd.getWorkflow().getTimeTriggers().size());

    WorkflowDefinition definition =
        workflowDao.getWorkflowDefinition(wfd.getWorkflow().getId(), "latest");
    assertNotNull(definition.getInternalId());
    assertEquals(wfd, definition);

    wfd.getMetadata().setCreateTime(wfd.getMetadata().getCreateTime() + 1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    Mockito.verify(triggerClient, Mockito.times(1))
        .upsertTriggerSubscription(
            any(),
            workflowCaptor.capture(),
            Mockito.eq(wfd.getTriggerUuids()),
            Mockito.eq(wfd.getTriggerUuids()));
    capturedWorkflow = workflowCaptor.getValue();
    Assert.assertNotNull(capturedWorkflow);
    Assert.assertEquals("sample-active-wf-with-triggers", capturedWorkflow.getId());
    Assert.assertEquals(wfd.getWorkflow().getTimeTriggers(), capturedWorkflow.getTimeTriggers());
    Assert.assertEquals(
        wfd.getWorkflow().getSignalTriggers(), capturedWorkflow.getSignalTriggers());
    assertEquals(1, wfd.getWorkflow().getSignalTriggers().size());
    assertEquals(4, wfd.getWorkflow().getTimeTriggers().size());
  }

  @Test
  public void testGetMaestroWorkflow() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    assertEquals(TEST_WORKFLOW_ID1, wfd.getWorkflow().getId());
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);

    MaestroWorkflow maestroWorkflow = workflowDao.getMaestroWorkflow(wfd.getWorkflow().getId());
    assertEquals(1L, maestroWorkflow.getActiveVersionId().longValue());
    assertNotNull(maestroWorkflow.getActivateTime());
    assertNotNull(maestroWorkflow.getActivatedBy());
    assertNotNull(maestroWorkflow.getModifyTime());
    assertNull("metadata should be unset", maestroWorkflow.getMetadata());
    assertNull("definition should be unset", maestroWorkflow.getDefinition());
    assertEquals(
        wfd.getPropertiesSnapshot().getOwner(), maestroWorkflow.getPropertiesSnapshot().getOwner());
    assertEquals(
        wfd.getPropertiesSnapshot().getAccessControl(),
        maestroWorkflow.getPropertiesSnapshot().getAccessControl());
    assertEquals(
        wfd.getPropertiesSnapshot().getRunStrategy(),
        maestroWorkflow.getPropertiesSnapshot().getRunStrategy());
    assertEquals(
        wfd.getPropertiesSnapshot().getStepConcurrency(),
        maestroWorkflow.getPropertiesSnapshot().getStepConcurrency());
    assertEquals(
        wfd.getPropertiesSnapshot().getAlerting(),
        maestroWorkflow.getPropertiesSnapshot().getAlerting());
    assertEquals(1L, maestroWorkflow.getLatestVersionId().longValue());
  }

  @Test
  public void getInvalidWorkflowVersion() throws Exception {
    AssertHelper.assertThrows(
        "cannot get non-existing workflow",
        MaestroNotFoundException.class,
        "has not been created yet or has been deleted",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID2, "latest"));

    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID2);
    wfd.setIsActive(false);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    WorkflowDefinition def = workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID2, "latest");
    assertNotNull(def.getInternalId());
    assertEquals(
        wfd.getMetadata().getWorkflowVersionId(), def.getMetadata().getWorkflowVersionId());
    AssertHelper.assertThrows(
        "there is no active version",
        MaestroNotFoundException.class,
        "Cannot find an active version for workflow",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID2, "active"));
    AssertHelper.assertThrows(
        "cannot get a non-positive workflow version",
        InvalidWorkflowVersionException.class,
        "Invalid workflow version",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID2, "-1"));
    AssertHelper.assertThrows(
        "cannot get a non-integer workflow version",
        InvalidWorkflowVersionException.class,
        "Invalid workflow version",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID2, "abc"));
    AssertHelper.assertThrows(
        "cannot get an invalid workflow version",
        MaestroNotFoundException.class,
        "Cannot find workflow",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID2, "100"));
  }

  @Test
  public void testDeleteWorkflow() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);

    WorkflowDefinition def = workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID1, "latest");
    assertNotNull(wfd.getInternalId());
    assertNotNull(def.getInternalId());
    assertEquals(wfd.getInternalId(), def.getInternalId());
    assertEquals(wfd.getWorkflow(), def.getWorkflow());
    workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester"));
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);
    AssertHelper.assertThrows(
        "The workflow should have been deleted",
        MaestroNotFoundException.class,
        "has not been created yet or has been deleted",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID1, "latest"));
  }

  @Test
  public void testWorkflowInvalidDeletion() throws Exception {
    AssertHelper.assertThrows(
        "The inline workflow cannot be deleted.",
        MaestroUnprocessableEntityException.class,
        "Cannot delete an inline foreach workflow [maestro_foreach",
        () -> workflowDao.deleteWorkflow(TEST_INLINE_WORKFLOW_ID1, User.create("tester")));

    AssertHelper.assertThrows(
        "The workflow does not exist.",
        MaestroNotFoundException.class,
        "No workflow is deleted because workflow [sample-active-wf-with-props] is non-existing",
        () -> workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester")));

    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    verify(queueSystem, times(1)).enqueue(any(), any(WorkflowVersionUpdateJobEvent.class));
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);

    when(queueSystem.enqueue(any(), any())).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "The workflow cannot be deleted due to publishing event failure.",
        RuntimeException.class,
        "test",
        () -> workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester")));
    verify(queueSystem, times(0)).notify(any());
    reset(queueSystem);

    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId(TEST_WORKFLOW_ID1);
    instanceDao.runWorkflowInstances(TEST_WORKFLOW_ID1, Collections.singletonList(instance));
    AssertHelper.assertThrows(
        "The workflow cannot be deleted due to running instances.",
        IllegalArgumentException.class,
        "Cannot delete the workflow [sample-active-wf-with-props] because there are still [1] number",
        () -> workflowDao.deleteWorkflow(TEST_WORKFLOW_ID1, User.create("tester")));
  }

  @Test
  public void testDeactivateWorkflow() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID6);
    testWorkflowUpdate(wfd, true, null, null, false, true, 1);
    String timeline = workflowDao.deactivate(TEST_WORKFLOW_ID6, User.create("test"));
    assertEquals(
        "Caller [test] deactivated workflow [sample-active-wf-with-triggers], whose last active version is [1]",
        timeline);
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);

    WorkflowDefinition def = workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID6, "latest");
    assertEquals(false, def.getIsActive());
    AssertHelper.assertThrows(
        "There should be no active version",
        MaestroNotFoundException.class,
        "Cannot find an active version for workflow [sample-active-wf-with-triggers]",
        () -> workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID6, "active"));

    timeline = workflowDao.deactivate(TEST_WORKFLOW_ID6, User.create("test"));
    assertEquals(
        "Caller [test] do nothing as there is no active workflow version for [sample-active-wf-with-triggers]",
        timeline);
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);

    WorkflowVersionUpdateJobEvent timelineEvent =
        (WorkflowVersionUpdateJobEvent)
            workflowDao.activate(TEST_WORKFLOW_ID6, "1", User.create("test"));
    assertEquals(
        "Caller [test] activates workflow version [sample-active-wf-with-triggers][1], previous active version is [0]",
        timelineEvent.getLog());
    verifyTriggerUpdate(false, true, 1);

    AssertHelper.assertThrows(
        "Cannot deactivate non-existing workflow",
        MaestroNotFoundException.class,
        "Cannot deactivate workflow [non-existing] as it is not found",
        () -> workflowDao.deactivate("non-existing", User.create("test")));
  }

  @Test
  public void testActivateWorkflowVersion() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID6);
    testWorkflowUpdate(wfd, false, null, null, true, true, 0);

    WorkflowVersionUpdateJobEvent timelineEvent =
        (WorkflowVersionUpdateJobEvent)
            workflowDao.activate(TEST_WORKFLOW_ID6, "1", User.create("test"));
    assertEquals(
        "Caller [test] activates workflow version [sample-active-wf-with-triggers][1], previous active version is [0]",
        timelineEvent.getLog());
    verifyTriggerUpdate(false, true, 1);

    WorkflowDefinition def = workflowDao.getWorkflowDefinition(TEST_WORKFLOW_ID6, "latest");
    assertEquals(true, def.getIsActive());

    AssertHelper.assertThrows(
        "Cannot activate non-existing workflow version",
        MaestroNotFoundException.class,
        "Cannot find workflow [sample-active-wf-with-triggers] with version [2]",
        () -> workflowDao.activate(TEST_WORKFLOW_ID6, "2", User.create("test")));

    timelineEvent =
        (WorkflowVersionUpdateJobEvent)
            workflowDao.activate(TEST_WORKFLOW_ID6, "1", User.create("test"));
    assertEquals(
        "Caller [test] do nothing as workflow version [sample-active-wf-with-triggers][1] is already active",
        timelineEvent.getLog());
    verify(queueSystem, times(0)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);
    verify(triggerClient, times(0)).upsertTriggerSubscription(any(), any(), any(), any());

    // add a new active version
    testWorkflowUpdate(wfd, true, null, null, false, false, 1);

    timelineEvent =
        (WorkflowVersionUpdateJobEvent)
            workflowDao.activate(TEST_WORKFLOW_ID6, "1", User.create("test"));
    assertEquals(
        "Caller [test] activates workflow version [sample-active-wf-with-triggers][1], previous active version is [2]",
        timelineEvent.getLog());
    verifyTriggerUpdate(false, false, 1);

    AssertHelper.assertThrows(
        "Cannot activate non-existing workflow",
        MaestroNotFoundException.class,
        "Cannot find workflow [non-existing]",
        () -> workflowDao.activate("non-existing", "1", User.create("test")));
  }

  @Test
  public void testGetWorkflowOverview() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    long internalId = wfd.getInternalId();
    wfd.setIsActive(false);
    workflowDao.addWorkflowDefinition(wfd, null);
    assertEquals(internalId, (long) wfd.getInternalId());

    WorkflowOverviewResponse response = workflowDao.getWorkflowOverview(TEST_WORKFLOW_ID1);
    assertEquals(TEST_WORKFLOW_ID1, response.getWorkflowId());
    assertEquals(1L, response.getActiveVersionId().longValue());
    assertEquals(2L, response.getLatestVersionId().longValue());
    assertEquals(1L, response.getDefaultVersionId().longValue());
    assertEquals(RunStrategy.create(20), response.getPropertiesSnapshot().getRunStrategy());
    assertEquals(20L, response.getStepConcurrency().longValue());
    Map<WorkflowInstance.Status, Long> statusStats = new EnumMap<>(WorkflowInstance.Status.class);
    statusStats.put(WorkflowInstance.Status.CREATED, 0L);
    statusStats.put(WorkflowInstance.Status.IN_PROGRESS, 0L);
    statusStats.put(WorkflowInstance.Status.PAUSED, 0L);
    assertEquals(statusStats, response.getNonterminalInstances());
    assertEquals(0L, response.getFailedInstances().longValue());
    assertNull(response.getLatestInstanceId());

    workflowDao.deactivate(TEST_WORKFLOW_ID1, User.create("test"));
    WorkflowOverviewResponse response1 = workflowDao.getWorkflowOverview(TEST_WORKFLOW_ID1);
    assertNull(response1.getActiveVersionId());
    assertEquals(2L, response1.getLatestVersionId().longValue());
    assertEquals(2L, response1.getDefaultVersionId().longValue());

    AssertHelper.assertThrows(
        "Cannot get non-existing workflow overview",
        MaestroNotFoundException.class,
        "Cannot find workflow [non-existing], which is either not created or has been deleted.",
        () -> workflowDao.getWorkflowOverview("non-existing"));
  }

  @Test
  public void testGetWorkflowPropertiesSnapshot() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    PropertiesSnapshot snapshot =
        workflowDao.getWorkflowPropertiesSnapshot(TEST_WORKFLOW_ID1, "latest");
    assertEquals(TEST_WORKFLOW_ID1, snapshot.getWorkflowId());
    assertEquals(RunStrategy.Rule.PARALLEL, snapshot.getRunStrategy().getRule());
    assertEquals(20, snapshot.getRunStrategy().getWorkflowConcurrency());
    assertEquals(20, snapshot.getStepConcurrency().longValue());
    assertEquals(1, snapshot.getAlerting().getTct().getCompletedByHour().intValue());
  }

  @Test
  public void testGetWorkflowPropertiesSnapshotForInlineWorkflow() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId(TEST_INLINE_WORKFLOW_ID1);
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId(TEST_WORKFLOW_ID1);
    initiator.setAncestors(Collections.singletonList(parent));
    instance.setInitiator(initiator);

    instanceDao.runWorkflowInstances(TEST_INLINE_WORKFLOW_ID1, Collections.singletonList(instance));

    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());

    PropertiesSnapshot snapshot =
        workflowDao.getWorkflowPropertiesSnapshot(TEST_INLINE_WORKFLOW_ID1, "latest");
    assertEquals(TEST_WORKFLOW_ID1, snapshot.getWorkflowId());
    assertEquals(RunStrategy.Rule.PARALLEL, snapshot.getRunStrategy().getRule());
    assertEquals(20, snapshot.getRunStrategy().getWorkflowConcurrency());
    assertEquals(20, snapshot.getStepConcurrency().longValue());
    assertEquals(1, snapshot.getAlerting().getTct().getCompletedByHour().intValue());

    assertEquals(wfd.getPropertiesSnapshot(), snapshot);

    AssertHelper.assertThrows(
        "Non-existing inline workflow id",
        MaestroNotFoundException.class,
        "Cannot find inline workflow [maestro_foreachsample-active-wf-with-props123]",
        () ->
            workflowDao.getWorkflowPropertiesSnapshot(TEST_INLINE_WORKFLOW_ID1 + "123", "latest"));
  }

  @Test
  public void testInvalidGetWorkflowPropertiesSnapshot() {
    AssertHelper.assertThrows(
        "Cannot get non-existing workflow's properties-snapshot",
        MaestroNotFoundException.class,
        "Cannot find workflow [" + TEST_WORKFLOW_ID1 + "]'s current properties-snapshot.",
        () -> workflowDao.getWorkflowPropertiesSnapshot(TEST_WORKFLOW_ID1, "latest"));

    AssertHelper.assertThrows(
        "Cannot get a specific version of snapshot",
        UnsupportedOperationException.class,
        "Specific snapshot version is not implemented.",
        () -> workflowDao.getWorkflowPropertiesSnapshot(TEST_WORKFLOW_ID1, "12345"));
  }

  @Test
  public void testGetRunStrategy() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    RunStrategy runStrategy = workflowDao.getRunStrategy(TEST_WORKFLOW_ID1);
    assertEquals(RunStrategy.Rule.PARALLEL, runStrategy.getRule());
    assertEquals(20L, runStrategy.getWorkflowConcurrency());

    wfd = loadWorkflow(TEST_WORKFLOW_ID2);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(Defaults.DEFAULT_RUN_STRATEGY, workflowDao.getRunStrategy(TEST_WORKFLOW_ID2));
  }

  @Test
  public void testRunStrategySwitching() throws Exception {
    User tester = User.create("test");
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    Properties properties = wfd.getPropertiesSnapshot().extractProperties();
    properties.setRunStrategy(RunStrategy.create("SEQUENTIAL"));
    WorkflowDefinition definition = workflowDao.addWorkflowDefinition(wfd, properties);
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);

    for (var rs :
        Arrays.asList(
            RunStrategy.create(10),
            RunStrategy.create("first_only"),
            RunStrategy.create("last_only"),
            RunStrategy.create("strict_sequential"),
            RunStrategy.create("sequential"),
            RunStrategy.create("first_only"),
            RunStrategy.create("strict_sequential"),
            RunStrategy.create("last_only"),
            RunStrategy.create(10),
            RunStrategy.create("sequential"),
            RunStrategy.create("strict_sequential"),
            RunStrategy.create("last_only"),
            RunStrategy.create("first_only"),
            RunStrategy.create(10),
            RunStrategy.create("strict_sequential"),
            RunStrategy.create("first_only"),
            RunStrategy.create("sequential"),
            RunStrategy.create("last_only"),
            RunStrategy.create("sequential"))) {
      properties.setRunStrategy(rs);
      assertNotNull(
          workflowDao.updateWorkflowProperties(
              TEST_WORKFLOW_ID1, tester, properties, PROPERTIES_UPDATE));
      verify(queueSystem, times(1)).enqueue(any(), any());
      verify(queueSystem, times(1)).notify(any());
      reset(queueSystem);
    }

    MaestroRunStrategyDao runStrategyDao =
        new MaestroRunStrategyDao(dataSource, MAPPER, config, queueSystem, metricRepo);
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId(TEST_WORKFLOW_ID1);
    instance.setWorkflowInstanceId(0L);
    instance.setWorkflowRunId(0L);
    instance.setWorkflowUuid("uuid1");
    runStrategyDao.startWithRunStrategy(instance, properties.getRunStrategy());
    instance.setWorkflowId(TEST_WORKFLOW_ID1);
    instance.setWorkflowInstanceId(0L);
    instance.setWorkflowRunId(0L);
    instance.setWorkflowUuid("uuid2");
    runStrategyDao.startWithRunStrategy(instance, properties.getRunStrategy());
    reset(queueSystem);

    Arrays.asList(RunStrategy.create("first_only"), RunStrategy.create("last_only"))
        .forEach(
            rs -> {
              properties.setRunStrategy(rs);
              AssertHelper.assertThrows(
                  "Cannot update run strategy",
                  MaestroPreconditionFailedException.class,
                  "because there are 2 nonterminal workflow instances",
                  () ->
                      workflowDao.updateWorkflowProperties(
                          TEST_WORKFLOW_ID1, tester, properties, PROPERTIES_UPDATE));
            });

    MaestroWorkflowInstanceDao instanceDao =
        new MaestroWorkflowInstanceDao(dataSource, MAPPER, config, queueSystem, metricRepo);
    instanceDao.tryTerminateQueuedInstance(instance, WorkflowInstance.Status.FAILED, "test-reason");
    properties.setRunStrategy(RunStrategy.create("strict_sequential"));
    AssertHelper.assertThrows(
        "Cannot update run strategy",
        MaestroPreconditionFailedException.class,
        "because there are 1 FAILED workflow instances",
        () ->
            workflowDao.updateWorkflowProperties(
                TEST_WORKFLOW_ID1, tester, properties, PROPERTIES_UPDATE));
  }

  @Test
  public void testGetInlineWorkflowDefinition() throws Exception {
    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId(TEST_INLINE_WORKFLOW_ID1);
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId(TEST_WORKFLOW_ID1);
    initiator.setAncestors(Collections.singletonList(parent));
    instance.setInitiator(initiator);

    instanceDao.runWorkflowInstances(TEST_INLINE_WORKFLOW_ID1, Collections.singletonList(instance));

    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());

    WorkflowDefinition def = workflowDao.getWorkflowDefinition(TEST_INLINE_WORKFLOW_ID1, "latest");

    Metadata expected = new Metadata();
    expected.setWorkflowVersionId(1L);
    expected.setWorkflowId(TEST_INLINE_WORKFLOW_ID1);
    assertEquals(expected, def.getMetadata());
    assertEquals(wfd.getPropertiesSnapshot(), def.getPropertiesSnapshot());
    assertEquals(instance.getRuntimeWorkflow(), def.getWorkflow());

    AssertHelper.assertThrows(
        "Non-existing inline workflow id",
        MaestroNotFoundException.class,
        "Cannot find inline workflow [maestro_foreachsample-active-wf-with-props123]",
        () -> workflowDao.getWorkflowDefinition(TEST_INLINE_WORKFLOW_ID1 + "123", "latest"));
  }

  @Test
  public void testGetParamFromWorkflows() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);

    wfd = loadWorkflow(TEST_WORKFLOW_ID7);
    definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);

    Map<String, ParamDefinition> results = workflowDao.getParamFromWorkflows("sample", "foo");
    assertEquals(2, results.size());
    assertEquals("bar", results.get("sample-active-wf-with-props").getValue());
    assertEquals("bar", results.get("sample-active-wf-with-output-signals").getValue());
  }

  @Test
  public void testGetWorkflowTimeline() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    WorkflowDefinition definition =
        workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    assertNotNull(wfd.getInternalId());
    assertEquals(wfd, definition);
    WorkflowTimeline results = workflowDao.getWorkflowTimeline(TEST_WORKFLOW_ID1);
    assertEquals(TEST_WORKFLOW_ID1, results.getWorkflowId());
    assertEquals(1, results.getTimelineEvents().size());
    assertEquals(
        "Created a new workflow version [1] for workflow id [sample-active-wf-with-props]",
        results.getTimelineEvents().getFirst().getLog());
    assertEquals(1598399975650L, results.getTimelineEvents().getFirst().getTimestamp());
  }

  @Test
  public void testScanWorkflowDefinitionLimitedToWorkflow() throws Exception {
    // insert 10 versions records.
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    for (int i = 0; i < 10; i++) {
      wfd.getMetadata().setCreateTime((long) i);
      workflowDao.addWorkflowDefinition(wfd, wfd.getPropertiesSnapshot().extractProperties());
    }

    // start
    List<WorkflowDefinition> definitionList =
        workflowDao.scanWorkflowDefinition(TEST_WORKFLOW_ID1, 0, 3);
    assertEquals(3, definitionList.size());
    assertEquals(TEST_WORKFLOW_ID1, definitionList.get(0).getMetadata().getWorkflowId());
    assertEquals(10, definitionList.get(0).getMetadata().getWorkflowVersionId().longValue());
    assertEquals(TEST_WORKFLOW_ID1, definitionList.get(2).getMetadata().getWorkflowId());
    assertEquals(8, definitionList.get(2).getMetadata().getWorkflowVersionId().longValue());

    // fetch next 3
    definitionList =
        workflowDao.scanWorkflowDefinition(
            definitionList.get(2).getMetadata().getWorkflowId(),
            definitionList.get(2).getMetadata().getWorkflowVersionId(),
            3);
    assertEquals(3, definitionList.size());
    assertEquals(TEST_WORKFLOW_ID1, definitionList.get(0).getMetadata().getWorkflowId());
    assertEquals(7, definitionList.get(0).getMetadata().getWorkflowVersionId().longValue());
    assertEquals(TEST_WORKFLOW_ID1, definitionList.get(2).getMetadata().getWorkflowId());
    assertEquals(5, definitionList.get(2).getMetadata().getWorkflowVersionId().longValue());

    // fetch next 4
    definitionList =
        workflowDao.scanWorkflowDefinition(
            definitionList.get(2).getMetadata().getWorkflowId(),
            definitionList.get(2).getMetadata().getWorkflowVersionId(),
            4);
    assertEquals(4, definitionList.size());
    assertEquals(TEST_WORKFLOW_ID1, definitionList.get(0).getMetadata().getWorkflowId());
    assertEquals(4, definitionList.get(0).getMetadata().getWorkflowVersionId().longValue());
    assertEquals(TEST_WORKFLOW_ID1, definitionList.get(3).getMetadata().getWorkflowId());
    assertEquals(1, definitionList.get(3).getMetadata().getWorkflowVersionId().longValue());

    // further fetch should not return anything
    definitionList =
        workflowDao.scanWorkflowDefinition(
            definitionList.get(3).getMetadata().getWorkflowId(),
            definitionList.get(3).getMetadata().getWorkflowVersionId(),
            3);
    assertTrue(definitionList.isEmpty());
  }

  @Test
  public void testGetNewProperties() throws Exception {
    WorkflowDefinition wfd = loadWorkflow(TEST_WORKFLOW_ID1);
    Properties newProps = new Properties();
    newProps.setAlerting(null);
    PropertiesSnapshot ps = wfd.getPropertiesSnapshot();

    // properties will not get reset when it's update_properties path
    Properties resetNotAllowed = PROPERTIES_UPDATE.getNewProperties(newProps, ps);
    assertNotNull(resetNotAllowed.getAlerting());
    assertNotNull(resetNotAllowed.getRunStrategy());
    assertNotNull(resetNotAllowed.getStepConcurrency());
    assertNotNull(resetNotAllowed.getOwner());

    // alerting, run strategy, step concurrency will get reset when it's add_workflow path
    Properties resetAllowed =
        new PropertiesUpdate(Type.ADD_WORKFLOW_DEFINITION).getNewProperties(newProps, ps);
    assertNull(resetAllowed.getAlerting());
    assertNull(resetAllowed.getRunStrategy());
    assertNull(resetAllowed.getStepConcurrency());

    // owner will not get reset when it's add_workflow path
    assertNotNull(resetAllowed.getOwner());

    // properties explicitly reset will be reset when it's update_properties path
    PropertiesUpdate update = new PropertiesUpdate(Type.UPDATE_PROPERTIES);
    update.setResetRunStrategyRule(true);
    update.setResetStepConcurrency(true);
    Properties reset = update.getNewProperties(newProps, ps);

    assertNull(reset.getRunStrategy());
    assertNull(reset.getStepConcurrency());
  }
}
