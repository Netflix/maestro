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

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.api.RestartPolicy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Optional;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RunRequestTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testExtractCurrentNodeFromRestartConfig() {
    AssertHelper.assertThrows(
        "Invalid get",
        IllegalArgumentException.class,
        "Cannot get restart info in empty restart configuration",
        () -> RunRequest.getCurrentNode(RestartConfig.builder().build()));
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    Assert.assertEquals(
        new RestartConfig.RestartNode("foo", 1, "bar"), RunRequest.getCurrentNode(config));
  }

  @Test
  public void testGetNextNode() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    Assert.assertEquals(Optional.empty(), RunRequest.getNextNode(config));
    config = config.toBuilder().addRestartNode("bar", 1, "bar").build();
    Assert.assertEquals(
        Optional.of(new RestartConfig.RestartNode("foo", 1, "bar")),
        RunRequest.getNextNode(config));
  }

  @Test
  public void testGetRestartWorkflowId() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    Assert.assertEquals("foo", runRequest.getRestartWorkflowId());
  }

  @Test
  public void testGtRestartInstanceId() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    Assert.assertEquals(1, runRequest.getRestartInstanceId());
  }

  @Test
  public void testGetRestartStepId() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    Assert.assertEquals("bar", runRequest.getRestartStepId());
  }

  @Test
  public void testUpdateForUpstream() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    runRequest.updateForUpstream("parent", 2, "sub-step");
    Assert.assertEquals("[parent][2]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("sub-step", runRequest.getRestartStepId());
    Assert.assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, runRequest.getCurrentPolicy());
  }

  @Test
  public void testUpdateForDownstreamIfMatch() {
    RestartConfig config =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .build();
    RunRequest runRequest =
        RunRequest.builder()
            .requester(User.create("tester"))
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    runRequest.updateForUpstream("parent", 2, "sub-step");
    Assert.assertEquals("[parent][2]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("sub-step", runRequest.getRestartStepId());
    Assert.assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, runRequest.getCurrentPolicy());

    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("foo");
    instance.setWorkflowInstanceId(1);
    runRequest.updateForDownstreamIfNeeded("sub-step", instance);
    Assert.assertEquals("[foo][1]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("bar", runRequest.getRestartStepId());
    Assert.assertEquals(RunPolicy.RESTART_FROM_BEGINNING, runRequest.getCurrentPolicy());
  }

  @Test
  public void testUpdateForDownstreamIfNotMatch() {
    RestartConfig config =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .build();
    RunRequest runRequest =
        RunRequest.builder()
            .requester(User.create("tester"))
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    runRequest.updateForUpstream("parent", 2, "sub-step");
    Assert.assertEquals("[parent][2]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("sub-step", runRequest.getRestartStepId());
    Assert.assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, runRequest.getCurrentPolicy());

    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("parent");
    instance.setWorkflowInstanceId(2);
    runRequest.updateForDownstreamIfNeeded("not-match", instance);
    Assert.assertNull(runRequest.getRestartConfig());
    Assert.assertEquals(RunPolicy.RESTART_FROM_INCOMPLETE, runRequest.getCurrentPolicy());
  }

  @Test
  public void testUpdateForDownstreamIfMatchInMiddle() {
    RestartConfig config =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .addRestartNode("parent", 2, "sub-step")
            .addRestartNode("grandparent", 3, "sub-step")
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .build();
    RunRequest runRequest =
        RunRequest.builder()
            .requester(User.create("tester"))
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .restartConfig(config)
            .build();

    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("parent");
    instance.setWorkflowInstanceId(2);
    runRequest.updateForDownstreamIfNeeded("sub-step", instance);
    Assert.assertEquals("[parent][2]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("sub-step", runRequest.getRestartStepId());
    Assert.assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, runRequest.getCurrentPolicy());
  }

  @Test
  public void testUpdateForDownstreamIfLastMatch() {
    RestartConfig config =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(config)
            .build();
    Assert.assertEquals("[foo][1]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("bar", runRequest.getRestartStepId());
    Assert.assertEquals(RunPolicy.RESTART_FROM_BEGINNING, runRequest.getCurrentPolicy());

    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("foo");
    instance.setWorkflowInstanceId(1);
    runRequest.updateForDownstreamIfNeeded("bar", instance);
    Assert.assertNull(runRequest.getRestartConfig());
    Assert.assertEquals(RunPolicy.RESTART_FROM_INCOMPLETE, runRequest.getCurrentPolicy());
  }

  @Test
  public void testClearRestartFor() {
    RestartConfig config =
        RestartConfig.builder()
            .addRestartNode("foo", 1, "bar")
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    runRequest.clearRestartFor(RunPolicy.RESTART_FROM_SPECIFIC);
    Assert.assertNull(runRequest.getRestartConfig());
    Assert.assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, runRequest.getCurrentPolicy());
  }

  @Test
  public void testGetWorkflowIdentity() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .build();
    Assert.assertEquals("[foo][1]", runRequest.getWorkflowIdentity());
    Assert.assertEquals("bar", runRequest.getRestartStepId());
  }

  @Test
  public void testIsFreshRun() {
    RunRequest runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RestartPolicy.RESTART_FROM_BEGINNING)
            .build();
    Assert.assertFalse(runRequest.isFreshRun());
    runRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Assert.assertTrue(runRequest.isFreshRun());
  }

  @Test
  public void testValidateIdentityForManualRun() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .requester(User.create("tester"))
            .build();
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("foo");
    instance.setWorkflowInstanceId(1);
    runRequest.validateIdentity(instance);

    instance.setWorkflowInstanceId(2);
    AssertHelper.assertThrows(
        "Mismatch instance identity",
        IllegalArgumentException.class,
        "Cannot restart a workflow instance ",
        () -> runRequest.validateIdentity(instance));
  }

  @Test
  public void testValidateIdentityForForeachRun() {
    RestartConfig config = RestartConfig.builder().addRestartNode("foo", 1, "bar").build();
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(config)
            .initiator(UpstreamInitiator.withType(Initiator.Type.FOREACH))
            .build();
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("foo");
    instance.setWorkflowInstanceId(2);
    runRequest.validateIdentity(instance);

    instance.setWorkflowId("foo1");
    AssertHelper.assertThrows(
        "Mismatch instance identity",
        IllegalArgumentException.class,
        "Cannot restart a FOREACH iteration ",
        () -> runRequest.validateIdentity(instance));
  }

  @Test
  public void testIsSystemInitiatedRun() {
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requester(User.create("tester"))
            .build();
    Assert.assertFalse(runRequest.isSystemInitiatedRun());

    runRequest =
        RunRequest.builder()
            .initiator(UpstreamInitiator.withType(Initiator.Type.FOREACH))
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Assert.assertTrue(runRequest.isSystemInitiatedRun());
  }

  @Test
  public void testGetRequester() {
    RunRequest runRequest =
        RunRequest.builder()
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requester(User.create("tester"))
            .build();
    Assert.assertEquals("tester", runRequest.getRequester().getName());

    runRequest =
        RunRequest.builder()
            .initiator(UpstreamInitiator.withType(Initiator.Type.FOREACH))
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Assert.assertNull(runRequest.getRequester());
  }
}
