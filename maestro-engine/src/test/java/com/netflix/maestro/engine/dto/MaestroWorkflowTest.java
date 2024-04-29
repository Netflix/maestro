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
package com.netflix.maestro.engine.dto;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.Metadata;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import org.junit.Test;

public class MaestroWorkflowTest extends MaestroBaseTest {

  @Test
  public void testMaestroWorkflowValidation() {
    Properties properties = new Properties();
    properties.setOwner(User.create("bar"));
    PropertiesSnapshot snapshot =
        PropertiesSnapshot.create("test", 123L, User.create("foo"), properties);
    AssertHelper.assertThrows(
        "workflowId cannot be null",
        NullPointerException.class,
        "workflowId cannot be null",
        () ->
            MaestroWorkflow.builder()
                .propertiesSnapshot(snapshot)
                .activeVersionId(0L)
                .metadata(new Metadata())
                .definition(Workflow.builder().build())
                .internalId(12345L)
                .modifyTime(System.currentTimeMillis())
                .build());

    AssertHelper.assertThrows(
        "Unique internalId cannot be null",
        NullPointerException.class,
        "unique internalId cannot be null",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .propertiesSnapshot(snapshot)
                .activeVersionId(0L)
                .metadata(new Metadata())
                .definition(Workflow.builder().build())
                .modifyTime(System.currentTimeMillis())
                .build());

    AssertHelper.assertThrows(
        "properties snapshot cannot be null",
        NullPointerException.class,
        "properties snapshot cannot be null",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .activeVersionId(0L)
                .metadata(new Metadata())
                .definition(Workflow.builder().build())
                .internalId(12345L)
                .modifyTime(System.currentTimeMillis())
                .build());

    properties.setOwner(null);
    PropertiesSnapshot snapshotWithoutOwner =
        PropertiesSnapshot.create("test", 123L, User.create("foo"), properties);
    AssertHelper.assertThrows(
        "owner cannot be null",
        NullPointerException.class,
        "owner cannot be null",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .propertiesSnapshot(snapshotWithoutOwner)
                .activeVersionId(0L)
                .metadata(new Metadata())
                .definition(Workflow.builder().build())
                .internalId(12345L)
                .modifyTime(System.currentTimeMillis())
                .build());

    properties.setOwner(User.create("foo"));
    PropertiesSnapshot snapshotWithoutCreateTime =
        PropertiesSnapshot.create("test", null, User.create("foo"), properties);
    AssertHelper.assertThrows(
        "create time cannot be null",
        NullPointerException.class,
        "create time cannot be null",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .propertiesSnapshot(snapshotWithoutCreateTime)
                .activeVersionId(0L)
                .metadata(new Metadata())
                .definition(Workflow.builder().build())
                .internalId(12345L)
                .modifyTime(System.currentTimeMillis())
                .build());

    AssertHelper.assertThrows(
        "Active version cannot be null",
        NullPointerException.class,
        "Active version cannot be null",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .propertiesSnapshot(snapshot)
                .metadata(new Metadata())
                .definition(Workflow.builder().build())
                .internalId(12345L)
                .modifyTime(System.currentTimeMillis())
                .build());

    AssertHelper.assertThrows(
        "latest version id cannot be null",
        NullPointerException.class,
        "latest version id cannot be null for",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .propertiesSnapshot(snapshot)
                .activeVersionId(0L)
                .internalId(12345L)
                .modifyTime(System.currentTimeMillis())
                .build());

    AssertHelper.assertThrows(
        "modify time cannot be null",
        NullPointerException.class,
        "modify time cannot be null for",
        () ->
            MaestroWorkflow.builder()
                .workflowId("test-workflow")
                .propertiesSnapshot(snapshot)
                .internalId(12345L)
                .activeVersionId(0L)
                .latestVersionId(0L)
                .build());
  }
}
