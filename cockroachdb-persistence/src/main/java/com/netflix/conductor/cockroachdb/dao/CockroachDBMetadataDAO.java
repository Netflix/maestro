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
package com.netflix.conductor.cockroachdb.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.cockroachdb.util.StatementPreparer;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.MetadataDAO;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CockroachDB implementation of MetadataDAO.
 *
 * <p>It manages workflow and task definition metadata. TODO add local cache if needed
 *
 * @author jun-he
 */
public class CockroachDBMetadataDAO extends CockroachDBBaseDAO implements MetadataDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBMetadataDAO.class);

  private static final String UPSERT_TASK_DEFINITION_STATEMENT =
      "UPSERT INTO task_definition (task_name,payload) VALUES (?,?)";
  private static final String GET_TASK_DEFINITION_STATEMENT =
      "SELECT payload FROM task_definition where task_name = ?";
  private static final String GET_TASK_DEFINITIONS_STATEMENT =
      "SELECT payload FROM task_definition";
  private static final String REMOVE_TASK_DEFINITIONS_STATEMENT =
      "DELETE FROM task_definition WHERE task_name = ?";

  private static final String CREATE_WORKFLOW_DEFINITION_STATEMENT =
      "INSERT INTO workflow_definition (workflow_name,version,payload) VALUES (?,?,?)";
  private static final String UPSERT_WORKFLOW_DEFINITION_STATEMENT =
      "UPSERT INTO workflow_definition (workflow_name,version,payload) VALUES (?,?,?)";
  private static final String GET_WORKFLOW_DEFINITION_STATEMENT =
      "SELECT payload FROM workflow_definition where workflow_name = ? and version = ?";
  private static final String GET_LATEST_WORKFLOW_DEFINITION_STATEMENT =
      "SELECT payload FROM workflow_definition where workflow_name = ? order by version DESC LIMIT 1";
  private static final String GET_WORKFLOW_DEFINITIONS_STATEMENT =
      "SELECT payload FROM workflow_definition";
  private static final String REMOVE_WORKFLOW_DEFINITIONS_STATEMENT =
      "DELETE FROM workflow_definition WHERE workflow_name = ? and version = ?";

  public CockroachDBMetadataDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  @Override
  public void createTaskDef(TaskDef taskDef) {
    LOG.info("Creating a task definition: {}", taskDef);
    createOrUpdateTaskDefinition(taskDef, "createTaskDef");
  }

  @Override
  public String updateTaskDef(TaskDef taskDef) {
    LOG.info("Updating a task definition with the name: {}", taskDef.getName());
    return createOrUpdateTaskDefinition(taskDef, "updateTaskDef");
  }

  private String createOrUpdateTaskDefinition(TaskDef taskDef, String methodName) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                UPSERT_TASK_DEFINITION_STATEMENT,
                statement -> {
                  statement.setString(1, taskDef.getName());
                  statement.setString(2, toJson(taskDef));
                }),
        methodName,
        "Failed {} with task definition name {}",
        methodName,
        taskDef.getName());
    return taskDef.getName();
  }

  @Override
  public TaskDef getTaskDef(String name) {
    return withMetricLogError(
        () ->
            getPayload(
                GET_TASK_DEFINITION_STATEMENT,
                statement -> statement.setString(1, name),
                TaskDef.class),
        "getTaskDef",
        "Failed getting a task definition with name {}",
        name);
  }

  @Override
  public List<TaskDef> getAllTaskDefs() {
    return withMetricLogError(
        () -> getPayloads(GET_TASK_DEFINITIONS_STATEMENT, StatementPreparer.NO_OP, TaskDef.class),
        "getAllTaskDefs",
        "Failed getting all task definitions");
  }

  @Override
  public void removeTaskDef(String name) {
    LOG.info("Removing a task definition with name {}", name);
    withMetricLogError(
        () -> {
          int cnt =
              withRetryableUpdate(
                  REMOVE_TASK_DEFINITIONS_STATEMENT, statement -> statement.setString(1, name));
          if (cnt != 1) {
            throw new ApplicationException(
                ApplicationException.Code.NOT_FOUND,
                "Cannot remove the task - no such task definition with name " + name);
          }
          return cnt;
        },
        "removeTaskDef",
        "Failed removing a task definition with name {}",
        name);
  }

  @Override
  public void createWorkflowDef(WorkflowDef def) {
    LOG.info("Creating a workflow definition: {}", def);
    createOrUpdateWorkflowDefinition(
        CREATE_WORKFLOW_DEFINITION_STATEMENT, def, "createWorkflowDef");
  }

  @Override
  public void updateWorkflowDef(WorkflowDef def) {
    LOG.info("Updating a workflow definition with the name: {}", def.getName());
    createOrUpdateWorkflowDefinition(
        UPSERT_WORKFLOW_DEFINITION_STATEMENT, def, "updateWorkflowDef");
  }

  private void createOrUpdateWorkflowDefinition(
      String stmt, WorkflowDef workflowDef, String methodName) {
    withMetricLogError(
        () ->
            withRetryableUpdate(
                stmt,
                statement -> {
                  statement.setString(1, workflowDef.getName());
                  statement.setInt(2, workflowDef.getVersion());
                  statement.setString(3, toJson(workflowDef));
                }),
        methodName,
        "Failed {} with workflow definition name {} and version {}",
        methodName,
        workflowDef.getName(),
        workflowDef.getVersion());
  }

  @Override
  public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
    return Optional.ofNullable(
        withMetricLogError(
            () ->
                getPayload(
                    GET_LATEST_WORKFLOW_DEFINITION_STATEMENT,
                    statement -> statement.setString(1, name),
                    WorkflowDef.class),
            "getLatestWorkflowDef",
            "Failed getting the latest version of workflow definition with name {}",
            name));
  }

  @Override
  public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
    return Optional.ofNullable(
        withMetricLogError(
            () ->
                getPayload(
                    GET_WORKFLOW_DEFINITION_STATEMENT,
                    statement -> {
                      statement.setString(1, name);
                      statement.setInt(2, version);
                    },
                    WorkflowDef.class),
            "getWorkflowDef",
            "Failed getting a workflow definition with name {} and version {}",
            name,
            version));
  }

  @Override
  public void removeWorkflowDef(String name, Integer version) {
    LOG.info("Removing a workflow definition with name {} and version {}", name, version);
    withMetricLogError(
        () -> {
          int cnt =
              withRetryableUpdate(
                  REMOVE_WORKFLOW_DEFINITIONS_STATEMENT,
                  statement -> {
                    statement.setString(1, name);
                    statement.setInt(2, version);
                  });
          if (cnt != 1) {
            throw new ApplicationException(
                ApplicationException.Code.NOT_FOUND,
                String.format(
                    "Cannot remove the workflow - no such workflow definition with name %s and version %s",
                    name, version));
          }
          return cnt;
        },
        "removeWorkflowDef",
        "Failed removing a workflow definition with name {} and version {}",
        name,
        version);
  }

  @Override
  public List<WorkflowDef> getAllWorkflowDefs() {
    return withMetricLogError(
        () ->
            getPayloads(
                GET_WORKFLOW_DEFINITIONS_STATEMENT, StatementPreparer.NO_OP, WorkflowDef.class),
        "getAllWorkflowDefs",
        "Failed getting all workflow definitions");
  }
}
