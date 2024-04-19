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
import com.netflix.conductor.cockroachdb.CockroachDBDataSourceProvider;
import com.netflix.conductor.cockroachdb.CockroachDBTestConfiguration;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class CockroachDBBaseTest {
  static CockroachDBConfiguration config;
  static DataSource dataSource;
  static ObjectMapper objectMapper;

  static final String TEST_WORKFLOW_ID = "test-workflow";
  static final String TEST_WORKFLOW_NAME = TEST_WORKFLOW_ID + "name";
  static final String TEST_TASK_DEF_NAME = "test-task-def";
  static final String TEST_TASK_ID_1 = "test-task-1";
  static final String TEST_TASK_ID_2 = "test-task-2";

  @BeforeClass
  public static void init() {
    config = new CockroachDBTestConfiguration();
    dataSource = new CockroachDBDataSourceProvider(config).get();
    objectMapper = new JsonMapperProvider().get();
  }

  @AfterClass
  public static void destroy() {}

  Workflow createTestWorkflow(String workflowId) {
    WorkflowDef def = new WorkflowDef();
    def.setName(workflowId + "name");
    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setWorkflowDefinition(def);
    workflow.setStartTime(System.currentTimeMillis());
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    return workflow;
  }

  TaskDef createTaskDef(int rateLimit, int concurLimit) {
    TaskDef taskDef = new TaskDef(TEST_TASK_DEF_NAME);
    taskDef.setRateLimitFrequencyInSeconds(60);
    taskDef.setRateLimitPerFrequency(rateLimit);
    taskDef.setConcurrentExecLimit(concurLimit);
    return taskDef;
  }

  Task createRunningTestTask(String taskId) {
    Task task = new Task();
    task.setTaskId(taskId);
    task.setTaskDefName(TEST_TASK_DEF_NAME);
    task.setStatus(Task.Status.IN_PROGRESS);
    task.setStartTime(System.currentTimeMillis());
    task.setWorkflowInstanceId(TEST_WORKFLOW_ID);
    return task;
  }
}
