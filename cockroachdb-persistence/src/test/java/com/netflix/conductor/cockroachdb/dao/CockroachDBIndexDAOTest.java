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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CockroachDBIndexDAOTest extends CockroachDBBaseTest {

  private IndexDAO dao;
  private ExecutionDAO executionDAO;

  Workflow workflow;

  @Before
  public void setUp() {
    dao = new CockroachDBIndexDAO(dataSource, objectMapper, config);
    executionDAO =
        new CockroachDBExecutionDAO(
            dataSource,
            new CockroachDBIndexDAO(dataSource, objectMapper, config),
            objectMapper,
            config);
    workflow = createTestWorkflow(TEST_WORKFLOW_ID);
    executionDAO.createWorkflow(workflow);
  }

  @After
  public void tearDown() {
    executionDAO.removeWorkflow(TEST_WORKFLOW_ID);
  }

  @Test
  public void searchWorkflowsTest() {
    SearchResult<String> result = dao.searchWorkflows(TEST_WORKFLOW_NAME, "RUNNING", 0, 10, null);
    assertEquals(1, result.getTotalHits());
    assertEquals(TEST_WORKFLOW_ID, result.getResults().get(0));
    result = dao.searchWorkflows(TEST_WORKFLOW_ID, "RUNNING", 1, 10, null);
    assertEquals(0, result.getTotalHits());
  }

  @Test
  public void searchTasksTest() {
    SearchResult<String> result = dao.searchTasks(TEST_TASK_DEF_NAME, "IN_PROGRESS", 0, 10, null);
    assertEquals(0, result.getTotalHits());
    Task task = createRunningTestTask(TEST_TASK_ID_1);
    executionDAO.updateTask(task);
    result = dao.searchTasks(TEST_TASK_DEF_NAME, "IN_PROGRESS", 0, 10, null);
    assertEquals(1, result.getTotalHits());
    assertEquals(TEST_TASK_ID_1, result.getResults().get(0));
    result = dao.searchTasks(TEST_TASK_DEF_NAME, "IN_PROGRESS", 1, 10, null);
    assertEquals(0, result.getTotalHits());
  }

  @Test
  public void addTaskExecutionLogsTest() {
    List<TaskExecLog> logs = new ArrayList<>();
    logs.add(createLog(TEST_TASK_ID_1, "log1"));
    logs.add(createLog(TEST_TASK_ID_1, "log2"));
    logs.add(createLog(TEST_TASK_ID_1, "log3"));

    dao.addTaskExecutionLogs(logs);

    List<TaskExecLog> indexedLogs =
        tryFindResults(() -> dao.getTaskExecutionLogs(TEST_TASK_ID_1), 3);

    assertEquals(3, indexedLogs.size());

    assertTrue("Not all logs was indexed", indexedLogs.containsAll(logs));
  }

  @Test
  public void asyncAddTaskExecutionLogsTest() throws Exception {
    List<TaskExecLog> logs = new ArrayList<>();
    logs.add(createLog(TEST_TASK_ID_2, "log1"));
    logs.add(createLog(TEST_TASK_ID_2, "log2"));
    logs.add(createLog(TEST_TASK_ID_2, "log3"));

    dao.asyncAddTaskExecutionLogs(logs).get();

    List<TaskExecLog> indexedLogs =
        tryFindResults(() -> dao.getTaskExecutionLogs(TEST_TASK_ID_2), 3);

    assertEquals(3, indexedLogs.size());

    assertTrue("Not all logs was indexed", indexedLogs.containsAll(logs));
  }

  @Test
  public void getEventExecutionsTest() {
    String event = "event";
    EventExecution execution1 = createEventExecution(event);
    EventExecution execution2 = createEventExecution(event);

    executionDAO.addEventExecution(execution1);
    executionDAO.addEventExecution(execution2);

    List<EventExecution> indexedExecutions = tryFindResults(() -> dao.getEventExecutions(event), 2);

    assertEquals(2, indexedExecutions.size());

    assertTrue(
        "Not all event executions was indexed",
        indexedExecutions.containsAll(Arrays.asList(execution1, execution2)));
  }

  private <T> List<T> tryFindResults(Supplier<List<T>> searchFunction, int resultsCount) {
    List<T> result = Collections.emptyList();
    for (int i = 0; i < 20; i++) {
      result = searchFunction.get();
      if (result.size() == resultsCount) {
        return result;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return result;
  }

  private TaskExecLog createLog(String taskId, String log) {
    TaskExecLog taskExecLog = new TaskExecLog(log);
    taskExecLog.setTaskId(taskId);
    return taskExecLog;
  }

  private EventExecution createEventExecution(String event) {
    EventExecution execution = new EventExecution(uuid(), uuid());
    execution.setName("name");
    execution.setEvent(event);
    execution.setCreated(System.currentTimeMillis());
    execution.setStatus(EventExecution.Status.COMPLETED);
    execution.setAction(EventHandler.Action.Type.start_workflow);
    execution.setOutput(ImmutableMap.of("a", 1, "b", 2, "c", 3));
    return execution;
  }

  private String uuid() {
    return UUID.randomUUID().toString();
  }
}
