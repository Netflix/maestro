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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CockroachDBExecutionDAOTest extends CockroachDBBaseTest {
  private ExecutionDAO dao;

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    dao =
        new CockroachDBExecutionDAO(
            dataSource,
            new CockroachDBIndexDAO(dataSource, objectMapper, config),
            objectMapper,
            config);
    dao.createWorkflow(createTestWorkflow(TEST_WORKFLOW_ID));
  }

  @After
  public void tearDown() {
    dao.removeWorkflow(TEST_WORKFLOW_ID);
  }

  public ExecutionDAO getExecutionDAO() {
    return dao;
  }

  @Test
  public void testTaskExceedsLimit() {
    TaskDef taskDefinition = createTaskDef(0, 1);
    WorkflowTask workflowTask = new WorkflowTask();
    workflowTask.setName("task1");
    workflowTask.setTaskDefinition(taskDefinition);

    List<Task> tasks = new LinkedList<>();
    for (int i = 0; i < 15; i++) {
      Task task = new Task();
      task.setScheduledTime(1L);
      task.setSeq(i + 1);
      task.setTaskId("t_" + i);
      task.setWorkflowInstanceId(TEST_WORKFLOW_ID);
      task.setReferenceTaskName("task_" + i);
      task.setTaskDefName(taskDefinition.getName());
      tasks.add(task);
      task.setStatus(Task.Status.SCHEDULED);
      task.setWorkflowTask(workflowTask);
    }

    getExecutionDAO().createTasks(tasks);
    assertFalse(getExecutionDAO().exceedsInProgressLimit(tasks.get(0)));
    tasks.get(0).setStatus(Task.Status.IN_PROGRESS);
    getExecutionDAO().updateTask(tasks.get(0));
    assertFalse(getExecutionDAO().exceedsInProgressLimit(tasks.get(0)));

    tasks.remove(0);
    for (Task task : tasks) {
      assertTrue(getExecutionDAO().exceedsInProgressLimit(task));
    }
  }

  @Test
  public void testTaskExceedsLimitCrossWorkflows() {
    TaskDef taskDefinition = createTaskDef(0, 1);
    WorkflowTask workflowTask = new WorkflowTask();
    workflowTask.setName("task1");
    workflowTask.setTaskDefinition(taskDefinition);

    List<Task> tasks = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      Task task = new Task();
      task.setScheduledTime(1L);
      task.setSeq(i + 1);
      task.setTaskId("t_" + i);
      task.setWorkflowInstanceId(TEST_WORKFLOW_ID + i);
      task.setReferenceTaskName("task_" + i);
      task.setTaskDefName(taskDefinition.getName());
      tasks.add(task);
      task.setStatus(Task.Status.SCHEDULED);
      task.setWorkflowTask(workflowTask);
      dao.createWorkflow(createTestWorkflow(TEST_WORKFLOW_ID + i));
      dao.createTasks(Collections.singletonList(task));
    }

    Task firstTask = tasks.remove(0);
    assertFalse(getExecutionDAO().exceedsInProgressLimit(firstTask));
    firstTask.setStatus(Task.Status.IN_PROGRESS);
    getExecutionDAO().updateTask(firstTask);
    assertFalse(getExecutionDAO().exceedsInProgressLimit(firstTask));

    for (Task task : tasks) {
      assertTrue(getExecutionDAO().exceedsInProgressLimit(task));
      dao.removeWorkflow(task.getWorkflowInstanceId());
    }
    dao.removeWorkflow(firstTask.getWorkflowInstanceId());
  }

  @Test
  public void testCreateTaskException() {
    Task task = new Task();
    task.setScheduledTime(1L);
    task.setSeq(1);
    task.setTaskId(TEST_TASK_ID_1);
    task.setTaskDefName(TEST_TASK_DEF_NAME);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Workflow instance id cannot be null");
    getExecutionDAO().createTasks(Collections.singletonList(task));

    task.setWorkflowInstanceId(TEST_WORKFLOW_ID);
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Task reference name cannot be null");
    getExecutionDAO().createTasks(Collections.singletonList(task));
  }

  @Test
  public void testTaskCreateDups() {
    List<Task> tasks = new ArrayList<>();
    String workflowId = TEST_WORKFLOW_ID;

    for (int i = 0; i < 3; i++) {
      Task task = new Task();
      task.setScheduledTime(1L);
      task.setSeq(i + 1);
      task.setTaskId(workflowId + "_t" + i);
      task.setReferenceTaskName("t" + i);
      task.setRetryCount(0);
      task.setWorkflowInstanceId(workflowId);
      task.setTaskDefName("task" + i);
      task.setStatus(Task.Status.IN_PROGRESS);
      tasks.add(task);
    }

    // insert a retried task
    Task task = new Task();
    task.setScheduledTime(1L);
    task.setSeq(1);
    task.setTaskId(workflowId + "_t" + 2 + "_" + 1); // it should has a different task id
    task.setRetriedTaskId(workflowId + "_t" + 2);
    task.setReferenceTaskName("t" + 2);
    task.setRetryCount(1);
    task.setWorkflowInstanceId(workflowId);
    task.setTaskDefName("task" + 2);
    task.setStatus(Task.Status.IN_PROGRESS);
    tasks.add(task);

    // duplicate task
    task = new Task();
    task.setScheduledTime(1L);
    task.setSeq(1);
    task.setTaskId(workflowId + "_t" + 1);
    task.setReferenceTaskName("t" + 1);
    task.setRetryCount(0);
    task.setWorkflowInstanceId(workflowId);
    task.setTaskDefName("task" + 1);
    task.setStatus(Task.Status.IN_PROGRESS);
    tasks.add(task);

    List<Task> created = getExecutionDAO().createTasks(tasks);
    assertEquals(tasks.size() - 1, created.size()); // 1 less

    Set<String> srcIds =
        tasks.stream()
            .map(t -> t.getReferenceTaskName() + "." + t.getRetryCount())
            .collect(Collectors.toSet());
    Set<String> createdIds =
        created.stream()
            .map(t -> t.getReferenceTaskName() + "." + t.getRetryCount())
            .collect(Collectors.toSet());

    assertEquals(srcIds, createdIds);

    List<Task> pending = getExecutionDAO().getPendingTasksByWorkflow("task0", workflowId);
    assertNotNull(pending);
    assertEquals(1, pending.size());
    assertEquals(tasks.get(0), pending.get(0));

    List<Task> found = getExecutionDAO().getTasks(tasks.get(0).getTaskDefName(), null, 1);
    assertNotNull(found);
    assertEquals(1, found.size());
    assertEquals(tasks.get(0), found.get(0));
  }

  @Test
  public void testTaskOps() {
    List<Task> tasks = new LinkedList<>();
    String workflowId = TEST_WORKFLOW_ID;

    for (int i = 0; i < 3; i++) {
      Task task = new Task();
      task.setScheduledTime(1L);
      task.setSeq(1);
      task.setTaskId(workflowId + "_t" + i);
      task.setReferenceTaskName("testTaskOps" + i);
      task.setRetryCount(0);
      task.setWorkflowInstanceId(workflowId);
      task.setTaskDefName("testTaskOps" + i);
      task.setStatus(Task.Status.IN_PROGRESS);
      tasks.add(task);
    }

    dao.createWorkflow(createTestWorkflow("x" + workflowId));

    for (int i = 0; i < 3; i++) {
      Task task = new Task();
      task.setScheduledTime(1L);
      task.setSeq(1);
      task.setTaskId("x" + workflowId + "_t" + i);
      task.setReferenceTaskName("testTaskOps" + i);
      task.setRetryCount(0);
      task.setWorkflowInstanceId("x" + workflowId);
      task.setTaskDefName("testTaskOps" + i);
      task.setStatus(Task.Status.IN_PROGRESS);
      getExecutionDAO().createTasks(Collections.singletonList(task));
    }

    List<Task> created = getExecutionDAO().createTasks(tasks);
    assertEquals(tasks.size(), created.size());

    List<Task> pending =
        getExecutionDAO().getPendingTasksForTaskType(tasks.get(0).getTaskDefName());
    assertNotNull(pending);
    assertEquals(2, pending.size());
    // Pending list can come in any order.  finding the one we are looking for and then comparing
    Task matching =
        pending.stream()
            .filter(task -> task.getTaskId().equals(tasks.get(0).getTaskId()))
            .findAny()
            .get();
    assertEquals(matching, tasks.get(0));

    for (int i = 0; i < 3; i++) {
      Task found = getExecutionDAO().getTask(workflowId + "_t" + i);
      assertNotNull(found);
      found.getOutputData().put("updated", true);
      found.setStatus(Task.Status.COMPLETED);
      getExecutionDAO().updateTask(found);
    }

    List<String> taskIds = tasks.stream().map(Task::getTaskId).collect(Collectors.toList());
    List<Task> found = getExecutionDAO().getTasks(taskIds);
    assertEquals(taskIds.size(), found.size());
    found.forEach(
        task -> {
          assertTrue(task.getOutputData().containsKey("updated"));
          assertEquals(true, task.getOutputData().get("updated"));
          boolean removed = getExecutionDAO().removeTask(task.getTaskId());
          assertTrue(removed);
        });

    found = getExecutionDAO().getTasks(taskIds);
    assertTrue(found.isEmpty());
    dao.removeWorkflow("x" + workflowId);
  }

  @Test
  public void testPending() {
    WorkflowDef def = new WorkflowDef();
    def.setName("pending_count_test");

    Workflow workflow = createTestWorkflow(TEST_WORKFLOW_ID);
    workflow.setWorkflowDefinition(def);

    List<String> workflowIds = generateWorkflows(workflow, 10);
    long count = getExecutionDAO().getPendingWorkflowCount(def.getName());
    assertEquals(10, count);

    for (String workflowId : workflowIds) {
      getExecutionDAO().removeWorkflow(workflowId);
    }

    count = getExecutionDAO().getPendingWorkflowCount(def.getName());
    assertEquals(0, count);
  }

  @Test
  public void complexExecutionTest() {
    Workflow workflow = createComplexTestWorkflow();
    int numTasks = workflow.getTasks().size();

    String workflowId = getExecutionDAO().updateWorkflow(workflow);
    assertEquals(workflow.getWorkflowId(), workflowId);

    List<Task> created = getExecutionDAO().createTasks(workflow.getTasks());
    assertEquals(workflow.getTasks().size(), created.size());

    Workflow workflowWithTasks = getExecutionDAO().getWorkflow(workflow.getWorkflowId(), true);
    assertEquals(workflowId, workflowWithTasks.getWorkflowId());
    assertEquals(numTasks, workflowWithTasks.getTasks().size());

    Workflow found = getExecutionDAO().getWorkflow(workflowId, false);
    assertTrue(found.getTasks().isEmpty());

    workflow.getTasks().clear();
    assertEquals(workflow, found);

    workflow.getInput().put("updated", true);
    getExecutionDAO().updateWorkflow(workflow);
    found = getExecutionDAO().getWorkflow(workflowId);
    assertNotNull(found);
    assertTrue(found.getInput().containsKey("updated"));
    assertEquals(true, found.getInput().get("updated"));

    List<String> running =
        getExecutionDAO()
            .getRunningWorkflowIds(workflow.getWorkflowName(), workflow.getWorkflowVersion());
    assertNotNull(running);
    assertTrue(running.isEmpty());

    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    getExecutionDAO().updateWorkflow(workflow);

    running =
        getExecutionDAO()
            .getRunningWorkflowIds(workflow.getWorkflowName(), workflow.getWorkflowVersion());
    assertNotNull(running);
    assertEquals(1, running.size());
    assertEquals(workflow.getWorkflowId(), running.get(0));

    List<Workflow> pending =
        getExecutionDAO()
            .getPendingWorkflowsByType(workflow.getWorkflowName(), workflow.getWorkflowVersion());
    assertNotNull(pending);
    assertEquals(1, pending.size());
    assertEquals(3, pending.get(0).getTasks().size());
    pending.get(0).getTasks().clear();
    assertEquals(workflow, pending.get(0));

    List<Workflow> bytime =
        getExecutionDAO()
            .getWorkflowsByType(
                workflow.getWorkflowName(),
                workflow.getCreateTime() - 10,
                workflow.getCreateTime() + 10);
    assertNotNull(bytime);
    assertEquals(1, bytime.size());

    workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
    getExecutionDAO().updateWorkflow(workflow);
    running =
        getExecutionDAO()
            .getRunningWorkflowIds(workflow.getWorkflowName(), workflow.getWorkflowVersion());
    assertNotNull(running);
    assertTrue(running.isEmpty());

    bytime =
        getExecutionDAO()
            .getWorkflowsByType(
                workflow.getWorkflowName(),
                workflow.getStartTime() - 10,
                workflow.getStartTime() + 10);
    assertNotNull(bytime);
    assertEquals(0, bytime.size());
    bytime =
        getExecutionDAO()
            .getWorkflowsByType(
                workflow.getWorkflowName(),
                workflow.getStartTime() + 100,
                workflow.getStartTime() + 200);
    assertNotNull(bytime);
    assertTrue(bytime.isEmpty());
  }

  protected Workflow createComplexTestWorkflow() {
    WorkflowDef def = new WorkflowDef();
    def.setName(TEST_WORKFLOW_ID);
    def.setVersion(3);
    def.setSchemaVersion(2);

    Workflow workflow = new Workflow();
    workflow.setWorkflowDefinition(def);
    workflow.setCorrelationId("correlationX");
    workflow.setCreatedBy("junit_tester");
    workflow.setEndTime(200L);

    Map<String, Object> input = new HashMap<>();
    input.put("param1", "param1 value");
    input.put("param2", 100);
    workflow.setInput(input);

    Map<String, Object> output = new HashMap<>();
    output.put("ouput1", "output 1 value");
    output.put("op2", 300);
    workflow.setOutput(output);

    workflow.setOwnerApp("workflow");
    workflow.setParentWorkflowId("parentWorkflowId");
    workflow.setParentWorkflowTaskId("parentWFTaskId");
    workflow.setReasonForIncompletion("missing recipe");
    workflow.setReRunFromWorkflowId("re-run from id1");
    workflow.setStartTime(90L);
    workflow.setStatus(Workflow.WorkflowStatus.FAILED);
    workflow.setWorkflowId(TEST_WORKFLOW_ID);

    List<Task> tasks = new LinkedList<>();

    Task task1 = new Task();
    task1.setScheduledTime(1L);
    task1.setSeq(1);
    task1.setTaskId(UUID.randomUUID().toString());
    task1.setReferenceTaskName("t1");
    task1.setWorkflowInstanceId(workflow.getWorkflowId());
    task1.setTaskDefName("task1");

    Task task2 = new Task();
    task2.setScheduledTime(2L);
    task2.setSeq(2);
    task2.setTaskId(UUID.randomUUID().toString());
    task2.setReferenceTaskName("t2");
    task2.setWorkflowInstanceId(workflow.getWorkflowId());
    task2.setTaskDefName("task2");

    Task task3 = new Task();
    task3.setScheduledTime(2L);
    task3.setSeq(3);
    task3.setTaskId(UUID.randomUUID().toString());
    task3.setReferenceTaskName("t3");
    task3.setWorkflowInstanceId(workflow.getWorkflowId());
    task3.setTaskDefName("task3");

    tasks.add(task1);
    tasks.add(task2);
    tasks.add(task3);

    workflow.setTasks(tasks);

    workflow.setUpdatedBy("junit_tester");
    workflow.setUpdateTime(800L);

    return workflow;
  }

  protected List<String> generateWorkflows(Workflow base, int count) {
    List<String> workflowIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String workflowId = UUID.randomUUID().toString();
      base.setWorkflowId(workflowId);
      base.setCorrelationId("corr001");
      base.setStatus(Workflow.WorkflowStatus.RUNNING);
      getExecutionDAO().createWorkflow(base);
      workflowIds.add(workflowId);
    }
    return workflowIds;
  }
}
