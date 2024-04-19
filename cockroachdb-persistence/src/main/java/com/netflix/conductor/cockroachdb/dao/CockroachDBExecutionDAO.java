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
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CockroachDB implementation of Execution DAO.
 *
 * @author jun-he
 */
public class CockroachDBExecutionDAO extends CockroachDBBaseDAO implements ExecutionDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBExecutionDAO.class);

  private static final String WORKFLOW_FLAG_COLUMN = "flag";

  // TODO change it back to INSERT after fixing conductor issue
  private static final String CREATE_TASK_STATEMENT =
      "UPSERT INTO task (workflow_instance_id,task_id,payload) VALUES (?,?,?)";
  private static final String UPSERT_TASK_STATEMENT =
      "UPSERT INTO task (workflow_instance_id,task_id,payload) VALUES (?,?,?)";
  private static final String GET_TASK_BY_TASK_ID_STATEMENT =
      "SELECT payload FROM task WHERE task_id = ?";
  private static final String GET_TASKS_BY_WORKFLOW_INSTANCE_ID_STATEMENT =
      "SELECT payload FROM task WHERE workflow_instance_id = ?";
  private static final String REMOVE_TASK_STATEMENT = "DELETE FROM task WHERE task_id = ?";
  private static final String REMOVE_TASKS_STATEMENT =
      "DELETE FROM task WHERE workflow_instance_id = ?";
  private static final String GET_RUNNING_TASK_IDS_BY_NAME_STATEMENT =
      "SELECT task_id AS id FROM task WHERE task_name = ? AND status = 'IN_PROGRESS' ";

  private static final String CREATE_WORKFLOW_INSTANCE_STATEMENT =
      "INSERT INTO workflow_instance (workflow_instance_id,payload) VALUES (?,?)";
  private static final String UPSERT_WORKFLOW_INSTANCE_STATEMENT =
      "UPSERT INTO workflow_instance (workflow_instance_id,payload) VALUES (?,?)";
  private static final String REMOVE_WORKFLOW_INSTANCE_STATEMENT =
      "DELETE FROM workflow_instance WHERE workflow_instance_id = ?";
  private static final String GET_WORKFLOW_INSTANCE_ONLY_STATEMENT =
      "SELECT TRUE AS flag, payload FROM workflow_instance WHERE workflow_instance_id = ? AND workflow_instance_id = ?";
  private static final String GET_WORKFLOW_INSTANCE_WITH_TASKS_STATEMENT =
      "SELECT TRUE AS flag, payload FROM workflow_instance WHERE workflow_instance_id = ? "
          + "UNION ALL SELECT FALSE AS flag, payload FROM task WHERE workflow_instance_id = ?";

  private static final String CREATE_EVENT_EXECUTION_STATEMENT =
      "INSERT INTO event_execution (event,handler_name,message_id,execution_id,payload) VALUES (?,?,?,?,?)";
  private static final String UPSERT_EVENT_EXECUTION_STATEMENT =
      "UPSERT INTO event_execution (event,handler_name,message_id,execution_id,payload) VALUES (?,?,?,?,?)";
  private static final String REMOVE_EVENT_EXECUTION_STATEMENT =
      "DELETE FROM event_execution WHERE event = ? AND handler_name = ? AND message_id = ? AND execution_id = ?";

  private final IndexDAO indexDAO;
  private final int insertBatchSize;
  private final int maxSearchSize;

  public CockroachDBExecutionDAO(
      DataSource dataSource,
      IndexDAO indexDAO,
      ObjectMapper objectMapper,
      CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
    this.indexDAO = indexDAO;
    this.insertBatchSize = config.getDbInsertBatchSize();
    this.maxSearchSize = config.getDbMaxSearchSize();
  }

  /**
   * Get in processing tasks within a workflow with the given workflow instance id
   *
   * @param taskName: related task's TaskType
   * @param workflowId: related workflow instance id
   * @return a list of tasks with TaskType = taskName for a given workflow with WorkflowId =
   *     workflowId
   */
  @Override
  public List<Task> getPendingTasksByWorkflow(String taskName, String workflowId) {
    List<Task> tasks = getTasksForWorkflow(workflowId);
    return tasks.stream()
        .filter(task -> taskName.equals(task.getTaskDefName()))
        .filter(task -> Task.Status.IN_PROGRESS.equals(task.getStatus()))
        .collect(Collectors.toList());
  }

  /**
   * It is used in paginated get in-progress tasks HTTP or GRPC endpoint.
   *
   * <p>First, it gets the taskIds from {@link IndexDAO} using the secondary index with follower
   * read mode.
   *
   * <p>Second, it gets tasks one by one (there is no transactional guarantee) with follower read
   * mode.
   */
  @Override
  public List<Task> getTasks(String taskName, String startKey, int count) {
    List<String> taskIds = searchInProgressTaskIdsByName(taskName);
    int startIdx = startKey == null ? 0 : taskIds.indexOf(startKey);
    if (startIdx < 0) {
      return Collections.emptyList();
    }
    return taskIds.stream()
        .skip(startIdx)
        .limit(count)
        .map(this::getReadOnlyTask)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private List<String> searchInProgressTaskIdsByName(String taskDefName) {
    return indexDAO.searchTasks(taskDefName, "IN_PROGRESS", 0, maxSearchSize, null).getResults();
  }

  /**
   * Inserts new tasks for a single workflow into the CockroachDB.
   *
   * @param tasks tasks to be inserted to DB
   * @return created tasks
   */
  @Override
  public List<Task> createTasks(List<Task> tasks) {
    validateTasks(tasks);
    Collection<Task> pendingTasks =
        tasks.stream()
            .collect(
                Collectors.toMap(Task::getTaskId, Function.identity(), (u, v) -> v, TreeMap::new))
            .values();

    return withMetricLogError(
        () -> {
          int cnt =
              withRetryableStatement(
                  CREATE_TASK_STATEMENT,
                  statement -> {
                    int count = 0;
                    int inserted = 0;
                    for (Task task : pendingTasks) {
                      addTask(statement, task);
                      statement.addBatch();
                      count++;
                      // batch INSERT with a batch size
                      if (count % insertBatchSize == 0 || count == pendingTasks.size()) {
                        int[] res = statement.executeBatch();
                        inserted += res.length;
                      }
                    }
                    return inserted;
                  });
          LOG.debug(
              "Created {}/{}/{} task instances for workflow {}",
              cnt,
              pendingTasks.size(),
              tasks.size(),
              tasks.get(0).getWorkflowInstanceId());
          return new ArrayList<>(pendingTasks);
        },
        "createTasks",
        "Fail creating {} task instances for workflow: {}",
        tasks.size(),
        tasks.get(0).getWorkflowInstanceId());
  }

  private void addTask(PreparedStatement statement, Task task) throws SQLException {
    statement.setString(1, task.getWorkflowInstanceId());
    statement.setString(2, task.getTaskId());
    statement.setString(3, toJson(task));
  }

  @Override
  public void updateTask(Task task) {
    withMetricLogError(
        () -> {
          int cnt =
              withRetryableTransaction(
                  conn -> {
                    int updated;
                    try (PreparedStatement statement =
                        conn.prepareStatement(UPSERT_TASK_STATEMENT)) {
                      addTask(statement, task);
                      updated = statement.executeUpdate();
                    }
                    return updated;
                  });
          LOG.debug(
              "updated {} task {} in a workflow instance {}",
              cnt,
              task.getTaskId(),
              task.getWorkflowInstanceId());
          return cnt;
        },
        "updateTask",
        "Failed updating a task with id {} in a workflow instance {}",
        task.getTaskId(),
        task.getWorkflowInstanceId());
  }

  @Override
  public boolean exceedsInProgressLimit(Task task) {
    Optional<TaskDef> taskDefinition = task.getTaskDefinition();
    if (!taskDefinition.isPresent()) {
      return false;
    }
    int limit = taskDefinition.get().concurrencyLimit();
    if (limit <= 0) {
      return false;
    }

    try {
      List<String> taskIds =
          withRetryableQuery(
              GET_RUNNING_TASK_IDS_BY_NAME_STATEMENT,
              statement -> statement.setString(1, task.getTaskDefName()),
              result -> idsFromResult(result, -1));
      if (!taskIds.contains(task.getTaskId()) && taskIds.size() >= limit) {
        LOG.info(
            "Task execution count is limited. task - {}:{}, limit: {}, current: {}",
            task.getTaskId(),
            task.getTaskDefName(),
            limit,
            taskIds.size());
        return true;
      } else {
        LOG.debug(
            "Task execution count is not limited. task - {}:{}, limit: {}, current: {}",
            task.getTaskId(),
            task.getTaskDefName(),
            limit,
            taskIds.size());
        return false;
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed checking in progress limit for task - {}:{} due to {}",
          task.getTaskId(),
          task.getTaskDefName(),
          e.getMessage());
      return true;
    }
  }

  @Override
  public boolean removeTask(String taskId) {
    return withRetryableStatement(
        REMOVE_TASK_STATEMENT,
        statement -> {
          statement.setString(1, taskId);
          if (statement.executeUpdate() == 1) {
            return true;
          } else {
            LOG.warn("cannot remove a notfound task with id {}", taskId);
            return false;
          }
        });
  }

  @Override
  public Task getTask(String taskId) {
    return withMetricLogError(
        () ->
            getPayload(
                GET_TASK_BY_TASK_ID_STATEMENT,
                statement -> statement.setString(1, taskId),
                Task.class),
        "getTask",
        "Failed getting a task by id {}",
        taskId);
  }

  private Task getReadOnlyTask(String taskId) {
    try {
      return getReadOnlyPayload(
          GET_TASK_BY_TASK_ID_STATEMENT, statement -> statement.setString(1, taskId), Task.class);
    } catch (Exception e) {
      LOG.warn("Failed getting a task by id {} due to {}", taskId, e.getMessage());
      return null;
    }
  }

  /** Best effort to get all Tasks for the given task ids. */
  @Override
  public List<Task> getTasks(List<String> taskIds) {
    return taskIds.stream()
        .map(this::getTask)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * It is used by admin and requeue API First, it gets the taskIds from {@link IndexDAO} using the
   * secondary index with follower read mode. Second, it gets tasks one by one (there is no
   * transactional guarantee) with follower read mode.
   */
  @Override
  public List<Task> getPendingTasksForTaskType(String taskType) {
    return searchInProgressTaskIdsByName(taskType).stream()
        .map(this::getReadOnlyTask)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public List<Task> getTasksForWorkflow(String workflowId) {
    List<Task> tasks =
        withMetricLogError(
            () ->
                getPayloads(
                    GET_TASKS_BY_WORKFLOW_INSTANCE_ID_STATEMENT,
                    statement -> statement.setString(1, workflowId),
                    Task.class),
            "getTasksForWorkflow",
            "Failed getting tasks for workflow with id {}",
            workflowId);
    tasks.sort(Comparator.comparingInt(Task::getSeq));
    return tasks;
  }

  @Override
  public String createWorkflow(Workflow workflow) {
    LOG.debug(
        "Creating a workflow instance with the name {} and id: {}",
        workflow.getWorkflowName(),
        workflow.getWorkflowId());
    return createOrUpdateWorkflow(workflow, CREATE_WORKFLOW_INSTANCE_STATEMENT, "createWorkflow");
  }

  @Override
  public String updateWorkflow(Workflow workflow) {
    LOG.debug(
        "Updating a workflow instance with the name {} and id: {}",
        workflow.getWorkflowName(),
        workflow.getWorkflowId());
    return createOrUpdateWorkflow(workflow, UPSERT_WORKFLOW_INSTANCE_STATEMENT, "updateWorkflow");
  }

  private String createOrUpdateWorkflow(Workflow workflow, String stmt, String methodName) {
    validateWorkflow(workflow);
    List<Task> tasks = workflow.getTasks();
    workflow.setTasks(Collections.emptyList());

    withMetricLogError(
        () ->
            withRetryableUpdate(
                stmt,
                statement -> {
                  statement.setString(1, workflow.getWorkflowId());
                  statement.setString(2, toJson(workflow));
                }),
        methodName,
        "Failed {} with workflow instance id {}",
        methodName,
        workflow.getWorkflowId());

    workflow.setTasks(tasks);
    return workflow.getWorkflowId();
  }

  @Override
  public boolean removeWorkflow(String workflowId) {
    LOG.debug("Removing a workflow instance with an id {}", workflowId);
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement wiStmt =
                          conn.prepareStatement(REMOVE_WORKFLOW_INSTANCE_STATEMENT);
                      PreparedStatement siStmt = conn.prepareStatement(REMOVE_TASKS_STATEMENT)) {
                    // remove associated tasks first
                    siStmt.setString(1, workflowId);
                    siStmt.executeUpdate();
                    wiStmt.setString(1, workflowId);
                    if (wiStmt.executeUpdate() == 1) {
                      LOG.info("Removed a workflow instance with id {}", workflowId);
                      return true;
                    } else {
                      LOG.warn("Cannot remove a notfound workflow with id {}", workflowId);
                      return false;
                    }
                  }
                }),
        "removeWorkflow",
        "Failed removing a workflow instance {}",
        workflowId);
  }

  /**
   * Currently, this feature is not implemented for CockroachDBExecutionDAO. It is not used by
   * conductor core and only used by Archiving Module
   */
  @Override
  public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
    throw new UnsupportedOperationException(
        "This method is not implemented in CockroachDBExecutionDAO.");
  }

  @Override
  public void removeFromPendingWorkflow(String workflowType, String workflowId) {
    // no-op
  }

  @Override
  public Workflow getWorkflow(String workflowId) {
    return getWorkflow(workflowId, true);
  }

  @Override
  public Workflow getWorkflow(String workflowId, boolean includeTasks) {
    return getWorkflow(
        workflowId,
        includeTasks
            ? GET_WORKFLOW_INSTANCE_WITH_TASKS_STATEMENT
            : GET_WORKFLOW_INSTANCE_ONLY_STATEMENT);
  }

  private Workflow getWorkflow(String workflowId, String getStatement) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                getStatement,
                statement -> {
                  statement.setString(1, workflowId);
                  statement.setString(2, workflowId);
                },
                this::workflowFromResult),
        "getWorkflow",
        "Failed getting a workflow instance {}",
        workflowId);
  }

  private Workflow workflowFromResult(ResultSet result) throws SQLException {
    Workflow workflow = null;
    List<Task> tasks = new ArrayList<>();
    while (result.next()) {
      String payload = result.getString(PAYLOAD_COLUMN);
      if (payload != null && !payload.isEmpty()) {
        if (result.getBoolean(WORKFLOW_FLAG_COLUMN)) {
          workflow = fromJson(payload, Workflow.class);
        } else {
          tasks.add(fromJson(payload, Task.class));
        }
      }
    }
    if (workflow != null && !tasks.isEmpty()) {
      tasks.sort(Comparator.comparingInt(Task::getSeq));
      workflow.setTasks(tasks);
    }
    return workflow;
  }

  /**
   * It is used by get all running workflow HTTP or GRPC endpoint. It gets the workflowIds from
   * {@link IndexDAO} using the secondary index with follower read mode.
   */
  @Override
  public List<String> getRunningWorkflowIds(String workflowName, int version) {
    return searchRunningWorkflowIdsByName(workflowName, version);
  }

  private List<String> searchRunningWorkflowIdsByName(String workflowName, int version) {
    return indexDAO
        .searchWorkflows(
            workflowName,
            "RUNNING",
            0,
            maxSearchSize,
            Arrays.asList("DESC", String.valueOf(version)))
        .getResults();
  }

  /**
   * It is used to requeue pending tasks for all the running workflows.
   *
   * <p>First, it gets the workflowIds from {@link IndexDAO} using the secondary index with follower
   * read mode. Second, it gets workflows one by one (there is no transactional guarantee) with
   * follower read mode.
   */
  @Override
  public List<Workflow> getPendingWorkflowsByType(String workflowName, int version) {
    return searchRunningWorkflowIdsByName(workflowName, version).stream()
        .map(
            workflowId ->
                withMetricLogError(
                    () ->
                        withReadOnlyQuery(
                            GET_WORKFLOW_INSTANCE_WITH_TASKS_STATEMENT,
                            statement -> {
                              statement.setString(1, workflowId);
                              statement.setString(2, workflowId);
                            },
                            this::workflowFromResult),
                    "getPendingWorkflowsByType",
                    "Failed getting a workflow instance with id {}",
                    workflowId))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * It is used to get the pending workflow count metric for WorkflowMonitor. It gets the count from
   * {@link IndexDAO} using the secondary index with follower read mode.
   */
  @Override
  public long getPendingWorkflowCount(String workflowName) {
    return searchRunningWorkflowIdsByName(workflowName, -1).size();
  }

  /**
   * It is used to get the pending task count metric for WorkflowMonitor. It gets the count from
   * {@link IndexDAO} using the secondary index with follower read mode.
   */
  @Override
  public long getInProgressTaskCount(String taskDefName) {
    return searchInProgressTaskIdsByName(taskDefName).size();
  }

  /**
   * It is used by get all running workflow HTTP or GRPC endpoint.
   *
   * <p>Assuming that its running workflow instance number is reasonable, startTime filter is
   * applied after getting all running workflow instances for a given workflow name.
   */
  @Override
  public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
    return getPendingWorkflowsByType(workflowName, -1).stream()
        .filter(
            workflow -> workflow.getStartTime() >= startTime && workflow.getStartTime() < endTime)
        .collect(Collectors.toList());
  }

  /**
   * Currently, this feature is not implemented for CockroachDBExecutionDAO. It is used to list
   * workflows for the given correlation id in HTTP or GRPC endpoint. If needed, add correlation id
   * to cockroachdb table schema and implement it.
   */
  @Override
  public List<Workflow> getWorkflowsByCorrelationId(
      String workflowName, String correlationId, boolean includeTasks) {
    throw new UnsupportedOperationException(
        "This method is not implemented in CockroachDBExecutionDAO.");
  }

  /** return true if getWorkflowsByCorrelationId is implemented. */
  @Override
  public boolean canSearchAcrossWorkflows() {
    return false;
  }

  @Override
  public boolean addEventExecution(EventExecution eventExecution) {
    LOG.debug(
        "Creating an event execution for event {} with handler name {} and execution id {}",
        eventExecution.getEvent(),
        eventExecution.getName(),
        eventExecution.getId());
    return addOrUpdateEventExecution(
        CREATE_EVENT_EXECUTION_STATEMENT, eventExecution, "addEventExecution");
  }

  @Override
  public void updateEventExecution(EventExecution eventExecution) {
    LOG.debug(
        "Updating an event execution for event {} with handler name {} and execution id {}",
        eventExecution.getEvent(),
        eventExecution.getName(),
        eventExecution.getId());
    addOrUpdateEventExecution(
        UPSERT_EVENT_EXECUTION_STATEMENT, eventExecution, "updateEventExecution");
  }

  private boolean addOrUpdateEventExecution(
      String stmt, EventExecution eventExecution, String methodName) {
    try {
      int cnt =
          withRetryableUpdate(
              stmt,
              statement -> {
                statement.setString(1, eventExecution.getEvent());
                statement.setString(2, eventExecution.getName());
                statement.setString(3, eventExecution.getMessageId());
                statement.setString(4, eventExecution.getId());
                statement.setString(5, toJson(eventExecution));
              });
      return cnt == 1;
    } catch (Exception e) {
      LOG.warn(
          "Failed {} for event {} with handler name {} due to {}",
          methodName,
          eventExecution.getEvent(),
          eventExecution.getName(),
          e.getMessage());
      // Best effort to store the execution event
      return false;
    }
  }

  @Override
  public void removeEventExecution(EventExecution eventExecution) {
    LOG.debug(
        "Removing an event execution for event {} with handler name {} and execution id {}",
        eventExecution.getEvent(),
        eventExecution.getName(),
        eventExecution.getId());
    withMetricLogError(
        () ->
            withRetryableUpdate(
                REMOVE_EVENT_EXECUTION_STATEMENT,
                statement -> {
                  statement.setString(1, eventExecution.getEvent());
                  statement.setString(2, eventExecution.getName());
                  statement.setString(3, eventExecution.getMessageId());
                  statement.setString(4, eventExecution.getId());
                }),
        "removeEventExecution",
        "Failed removing an event execution for event {} with handler name {}",
        eventExecution.getEvent(),
        eventExecution.getName());
  }
}
