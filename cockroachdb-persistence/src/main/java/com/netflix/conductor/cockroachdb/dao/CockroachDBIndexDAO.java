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
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CockroachDB implementation of Index DAO to index the workflow and task details for searching.
 *
 * <p>It directly search data in CockroachDB workflow and task tables using the secondary index. It
 * uses CockroachDB follower-read mechanism to reduce contention because the search results can be
 * slightly less than current.
 *
 * @author jun-he
 */
public class CockroachDBIndexDAO extends CockroachDBBaseDAO implements IndexDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBIndexDAO.class);

  private static final String CREATED_TIME_COLUMN = "created_time";
  private static final String LOG_COLUMN = "log";

  private static final String GET_WORKFLOW_INSTANCE_IDS_STATEMENT_TEMPLATE =
      "SELECT workflow_instance_id AS id, workflow_version AS version FROM workflow_instance "
          + "WHERE workflow_name = ? AND status = ? ORDER BY start_time %s LIMIT %s OFFSET %s"; // offset is not good

  private static final String GET_TASK_IDS_STATEMENT =
      "SELECT task_id AS id FROM task "
          + "WHERE task_name = ? AND status = ? ORDER BY start_time %s LIMIT %s OFFSET %s";

  private static final String CREATE_TASK_EXECUTION_LOGS_STATEMENT =
      "INSERT INTO task_execution_log (task_id,created_time,log) VALUES (?,?,?)";
  private static final String GET_TASK_EXECUTION_LOGS_STATEMENT =
      "SELECT task_id AS id, created_time, log FROM task_execution_log "
          + "WHERE task_id = ? ORDER BY created_time";

  private static final String GET_EVENT_EXECUTIONS_STATEMENT =
      "SELECT payload FROM event_execution WHERE event = ?";

  public CockroachDBIndexDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  @Override
  public void setup() {
    // no-op
  }

  @Override
  public void indexWorkflow(Workflow workflow) {
    // no-op
  }

  @Override
  public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
    // no-op
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void indexTask(Task task) {
    // no-op
  }

  @Override
  public CompletableFuture<Void> asyncIndexTask(Task task) {
    // no-op
    return CompletableFuture.completedFuture(null);
  }

  /**
   * It searches workflow instance ids in CockroachDB workflow_instance table using the secondary
   * index. Note that it does not support free text search and re-define the meaning of the
   * parameters.
   *
   * @param workflowName workflow name / workflow type
   * @param status workflow status
   * @param start select offset
   * @param count selection limit
   * @param options other search options, [0] is startTime sorting order, [1] is version, null means
   *     using default
   * @return a list of workflow instance ids wrapped in SearchResult
   */
  @Override
  public SearchResult<String> searchWorkflows(
      String workflowName, String status, int start, int count, List<String> options) {
    return withMetricLogError(
        () ->
            getSearchIds(
                GET_WORKFLOW_INSTANCE_IDS_STATEMENT_TEMPLATE,
                workflowName,
                status,
                start,
                count,
                options),
        "searchWorkflows",
        "Failed searching workflows by workflow name {} with status {}",
        workflowName,
        status);
  }

  private SearchResult<String> getSearchIds(
      String stmtTemplate, String name, String status, int start, int count, List<String> options) {
    final String sorted = options == null || options.isEmpty() ? "DESC" : options.get(0);
    final String getIdsStatement = String.format(stmtTemplate, sorted, count, start);
    final int version =
        options == null || options.size() < 2 ? -1 : Integer.parseInt(options.get(1));
    List<String> ids =
        withReadOnlyQuery(
            getIdsStatement,
            statement -> {
              statement.setString(1, name);
              statement.setString(2, status);
            },
            rs -> idsFromResult(rs, version));
    return new SearchResult<>(ids.size(), ids);
  }

  /**
   * It searches task ids in CockroachDB task table using the secondary index. Note that it does not
   * support free text search and re-define the meaning of the parameters.
   *
   * @param taskName task definition name
   * @param status task status
   * @param start select offset
   * @param count selection limit
   * @param options other search options, [0] is startTime sorting order, null means using default
   * @return a list of task ids wrapped in SearchResult
   */
  @Override
  public SearchResult<String> searchTasks(
      String taskName, String status, int start, int count, List<String> options) {
    return withMetricLogError(
        () -> getSearchIds(GET_TASK_IDS_STATEMENT, taskName, status, start, count, options),
        "searchTasks",
        "Failed searching tasks by task name {} with status {}",
        taskName,
        status);
  }

  @Override
  public void removeWorkflow(String workflowId) {
    // no-op
  }

  @Override
  public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
    // no-op
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
    // no-op
  }

  @Override
  public CompletableFuture<Void> asyncUpdateWorkflow(
      String workflowInstanceId, String[] keys, Object[] values) {
    // no-op
    return CompletableFuture.completedFuture(null);
  }

  /** Always return null */
  @Override
  public String get(String workflowInstanceId, String key) {
    return null;
  }

  /** Assume the number of logs is reasonable and fit into 1 batch */
  @Override
  public void addTaskExecutionLogs(List<TaskExecLog> logs) {
    if (logs == null || logs.isEmpty()) {
      return;
    }

    Set<TaskExecLog> taskLogs = new HashSet<>(logs);
    try {
      int cnt =
          withRetryableStatement(
              CREATE_TASK_EXECUTION_LOGS_STATEMENT,
              statement -> {
                for (TaskExecLog taskLog : taskLogs) {
                  statement.setString(1, taskLog.getTaskId());
                  statement.setLong(2, taskLog.getCreatedTime());
                  statement.setString(3, taskLog.getLog());
                  statement.addBatch();
                }
                int[] res = statement.executeBatch();
                return res.length;
              });
      LOG.debug(
          "Created {}/{} task execution logs for a task with id {}",
          cnt,
          logs.size(),
          logs.get(0).getTaskId());
    } catch (Exception e) {
      LOG.warn(
          "Fail creating {} task execution logs for a task with id {} due to {}",
          logs.size(),
          logs.get(0).getTaskId(),
          e.getMessage());
      // ignore error as the execution log is not in the critical path.
    }
  }

  @Override
  public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
    return CompletableFuture.runAsync(() -> addTaskExecutionLogs(logs));
  }

  @Override
  public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
    try {
      return withReadOnlyQuery(
          GET_TASK_EXECUTION_LOGS_STATEMENT,
          statement -> statement.setString(1, taskId),
          this::taskExecLogsFromResult);
    } catch (Exception e) {
      LOG.warn(
          "Cannot get task execution logs for task {} due to message = [{}]",
          taskId,
          e.getMessage());
      // ignore error as the execution log is not in the critical path.
      return Collections.emptyList();
    }
  }

  private List<TaskExecLog> taskExecLogsFromResult(ResultSet result) throws SQLException {
    List<TaskExecLog> logs = new ArrayList<>();
    while (result.next()) {
      String id = result.getString(ID_COLUMN);
      long createdTime = result.getLong(CREATED_TIME_COLUMN);
      String log = result.getString(LOG_COLUMN);
      TaskExecLog taskLog = new TaskExecLog();
      taskLog.setTaskId(id);
      taskLog.setCreatedTime(createdTime);
      taskLog.setLog(log);
      logs.add(taskLog);
    }
    return logs;
  }

  @Override
  public void addEventExecution(EventExecution eventExecution) {
    // no-op
  }

  @Override
  public List<EventExecution> getEventExecutions(String event) {
    return withMetricLogError(
        () ->
            getPayloads(
                GET_EVENT_EXECUTIONS_STATEMENT,
                statement -> statement.setString(1, event),
                EventExecution.class),
        "getEventExecutions",
        "Failed getting event executions with event {}",
        event);
  }

  @Override
  public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
    // no-op
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Message has never been used or retrieved and just for debugging purpose So just output the msg
   * to the log. Persist it if needed.
   */
  @Override
  public void addMessage(String queue, Message msg) {
    LOG.info("For queue {}, add a message: {}", queue, toJson(msg));
  }

  @Override
  public CompletableFuture<Void> asyncAddMessage(String queue, Message message) {
    addMessage(queue, message);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Currently, this feature is not implemented for CockroachDBIndexDAO. It is used for debugging
   * purpose only.
   */
  @Override
  public List<Message> getMessages(String queue) {
    throw new UnsupportedOperationException(
        "This method is not implemented in CockroachDBIndexDAO.");
  }

  /**
   * Currently, this feature is not implemented for CockroachDBIndexDAO. It is used for debugging
   * purpose or a workflow archival service.
   */
  @Override
  public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
    throw new UnsupportedOperationException(
        "This method is not implemented in CockroachDBIndexDAO.");
  }

  /**
   * Currently, this feature is not implemented for CockroachDBIndexDAO. It is used for debugging
   * purpose or a stale workflow completion service.
   */
  @Override
  public List<String> searchRecentRunningWorkflows(
      int lastModifiedHoursAgoFrom, int lastModifiedHoursAgoTo) {
    throw new UnsupportedOperationException(
        "This method is not implemented in CockroachDBIndexDAO.");
  }
}
