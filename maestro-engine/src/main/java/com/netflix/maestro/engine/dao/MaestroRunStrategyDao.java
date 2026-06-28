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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.TerminateThenRunJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.models.InstanceRunUuid;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ObjectHelper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * Workflow run strategy manager to handle starting workflow instance runs. It supports starting or
 * running instances by any maestro initiators, including manual, signal, time, and subworkflow
 * trigger. Six run strategy rules are implemented. Four of them (sequential, parallel,
 * strict_sequential, serial_latest_only) support queueing. Two of them (first_only and last_only)
 * do not allow queueing and the decision (start/stop) is made when the request is received.
 *
 * <p>If queueing is enabled, at the end, a {@link StartWorkflowJobEvent} event is emitted. If
 * disabling queueing, at the end, emit a {@link TerminateThenRunJobEvent} event if feasible.
 *
 * <p>Note that if users want to switch to FIRST_ONLY or LAST_ONLY, the request will be rejected if
 * there are more than ONE existing non-terminal (queued or running) instances. Maestro will
 * instruct users to stop extra non-terminal instances first. Maestro won't automatically stop those
 * to avoid unexpected behavior. Users have to manually stop them before switching to LAST_ONLY
 * because a new run might unexpectedly stop all previously queued or running instances.
 */
@SuppressWarnings({
  "PMD.ExhaustiveSwitchHasDefault",
  "PMD.ReplaceJavaUtilDate",
  "checkstyle:MultipleStringLiterals"
})
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP")
public class MaestroRunStrategyDao extends AbstractDatabaseDao {
  private static final String ONE_STRING = "1";
  private static final String TWO_STRING = "2";
  private static final int DO_NOTHING_CODE = 0;

  private static final String GET_LATEST_WORKFLOW_INSTANCE_ID_QUERY =
      "SELECT latest_instance_id AS id FROM maestro_workflow WHERE workflow_id=? FOR UPDATE";

  private static final String UPDATE_LATEST_WORKFLOW_INSTANCE_ID_QUERY =
      "UPDATE maestro_workflow SET latest_instance_id = ?, modify_ts = CURRENT_TIMESTAMP WHERE workflow_id=?";

  private static final String GET_LATEST_WORKFLOW_INSTANCE_RUN_ID_QUERY =
      "SELECT run_id AS id, status FROM maestro_workflow_instance "
          + "WHERE workflow_id=? AND instance_id=? ORDER BY run_id DESC LIMIT 1";

  private static final String INSERT_WORKFLOW_INSTANCE_QUERY =
      "INSERT INTO maestro_workflow_instance "
          + "(workflow_id,instance_id,run_id,uuid,correlation_id,initiator,"
          + "root_depth,initiator_type,create_ts,instance,status) "
          + "VALUES (?,?,?,?,?,?::jsonb,?,?,?,?::json,?)";

  private static final String INSERT_STOPPED_WORKFLOW_INSTANCE_QUERY =
      "INSERT INTO maestro_workflow_instance "
          + "(workflow_id,instance_id,run_id,uuid,correlation_id,initiator,"
          + "root_depth,initiator_type,create_ts,instance,status,"
          + "start_ts,end_ts,timeline) "
          + "VALUES (?,?,?,?,?,?::jsonb,?,?,?,?::json,?,?,?,ARRAY[?])";

  private static final String INSERT_TERMINATED_WORKFLOW_INSTANCE_QUERY =
      "INSERT INTO maestro_workflow_instance "
          + "(workflow_id,instance_id,run_id,uuid,correlation_id,initiator,"
          + "root_depth,initiator_type,create_ts,instance,status,"
          + "end_ts,timeline) "
          + "VALUES (?,?,?,?,?,?::jsonb,?,?,?,?::json,?,?,?)";

  // if an instance is restarted, it inherits the original instance id.
  private static final String RUN_STRATEGY_QUERY_TEMPLATE =
      "SELECT instance_id,run_id,uuid FROM maestro_workflow_instance "
          + "WHERE workflow_id=? AND %s ORDER BY instance_id ASC, run_id ASC LIMIT %s";

  private static final String GET_QUEUED_WORKFLOW_INSTANCES_QUERY =
      String.format(
          RUN_STRATEGY_QUERY_TEMPLATE,
          "status='CREATED' AND execution_id IS NULL",
          "(SELECT CASE WHEN COUNT(*) >= ? THEN 0 ELSE LEAST(?, ? - COUNT(*)) END FROM maestro_workflow_instance "
              + "WHERE workflow_id=? AND status IN ('IN_PROGRESS','CREATED') AND execution_id IS NOT NULL)");

  private static final String CHECK_LAST_RUN_FAILED_INSTANCES_QUERY =
      String.format(RUN_STRATEGY_QUERY_TEMPLATE, "status='FAILED'", ONE_STRING);

  private static final String EXIST_NON_TERMINAL_INSTANCE_QUERY =
      String.format(RUN_STRATEGY_QUERY_TEMPLATE, "status IN ('IN_PROGRESS','CREATED')", TWO_STRING);

  private static final String GET_RUNNING_INSTANCES_QUERY =
      String.format(
          RUN_STRATEGY_QUERY_TEMPLATE,
          "status IN ('IN_PROGRESS','CREATED') AND execution_id IS NOT NULL",
          TWO_STRING);

  private static final String FIRST_ONLY_TIMELINE_TEMPLATE =
      "[\"With FIRST_ONLY run strategy, this run is stopped due to a running one [%s].\"]";

  private static final String LAST_ONLY_TIMELINE_TEMPLATE =
      "[\"With LAST_ONLY run strategy, this run is stopped due to starting a new run.\"]";

  private static final String SERIAL_LATEST_ONLY_TIMELINE_TEMPLATE =
      "[\"With SERIAL_LATEST_ONLY run strategy, this run is stopped because new instance %s with run %s arrived.\"]";

  private static final String STOP_SERIAL_LATEST_ONLY_QUEUED_INSTANCE_QUERY =
      "UPDATE maestro_workflow_instance SET status = 'STOPPED', "
          + "end_ts = CURRENT_TIMESTAMP, modify_ts = CURRENT_TIMESTAMP, timeline = array_append(timeline, ?) "
          + "WHERE workflow_id=? AND status='CREATED' AND execution_id IS NULL RETURNING instance";

  private static final String STOP_QUEUED_INSTANCES_QUERY =
      "UPDATE maestro_workflow_instance SET status = 'STOPPED', start_ts = CURRENT_TIMESTAMP, "
          + "end_ts = CURRENT_TIMESTAMP, modify_ts = CURRENT_TIMESTAMP, timeline = array_append(timeline, ?) "
          + "WHERE (workflow_id, instance_id, run_id) IN ("
          + "SELECT workflow_id, instance_id, run_id FROM maestro_workflow_instance "
          + "WHERE workflow_id=? AND status='CREATED' AND execution_id IS NULL LIMIT 2) RETURNING instance";

  private static final String CHECK_EXISTING_UUID_QUERY =
      "SELECT uuid AS id FROM maestro_workflow_instance WHERE workflow_id=? AND uuid=?";

  private static final String CHECK_EXISTING_UUIDS_QUERY =
      "SELECT uuid AS id FROM maestro_workflow_instance WHERE workflow_id=? AND uuid = ANY (?)";

  private static final String UPDATE_WORKFLOW_INSTANCE_FAILED_STATUS =
      "UPDATE maestro_workflow_instance SET status = 'FAILED_2' "
          + "WHERE workflow_id=? AND instance_id=? AND run_id<? AND status='FAILED'";

  private static final Set<RunStrategy.Rule> SERIALIZABLE_RUN_STRATEGIES =
      Set.of(
          RunStrategy.Rule.FIRST_ONLY,
          RunStrategy.Rule.LAST_ONLY,
          RunStrategy.Rule.SERIAL_LATEST_ONLY);

  private static final String RUN_STRATEGY_TAG = "run_strategy";
  private static final User RUN_STRATEGY_USER =
      User.create(Constants.MAESTRO_PREFIX + RUN_STRATEGY_TAG);

  private final MaestroQueueSystem queueSystem;
  private final MaestroMetrics metrics;
  private final ObjectMapper objectMapper;

  /**
   * Carries the start status and transaction-attempt workflow instance out of a retryable
   * transaction.
   *
   * <p>The DAO copies the committed run fields from {@code startedInstance} back to the
   * caller-visible instance only after {@code withRetryableTransaction} succeeds.
   */
  private record StartResult(int startStatus, WorkflowInstance startedInstance) {}

  /**
   * Carries batch start statuses and transaction-attempt workflow instances out of a retryable
   * transaction.
   *
   * <p>Each transaction attempt mutates copied instances and a fresh UUID set. After the
   * transaction commits, the DAO copies the committed run fields from {@code startedInstances} back
   * to the caller-visible batch.
   */
  private record StartBatchResult(int[] startStatuses, List<WorkflowInstance> startedInstances) {}

  /** constructor. */
  public MaestroRunStrategyDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
    this.queueSystem = queueSystem;
    this.metrics = metrics;
    this.objectMapper = objectMapper;
  }

  private WorkflowInstance copyWorkflowInstance(WorkflowInstance instance) {
    // WorkflowInstance is persisted with Jackson; use the same model shape for isolated copies.
    return objectMapper.convertValue(instance, WorkflowInstance.class);
  }

  private List<WorkflowInstance> copyWorkflowInstances(List<WorkflowInstance> instances) {
    return instances.stream().map(this::copyWorkflowInstance).collect(Collectors.toList());
  }

  private void copyInstanceRunFields(WorkflowInstance source, WorkflowInstance target) {
    target.setWorkflowInstanceId(source.getWorkflowInstanceId());
    target.setWorkflowRunId(source.getWorkflowRunId());
    target.setCorrelationId(source.getCorrelationId());
    target.setCreateTime(source.getCreateTime());
  }

  private long getLatestInstanceId(Connection conn, String workflowId) throws SQLException {
    long latestInstanceId = -1;
    try (PreparedStatement stmt = conn.prepareStatement(GET_LATEST_WORKFLOW_INSTANCE_ID_QUERY)) {
      stmt.setString(1, workflowId);
      try (ResultSet result = stmt.executeQuery()) { // unnecessary, to avoid PMD false positive
        if (result.next()) {
          latestInstanceId = result.getLong(ID_COLUMN);
        }
      }
    }
    if (latestInstanceId < 0) {
      throw new MaestroNotFoundException(
          "Cannot find workflow [%s] while trying to start it", workflowId);
    }
    return latestInstanceId;
  }

  private long getLatestRunId(Connection conn, String workflowId, long workflowInstanceId)
      throws SQLException {
    try (PreparedStatement stmt =
        conn.prepareStatement(GET_LATEST_WORKFLOW_INSTANCE_RUN_ID_QUERY)) {
      stmt.setString(1, workflowId);
      stmt.setLong(2, workflowInstanceId);
      try (ResultSet result = stmt.executeQuery()) { // unnecessary, to avoid PMD false positive
        if (result.next()) {
          if (WorkflowInstance.Status.create(result.getString(STATUS_COLUMN)).isTerminal()) {
            return result.getLong(ID_COLUMN);
          } else {
            return -result.getLong(ID_COLUMN); // invalid
          }
        }
        return 0;
      }
    }
  }

  private boolean isDuplicated(Connection conn, WorkflowInstance instance) throws SQLException {
    try (PreparedStatement wfiStmt = conn.prepareStatement(CHECK_EXISTING_UUID_QUERY)) {
      wfiStmt.setString(1, instance.getWorkflowId());
      wfiStmt.setString(2, instance.getWorkflowUuid());
      try (ResultSet result = wfiStmt.executeQuery()) {
        return result.next();
      }
    }
  }

  private void completeInstanceInit(Connection conn, long nextInstanceId, WorkflowInstance instance)
      throws SQLException {
    if (instance.isFreshRun()) { // instance id is always set in fresh run
      instance.setWorkflowInstanceId(nextInstanceId);
      instance.setWorkflowRunId(1L);
    } else {
      // have to ensure only one run is running
      long latestRunId =
          getLatestRunId(conn, instance.getWorkflowId(), instance.getWorkflowInstanceId());
      if (latestRunId < 0) { // it means another run is restarted and cannot run
        throw new MaestroInvalidStatusException(
            "There is already a workflow instance run [%s][%s][%s] and cannot restart another run.",
            instance.getWorkflowId(), instance.getWorkflowInstanceId(), -latestRunId);
      }
      instance.setWorkflowRunId(latestRunId + 1);
      tryUpdateAncestorStatus(conn, instance);
    }
    instance.fillCorrelationIdIfNull();
    instance.setCreateTime(System.currentTimeMillis());
  }

  /**
   * Update a restarted failed run's status to be `FAILED_2`, meaning FAILED but have been
   * restarted.
   */
  private boolean tryUpdateAncestorStatus(Connection conn, WorkflowInstance instance)
      throws SQLException {
    try (PreparedStatement wfiStmt =
        conn.prepareStatement(UPDATE_WORKFLOW_INSTANCE_FAILED_STATUS)) {
      int idx = 0;
      wfiStmt.setString(++idx, instance.getWorkflowId());
      wfiStmt.setLong(++idx, instance.getWorkflowInstanceId());
      wfiStmt.setLong(++idx, instance.getWorkflowRunId());
      return wfiStmt.executeUpdate() == SUCCESS_WRITE_SIZE;
    }
  }

  /** Publish a {@link StartWorkflowJobEvent} job event. */
  private void publishStartWorkflowJobEvent(
      Connection conn, String workflowId, List<MessageDto> messages) throws SQLException {
    StartWorkflowJobEvent jobEvent = StartWorkflowJobEvent.create(workflowId);
    messages.add(queueSystem.enqueue(conn, jobEvent));
  }

  /**
   * If instanceRunUuid is null, no workflow instance to terminate.
   *
   * @param toTerminate the workflow instance to terminate
   * @param instance the workflow instance to run after termination
   * @return TerminateThenRunInstanceJobEvent
   */
  private TerminateThenRunJobEvent createTerminateInstanceJobEvent(
      InstanceRunUuid toTerminate, WorkflowInstance instance) {
    TerminateThenRunJobEvent jobEvent =
        TerminateThenRunJobEvent.init(
            instance.getWorkflowId(),
            Actions.WorkflowInstanceAction.STOP,
            RUN_STRATEGY_USER,
            "Stopped due to LAST_ONLY run strategy to start a new instance: "
                + instance.getIdentity());
    if (toTerminate != null) {
      jobEvent.addOneRun(toTerminate);
    }
    jobEvent.addRunAfter(
        instance.getWorkflowInstanceId(), instance.getWorkflowRunId(), instance.getWorkflowUuid());
    return jobEvent;
  }

  private void prepareCreateInstanceStatement(PreparedStatement wfiStmt, WorkflowInstance instance)
      throws SQLException {
    int idx = 0;
    wfiStmt.setString(++idx, instance.getWorkflowId());
    wfiStmt.setLong(++idx, instance.getWorkflowInstanceId());
    wfiStmt.setLong(++idx, instance.getWorkflowRunId());
    wfiStmt.setString(++idx, instance.getWorkflowUuid());
    wfiStmt.setString(++idx, instance.getCorrelationId());
    wfiStmt.setString(++idx, toJson(instance.getInitiator()));
    wfiStmt.setLong(++idx, instance.getInitiator().getDepth());
    wfiStmt.setString(++idx, instance.getInitiator().getType().name());
    wfiStmt.setTimestamp(++idx, new Timestamp(instance.getCreateTime()));
    wfiStmt.setString(++idx, toJson(instance));
    wfiStmt.setString(++idx, WorkflowInstance.Status.CREATED.name());
  }

  private int insertInstance(
      Connection conn,
      WorkflowInstance instance,
      boolean withQueue,
      InstanceRunUuid toTerminate,
      List<MessageDto> messages)
      throws SQLException {
    try (PreparedStatement wfiStmt = conn.prepareStatement(INSERT_WORKFLOW_INSTANCE_QUERY)) {
      prepareCreateInstanceStatement(wfiStmt, instance);
      int res = wfiStmt.executeUpdate();
      Checks.checkTrue(res == SUCCESS_WRITE_SIZE, "insertInstance expects to always return 1.");
      if (withQueue) {
        publishStartWorkflowJobEvent(conn, instance.getWorkflowId(), messages);
      } else {
        TerminateThenRunJobEvent jobEvent = createTerminateInstanceJobEvent(toTerminate, instance);
        messages.add(queueSystem.enqueue(conn, jobEvent));
      }
      return res;
    }
  }

  private void prepareStopInstanceStatement(
      PreparedStatement wfiStmt, WorkflowInstance instance, TimelineEvent timelineEvent)
      throws SQLException {
    int idx = 0;
    wfiStmt.setString(++idx, instance.getWorkflowId());
    wfiStmt.setLong(++idx, instance.getWorkflowInstanceId());
    wfiStmt.setLong(++idx, instance.getWorkflowRunId());
    wfiStmt.setString(++idx, instance.getWorkflowUuid());
    wfiStmt.setString(++idx, instance.getCorrelationId());
    wfiStmt.setString(++idx, toJson(instance.getInitiator()));
    wfiStmt.setLong(++idx, instance.getInitiator().getDepth());
    wfiStmt.setString(++idx, instance.getInitiator().getType().name());
    wfiStmt.setTimestamp(++idx, new Timestamp(instance.getCreateTime()));
    wfiStmt.setString(++idx, toJson(instance));
    wfiStmt.setString(++idx, WorkflowInstance.Status.STOPPED.name());
    wfiStmt.setTimestamp(++idx, new Timestamp(instance.getCreateTime()));
    wfiStmt.setTimestamp(++idx, new Timestamp(instance.getCreateTime()));
    wfiStmt.setString(++idx, toJson(timelineEvent));
  }

  private void publishInstanceUpdateJobEvent(
      Connection conn,
      WorkflowInstance instance,
      WorkflowInstance.Status status,
      long markTime,
      List<MessageDto> messages)
      throws SQLException {
    WorkflowInstanceUpdateJobEvent jobEvent =
        WorkflowInstanceUpdateJobEvent.create(instance, status, markTime);
    messages.add(queueSystem.enqueue(conn, jobEvent));
  }

  private void publishInstanceStopJobEvent(
      Connection conn, WorkflowInstance instance, long markTime, List<MessageDto> messages)
      throws SQLException {
    publishInstanceUpdateJobEvent(
        conn, instance, WorkflowInstance.Status.STOPPED, markTime, messages);
  }

  private int addStoppedInstance(
      Connection conn,
      WorkflowInstance instance,
      TimelineEvent timelineEvent,
      List<MessageDto> messages)
      throws SQLException {
    try (PreparedStatement wfiStmt =
        conn.prepareStatement(INSERT_STOPPED_WORKFLOW_INSTANCE_QUERY)) {
      prepareStopInstanceStatement(wfiStmt, instance, timelineEvent);
      int res = wfiStmt.executeUpdate();
      Checks.checkTrue(res == SUCCESS_WRITE_SIZE, "addStoppedInstance expects to always return 1.");
      publishInstanceStopJobEvent(conn, instance, instance.getCreateTime(), messages);
      return res;
    }
  }

  private int addTerminatedInstance(
      Connection conn, WorkflowInstance instance, List<MessageDto> messages) throws SQLException {
    try (PreparedStatement wfiStmt =
        conn.prepareStatement(INSERT_TERMINATED_WORKFLOW_INSTANCE_QUERY)) {
      int idx = 0;
      wfiStmt.setString(++idx, instance.getWorkflowId());
      wfiStmt.setLong(++idx, instance.getWorkflowInstanceId());
      wfiStmt.setLong(++idx, instance.getWorkflowRunId());
      wfiStmt.setString(++idx, instance.getWorkflowUuid());
      wfiStmt.setString(++idx, instance.getCorrelationId());
      wfiStmt.setString(++idx, toJson(instance.getInitiator()));
      wfiStmt.setLong(++idx, instance.getInitiator().getDepth());
      wfiStmt.setString(++idx, instance.getInitiator().getType().name());
      wfiStmt.setTimestamp(++idx, new Timestamp(instance.getCreateTime()));
      wfiStmt.setString(++idx, toJson(instance));
      wfiStmt.setString(++idx, instance.getStatus().name());
      wfiStmt.setTimestamp(++idx, new Timestamp(System.currentTimeMillis()));
      wfiStmt.setArray(
          ++idx,
          conn.createArrayOf(
              ARRAY_TYPE_NAME,
              Checks.notNull(
                      instance.getTimeline(),
                      "When addTerminatedInstance, workflow instance timeline cannot be null for %s",
                      instance.getIdentity())
                  .getTimelineEvents()
                  .stream()
                  .map(this::toJson)
                  .toArray(String[]::new)));
      int res = wfiStmt.executeUpdate();
      Checks.checkTrue(
          res == SUCCESS_WRITE_SIZE, "addTerminatedInstance expects to always return 1.");

      publishInstanceUpdateJobEvent(
          conn, instance, instance.getStatus(), instance.getCreateTime(), messages);
      return res;
    }
  }

  private InstanceRunUuid readInstanceRunUuidFromResult(ResultSet result) throws SQLException {
    return new InstanceRunUuid(
        result.getLong("instance_id"), result.getLong("run_id"), result.getString("uuid"));
  }

  private InstanceRunUuid getNonTerminalInstance(Connection conn, String workflowId)
      throws SQLException {
    try (PreparedStatement wfiStmt = conn.prepareStatement(EXIST_NON_TERMINAL_INSTANCE_QUERY)) {
      wfiStmt.setString(1, workflowId);
      try (ResultSet result = wfiStmt.executeQuery()) {
        if (result.next()) {
          InstanceRunUuid instanceRunUuid = readInstanceRunUuidFromResult(result);
          Checks.checkTrue(
              !result.next(),
              "Invalid case: finding more than 1 non-terminal runs beside [%s] with FIRST_ONLY run strategy.",
              instanceRunUuid);
          return instanceRunUuid;
        }
      }
    }
    return null;
  }

  private int startFirstOnlyInstance(
      Connection conn, WorkflowInstance instance, List<MessageDto> messages) throws SQLException {
    InstanceRunUuid runningOne = getNonTerminalInstance(conn, instance.getWorkflowId());
    if (runningOne != null) {
      int ret =
          addStoppedInstance(
              conn,
              instance,
              TimelineLogEvent.info(FIRST_ONLY_TIMELINE_TEMPLATE, runningOne),
              messages);
      LOG.info(
          "With FIRST_ONLY run strategy, add [{}] stopped instance due to a running one [{}]",
          ret,
          runningOne);
      return -ret;
    } else {
      return insertInstance(conn, instance, false, null, messages);
    }
  }

  private int stopLastOnlyQueuedInstance(
      Connection conn, String workflowId, List<MessageDto> messages) throws SQLException {
    try (PreparedStatement wfiStmt = conn.prepareStatement(STOP_QUEUED_INSTANCES_QUERY)) {
      wfiStmt.setString(1, toJson(TimelineLogEvent.info(LAST_ONLY_TIMELINE_TEMPLATE)));
      wfiStmt.setString(2, workflowId);
      try (ResultSet result = wfiStmt.executeQuery()) {
        if (result.next()) {
          WorkflowInstance instance = fromJson(result.getString(1), WorkflowInstance.class);
          publishInstanceStopJobEvent(conn, instance, System.currentTimeMillis(), messages);
          Checks.checkTrue(
              !result.next(),
              "Invalid case: finding more than 1 pending runs beside [%s][%s] with LAST_ONLY run strategy.",
              workflowId,
              instance.getWorkflowInstanceId());
          return 1;
        }
        return 0;
      }
    }
  }

  private InstanceRunUuid getLastOnlyRunningInstance(Connection conn, String workflowId)
      throws SQLException {
    InstanceRunUuid instanceRunUuid = null;
    try (PreparedStatement wfiStmt = conn.prepareStatement(GET_RUNNING_INSTANCES_QUERY)) {
      wfiStmt.setString(1, workflowId);
      try (ResultSet result = wfiStmt.executeQuery()) {
        if (result.next()) {
          instanceRunUuid = readInstanceRunUuidFromResult(result);
          Checks.checkTrue(
              !result.next(),
              "Invalid case: finding more than 1 running instances beside [%s] with LAST_ONLY run strategy.",
              instanceRunUuid);
        }
      }
    }
    return instanceRunUuid;
  }

  private int startLastOnlyInstance(
      Connection conn, WorkflowInstance instance, List<MessageDto> messages) throws SQLException {
    InstanceRunUuid toTerminate =
        stopLastOnlyRunningInstance(conn, instance.getWorkflowId(), messages);
    return insertInstance(conn, instance, false, toTerminate, messages);
  }

  private InstanceRunUuid stopLastOnlyRunningInstance(
      Connection conn, String workflowId, List<MessageDto> messages) throws SQLException {
    int queued = stopLastOnlyQueuedInstance(conn, workflowId, messages);
    InstanceRunUuid instanceRunUuid = getLastOnlyRunningInstance(conn, workflowId);
    int running = instanceRunUuid != null ? 1 : 0;

    if (queued + running > DO_NOTHING_CODE) {
      metrics.counter(
          MetricConstants.RUNSTRATEGY_STOPPED_INSTANCES_METRIC,
          getClass(),
          RUN_STRATEGY_TAG,
          RunStrategy.Rule.LAST_ONLY.name());
      LOG.info(
          "In a transaction, stopped [total {}/queued {}/running {}] workflow instances of [{}]",
          queued + running,
          queued,
          running,
          workflowId);
    }
    return instanceRunUuid;
  }

  private void updateLatestInstanceId(Connection conn, String workflowId, long latestInstanceId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UPDATE_LATEST_WORKFLOW_INSTANCE_ID_QUERY)) {
      stmt.setLong(1, latestInstanceId);
      stmt.setString(2, workflowId);
      Checks.checkTrue(
          stmt.executeUpdate() == SUCCESS_WRITE_SIZE,
          "updateLatestInstanceId expects to always return 1.");
    }
  }

  /**
   * This run strategy logic is called at receiving a {@link RunRequest} request. As FIRST_ONLY and
   * LAST_ONLY do not support queueing, both will directly emit a {@link TerminateThenRunJobEvent}
   * job event.
   *
   * @param instance workflow instance to start
   * @param runStrategy run strategy to check
   * @return start status code
   */
  public int startWithRunStrategy(WorkflowInstance instance, RunStrategy runStrategy) {
    List<MessageDto> messages = new ArrayList<>();
    StartResult result =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      messages.clear(); // clear it to handle the transaction retry
                      WorkflowInstance attemptInstance = copyWorkflowInstance(instance);
                      // Ensures run strategy is not violated
                      if (SERIALIZABLE_RUN_STRATEGIES.contains(runStrategy.getRule())) {
                        markTransactionSerializable(conn);
                      }
                      final long nextInstanceId =
                          getLatestInstanceId(conn, attemptInstance.getWorkflowId()) + 1;
                      if (isDuplicated(conn, attemptInstance)) {
                        return new StartResult(0, attemptInstance);
                      }
                      completeInstanceInit(conn, nextInstanceId, attemptInstance);
                      int res;
                      if (attemptInstance.getStatus().isTerminal()) {
                        // Save it directly and send a terminate event
                        res = addTerminatedInstance(conn, attemptInstance, messages);
                      } else {
                        switch (runStrategy.getRule()) {
                          case SEQUENTIAL:
                          case PARALLEL:
                          case STRICT_SEQUENTIAL:
                            res = insertInstance(conn, attemptInstance, true, null, messages);
                            break;
                          case FIRST_ONLY:
                            res = startFirstOnlyInstance(conn, attemptInstance, messages);
                            break;
                          case LAST_ONLY:
                            res = startLastOnlyInstance(conn, attemptInstance, messages);
                            break;
                          case SERIAL_LATEST_ONLY:
                            res = startSerialLatestOnlyInstance(conn, attemptInstance, messages);
                            break;
                          default:
                            throw new MaestroInternalError(
                                "When start, run strategy [%s] is not supported.", runStrategy);
                        }
                      }
                      if (attemptInstance.getWorkflowInstanceId() == nextInstanceId) {
                        updateLatestInstanceId(
                            conn, attemptInstance.getWorkflowId(), nextInstanceId);
                      }
                      return new StartResult(res, attemptInstance);
                    }),
            "startWithRunStrategy",
            "Failed to start a workflow [{}][{}] with run strategy [{}]",
            instance.getWorkflowId(),
            instance.getWorkflowUuid(),
            runStrategy);
    copyInstanceRunFields(result.startedInstance(), instance);
    messages.forEach(queueSystem::notify);
    return result.startStatus();
  }

  /**
   * Dequeue workflow instances considering the current run strategy. It won't update the instance
   * state but will always deterministically send out the run workflow job event. Downstream will
   * handle receiving duplicate job events.
   *
   * <p>It will be called when a {@link StartWorkflowJobEvent} job event is received. For run
   * strategy with disabled queue function (e.g. first_only and last_only), it does nothing as the
   * workflow run is handled when {@link RunRequest} is received.
   *
   * @param workflowId workflow id
   * @param runStrategy run strategy to check
   * @return the dequeued instance info.
   */
  public List<InstanceRunUuid> dequeueWithRunStrategy(String workflowId, RunStrategy runStrategy) {
    return withMetricLogError(
        () -> {
          switch (runStrategy.getRule()) {
            case SEQUENTIAL:
            case PARALLEL:
            case STRICT_SEQUENTIAL:
            case SERIAL_LATEST_ONLY:
              return dequeueWorkflowInstances(
                  workflowId,
                  runStrategy.getWorkflowConcurrency(),
                  runStrategy.getRule() == RunStrategy.Rule.STRICT_SEQUENTIAL);
            case FIRST_ONLY:
            case LAST_ONLY:
              return null; // no queueing support
            default:
              throw new MaestroInternalError(
                  "When dequeue, run strategy [%s] hasn't been implemented yet", runStrategy);
          }
        },
        "runWithStrategy",
        "Failed to run workflow [{}] with run strategy [{}]",
        workflowId,
        runStrategy);
  }

  private List<InstanceRunUuid> dequeueWorkflowInstances(
      String workflowId, long concurrency, boolean strict) {
    return withRetryableTransaction(
        conn -> {
          markTransactionSerializable(conn);
          if (strict && existLastRunFailedInstance(conn, workflowId)) {
            LOG.info(
                "Cannot run instance for workflow [{}] as it has failed instance last runs in history.",
                workflowId);
            return null;
          } else {
            return getRunWorkflowInstances(conn, workflowId, concurrency);
          }
        });
  }

  private boolean existLastRunFailedInstance(Connection conn, String workflowId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(CHECK_LAST_RUN_FAILED_INSTANCES_QUERY)) {
      stmt.setString(1, workflowId);
      try (ResultSet result = stmt.executeQuery()) {
        return result.next();
      }
    }
  }

  private List<InstanceRunUuid> getRunWorkflowInstances(
      Connection conn, String workflowId, long concurrency) throws SQLException {
    List<InstanceRunUuid> runInstances = new ArrayList<>();
    try (PreparedStatement stmt = conn.prepareStatement(GET_QUEUED_WORKFLOW_INSTANCES_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setLong(++idx, concurrency);
      stmt.setLong(++idx, Constants.DEQUEUE_SIZE_LIMIT);
      stmt.setLong(++idx, concurrency);
      stmt.setString(++idx, workflowId);
      try (ResultSet result = stmt.executeQuery()) {
        while (result.next()) {
          runInstances.add(readInstanceRunUuidFromResult(result));
        }
      }
      return runInstances;
    }
  }

  /**
   * Add a list of new workflow instance runs (i.e. run_id=1). The instance list has already been
   * sized to fit into the batch size limit. It will skip instances with duplicated uuids.
   *
   * @param workflowId workflow id
   * @param runStrategy run strategy to check
   * @param instances the list of workflow instances to create
   * @return the status of start workflow instances. Instances have also been updated.
   */
  public int[] startBatchWithRunStrategy(
      String workflowId, RunStrategy runStrategy, List<WorkflowInstance> instances) {
    if (instances == null || instances.isEmpty()) {
      return new int[0];
    }
    List<MessageDto> messages = new ArrayList<>();
    StartBatchResult result =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      messages.clear(); // clear it to handle the transaction retry
                      List<WorkflowInstance> attemptInstances = copyWorkflowInstances(instances);
                      Set<String> uuids =
                          attemptInstances.stream()
                              .map(WorkflowInstance::getWorkflowUuid)
                              .collect(Collectors.toSet());
                      if (SERIALIZABLE_RUN_STRATEGIES.contains(runStrategy.getRule())) {
                        markTransactionSerializable(conn);
                      }
                      final long nextInstanceId = getLatestInstanceId(conn, workflowId) + 1;
                      if (dedupAndCheckIfAllDuplicated(conn, workflowId, uuids)) {
                        return new StartBatchResult(
                            new int[attemptInstances.size()], attemptInstances);
                      }
                      long lastAssignedInstanceId =
                          completeInstancesInit(conn, nextInstanceId, uuids, attemptInstances);
                      int[] res;
                      switch (runStrategy.getRule()) {
                        case SEQUENTIAL:
                        case PARALLEL:
                        case STRICT_SEQUENTIAL:
                          res = enqueueInstances(conn, workflowId, attemptInstances, messages);
                          break;
                        case FIRST_ONLY:
                          res =
                              startFirstOnlyInstances(conn, workflowId, attemptInstances, messages);
                          break;
                        case LAST_ONLY:
                          res =
                              startLastOnlyInstances(conn, workflowId, attemptInstances, messages);
                          break;
                        case SERIAL_LATEST_ONLY:
                          res =
                              startSerialLatestOnlyInstances(
                                  conn, workflowId, attemptInstances, messages);
                          break;
                        default:
                          throw new MaestroInternalError(
                              "When startBatch, run strategy [%s] is not supported.", runStrategy);
                      }
                      if (lastAssignedInstanceId >= nextInstanceId) {
                        updateLatestInstanceId(conn, workflowId, lastAssignedInstanceId);
                      }
                      return new StartBatchResult(res, attemptInstances);
                    }),
            "startBatchWithRunStrategy",
            "Failed to start [{}] workflow instances for [{}] with run strategy [{}]",
            instances.size(),
            workflowId,
            runStrategy);
    for (int i = 0; i < instances.size(); ++i) {
      copyInstanceRunFields(result.startedInstances().get(i), instances.get(i));
    }
    messages.forEach(queueSystem::notify);
    return result.startStatuses();
  }

  /**
   * Removes UUIDs that already exist for the workflow from the mutable {@code uuids} set.
   *
   * @return {@code true} when every candidate UUID was already present and the batch should be
   *     treated as duplicated without creating any instance rows.
   */
  private boolean dedupAndCheckIfAllDuplicated(
      Connection conn, String workflowId, Set<String> uuids) throws SQLException {
    try (PreparedStatement wfiStmt = conn.prepareStatement(CHECK_EXISTING_UUIDS_QUERY)) {
      wfiStmt.setString(1, workflowId);
      wfiStmt.setArray(2, conn.createArrayOf(ARRAY_TYPE_NAME, uuids.toArray(new String[0])));
      try (ResultSet result = wfiStmt.executeQuery()) {
        while (result.next()) {
          uuids.remove(result.getString(ID_COLUMN));
        }
      }
    }
    return uuids.isEmpty();
  }

  /**
   * Assigns instance IDs, run IDs, correlation IDs, and create times to distinct instances that
   * remain in the mutable {@code uuids} set.
   *
   * <p>Duplicate UUIDs inside the same batch are skipped after the first assignment. UUIDs removed
   * by {@link #dedupAndCheckIfAllDuplicated(Connection, String, Set)} are skipped because they
   * already exist in the database.
   *
   * @return the last instance ID assigned, or {@code startingInstanceId - 1} when no instances were
   *     assigned.
   */
  private long completeInstancesInit(
      Connection conn, long startingInstanceId, Set<String> uuids, List<WorkflowInstance> instances)
      throws SQLException {
    long nextInstanceId = startingInstanceId;
    for (WorkflowInstance instance : instances) {
      if (uuids.contains(instance.getWorkflowUuid())) {
        completeInstanceInit(conn, nextInstanceId, instance);
        nextInstanceId++;
        uuids.remove(instance.getWorkflowUuid());
      }
    }
    return nextInstanceId - 1;
  }

  private int[] enqueueInstances(
      Connection conn,
      String workflowId,
      List<WorkflowInstance> instances,
      List<MessageDto> messages)
      throws SQLException {
    int[] ret = new int[instances.size()];
    int idx = 0;
    try (PreparedStatement wfiStmt = conn.prepareStatement(INSERT_WORKFLOW_INSTANCE_QUERY)) {
      for (WorkflowInstance instance : instances) {
        if (instance.getWorkflowInstanceId() != DO_NOTHING_CODE) {
          prepareCreateInstanceStatement(wfiStmt, instance);
          wfiStmt.addBatch();
          ret[idx] = SUCCESS_WRITE_SIZE;
        }
        ++idx;
      }
      int[] res = wfiStmt.executeBatch();
      if (res.length > DO_NOTHING_CODE) {
        Checks.checkTrue(
            Arrays.stream(res).allMatch(i -> i == SUCCESS_WRITE_SIZE),
            "executeBatch in enqueueInstances should return all 1s.");
        publishStartWorkflowJobEvent(conn, workflowId, messages);
      }
    }
    return ret;
  }

  private int[] startFirstOnlyInstances(
      Connection conn,
      String workflowId,
      List<WorkflowInstance> instances,
      List<MessageDto> messages)
      throws SQLException {
    InstanceRunUuid runningOne = getNonTerminalInstance(conn, workflowId);
    return startFirstOrLastOnlyInstances(conn, runningOne, instances, null, messages);
  }

  private int[] startLastOnlyInstances(
      Connection conn,
      String workflowId,
      List<WorkflowInstance> instances,
      List<MessageDto> messages)
      throws SQLException {
    InstanceRunUuid toTerminate = stopLastOnlyRunningInstance(conn, workflowId, messages);

    Collections.reverse(instances);
    int[] ret = startFirstOrLastOnlyInstances(conn, null, instances, toTerminate, messages);
    int tmp;
    for (int i = 0, j = ret.length - 1; i < j; ++i, --j) {
      tmp = ret[i];
      ret[i] = ret[j];
      ret[j] = tmp;
    }
    Collections.reverse(instances);
    return ret;
  }

  private int startSerialLatestOnlyInstance(
      Connection conn, WorkflowInstance instance, List<MessageDto> messages) throws SQLException {
    stopSerialLatestOnlyQueuedInstance(
        conn,
        instance.getWorkflowId(),
        instance.getWorkflowInstanceId(),
        instance.getWorkflowRunId(),
        messages);
    return insertInstance(conn, instance, true, null, messages);
  }

  /**
   * Starts a batch of workflow instances under the SERIAL_LATEST_ONLY run strategy.
   *
   * <p>The batch is reversed so the last arrival (highest instance id) is processed first and
   * becomes the sole queued candidate. All existing queued rows for the workflow and all earlier
   * rows in the batch are stopped in the same transaction. A single {@link StartWorkflowJobEvent}
   * is emitted for the new arrival. The instances list and the return array are restored to their
   * original order before returning.
   */
  private int[] startSerialLatestOnlyInstances(
      Connection conn,
      String workflowId,
      List<WorkflowInstance> instances,
      List<MessageDto> messages)
      throws SQLException {
    Collections.reverse(instances);
    int[] ret = new int[instances.size()];
    List<WorkflowInstance> stoppedInstances = new ArrayList<>();
    TimelineEvent timelineEvent = null;
    boolean firstInserted = false;
    int idx = 0;

    try (PreparedStatement insertStmt = conn.prepareStatement(INSERT_WORKFLOW_INSTANCE_QUERY);
        PreparedStatement stopStmt =
            conn.prepareStatement(INSERT_STOPPED_WORKFLOW_INSTANCE_QUERY)) {
      for (WorkflowInstance inst : instances) {
        if (inst.getWorkflowInstanceId() != DO_NOTHING_CODE) {
          if (!firstInserted) {
            stopSerialLatestOnlyQueuedInstance(
                conn, workflowId, inst.getWorkflowInstanceId(), inst.getWorkflowRunId(), messages);
            timelineEvent =
                TimelineLogEvent.info(
                    SERIAL_LATEST_ONLY_TIMELINE_TEMPLATE,
                    inst.getWorkflowInstanceId(),
                    inst.getWorkflowRunId());
            prepareCreateInstanceStatement(insertStmt, inst);
            insertStmt.addBatch();
            ret[idx] = SUCCESS_WRITE_SIZE;
            firstInserted = true;
          } else {
            prepareStopInstanceStatement(stopStmt, inst, timelineEvent);
            stopStmt.addBatch();
            ret[idx] = -SUCCESS_WRITE_SIZE;
            stoppedInstances.add(inst);
          }
        }
        ++idx;
      }

      if (firstInserted) {
        int[] insertRes = insertStmt.executeBatch();
        Checks.checkTrue(
            Arrays.stream(insertRes).allMatch(i -> i == SUCCESS_WRITE_SIZE),
            "executeBatch in startSerialLatestOnlyInstances insert should return all 1s.");
      }
      if (!stoppedInstances.isEmpty()) {
        int[] stopRes = stopStmt.executeBatch();
        Checks.checkTrue(
            Arrays.stream(stopRes).allMatch(i -> i == SUCCESS_WRITE_SIZE),
            "executeBatch in startSerialLatestOnlyInstances stop should return all 1s.");
        messages.add(
            queueSystem.enqueue(
                conn,
                WorkflowInstanceUpdateJobEvent.create(
                    stoppedInstances,
                    WorkflowInstance.Status.STOPPED,
                    System.currentTimeMillis())));
      }
    }

    if (firstInserted) {
      publishStartWorkflowJobEvent(conn, workflowId, messages);
    }

    Collections.reverse(instances);
    int tmp;
    for (int i = 0, j = ret.length - 1; i < j; ++i, --j) {
      tmp = ret[i];
      ret[i] = ret[j];
      ret[j] = tmp;
    }
    return ret;
  }

  private void stopSerialLatestOnlyQueuedInstance(
      Connection conn,
      String workflowId,
      long newInstanceId,
      long newRunId,
      List<MessageDto> messages)
      throws SQLException {
    try (PreparedStatement wfiStmt =
        conn.prepareStatement(STOP_SERIAL_LATEST_ONLY_QUEUED_INSTANCE_QUERY)) {
      wfiStmt.setString(
          1,
          toJson(
              TimelineLogEvent.info(
                  SERIAL_LATEST_ONLY_TIMELINE_TEMPLATE, newInstanceId, newRunId)));
      wfiStmt.setString(2, workflowId);
      List<WorkflowInstance> stopped = new ArrayList<>();
      try (ResultSet result = wfiStmt.executeQuery()) {
        while (result.next()) {
          stopped.add(fromJson(result.getString(1), WorkflowInstance.class));
        }
      }
      if (!stopped.isEmpty()) {
        messages.add(
            queueSystem.enqueue(
                conn,
                WorkflowInstanceUpdateJobEvent.create(
                    stopped, WorkflowInstance.Status.STOPPED, System.currentTimeMillis())));
        LOG.info(
            "With SERIAL_LATEST_ONLY run strategy, stopped [{}] queued instance(s) for workflow [{}]",
            stopped.size(),
            workflowId);
      }
    }
  }

  private int[] startFirstOrLastOnlyInstances(
      Connection conn,
      InstanceRunUuid instanceRunUuid,
      List<WorkflowInstance> instances,
      InstanceRunUuid toTerminate,
      List<MessageDto> messages)
      throws SQLException {
    InstanceRunUuid runningOne = instanceRunUuid;
    int[] ret = new int[instances.size()];
    int idx = 0;
    Iterator<WorkflowInstance> instanceIterator = instances.iterator();
    TerminateThenRunJobEvent jobEvent = null;

    while (runningOne == null && instanceIterator.hasNext()) {
      WorkflowInstance instance = instanceIterator.next();
      if (instance.getWorkflowInstanceId() != DO_NOTHING_CODE) {
        try (PreparedStatement wfiStmt = conn.prepareStatement(INSERT_WORKFLOW_INSTANCE_QUERY)) {
          prepareCreateInstanceStatement(wfiStmt, instance);
          ret[idx] = wfiStmt.executeUpdate();
        }
        Checks.checkTrue(
            ret[idx] == SUCCESS_WRITE_SIZE,
            "startFirstOrLastOnlyInstances failed due to invalid insert state %s!=1",
            ret[idx]);
        jobEvent = createTerminateInstanceJobEvent(toTerminate, instance);
        if (!ObjectHelper.isCollectionEmptyOrNull(jobEvent.getRunAfter())) {
          runningOne = jobEvent.getRunAfter().getFirst();
        }
      }
      ++idx;
    }

    if (runningOne == null) {
      return ret;
    }

    // Now insert the remaining distinct workflow instances as STOPPED with timeline info.
    List<WorkflowInstance> instanceStopped = new ArrayList<>();
    int stopped = 0;
    if (instanceIterator.hasNext()) {
      TimelineEvent timelineEvent = TimelineLogEvent.info(FIRST_ONLY_TIMELINE_TEMPLATE, runningOne);
      try (PreparedStatement wfiStmt =
          conn.prepareStatement(INSERT_STOPPED_WORKFLOW_INSTANCE_QUERY)) {
        while (instanceIterator.hasNext()) {
          WorkflowInstance instance = instanceIterator.next();
          if (instance.getWorkflowInstanceId() != DO_NOTHING_CODE) {
            prepareStopInstanceStatement(wfiStmt, instance, timelineEvent);
            wfiStmt.addBatch();
            ret[idx] = -SUCCESS_WRITE_SIZE;
            stopped++;
            instanceStopped.add(instance);
          }
          ++idx;
        }
        int[] res = wfiStmt.executeBatch();
        Checks.checkTrue(
            Arrays.stream(res).allMatch(i -> i == SUCCESS_WRITE_SIZE),
            "executeBatch in startFirstOnlyInstances should return all 1s.");
      }
    }

    if (!instanceStopped.isEmpty()) {
      messages.add(
          queueSystem.enqueue(
              conn,
              WorkflowInstanceUpdateJobEvent.create(
                  instanceStopped, WorkflowInstance.Status.STOPPED, System.currentTimeMillis())));
    }

    if (jobEvent != null) {
      messages.add(queueSystem.enqueue(conn, jobEvent));
    }

    LOG.info(
        "With FIRST_ONLY or LAST_ONLY run strategy, [started {}/stopped {}/skipped {}/total {}] "
            + "instances due to a running one [{}]",
        jobEvent != null ? 1 : 0,
        stopped,
        ret.length - stopped - (jobEvent != null ? 1 : 0),
        ret.length,
        runningOne);

    return ret;
  }
}
