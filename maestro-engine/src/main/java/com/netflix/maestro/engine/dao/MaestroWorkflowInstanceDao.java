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
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.engine.db.ForeachIterationOverview;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.utils.AggregatedViewHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.TerminateThenRunJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.models.InstanceRunUuid;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ObjectHelper;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * DAO for saving and retrieving Maestro workflow instance data model.
 *
 * <p>In the data model, we use `null` to indicate `unset`.
 */
// mute the false positive error due to https://github.com/spotbugs/spotbugs/issues/293
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
@Slf4j
public class MaestroWorkflowInstanceDao extends AbstractDatabaseDao {
  private static final String SINGLE_PLACE_HOLDER = "?,";
  private static final String DOUBLE_PLACE_HOLDER = "?::json,?,";
  private static final String QUAD_PLACE_HOLDER = "?,?,?::json,?,";
  private static final String VALUE_PLACE_HOLDER = "(?::json,?)";

  private static final String CREATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE =
      "INSERT INTO maestro_workflow_instance (instance,status) VALUES %s "
          + "ON CONFLICT (workflow_id,instance_id,run_id) DO NOTHING RETURNING instance_id";

  private static final String TERMINATE_QUEUED_WORKFLOW_PREFIX =
      "UPDATE maestro_workflow_instance SET (status,end_ts,modify_ts,timeline) "
          + "= (?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,array_append(timeline,?)) WHERE workflow_id=? ";

  private static final String TERMINATE_QUEUED_INSTANCE_QUERY =
      TERMINATE_QUEUED_WORKFLOW_PREFIX
          + "AND instance_id=? AND run_id=? AND status='CREATED' AND execution_id IS NULL";

  private static final String INSTANCE_IN_SUBQUERY =
      "(instance_id, run_id) IN (SELECT instance_id, run_id FROM maestro_workflow_instance WHERE workflow_id=? ";

  private static final String TERMINATE_QUEUED_INSTANCES_QUERY =
      TERMINATE_QUEUED_WORKFLOW_PREFIX
          + "AND status='CREATED' AND execution_id IS NULL AND "
          + INSTANCE_IN_SUBQUERY
          + "AND status='CREATED' AND execution_id IS NULL LIMIT ?) RETURNING instance";

  private static final String GET_RUNNING_INSTANCES_QUERY_PREFIX =
      "SELECT instance_id,run_id,uuid FROM maestro_workflow_instance ";

  private static final String GET_RUNNING_CREATED_INSTANCES_QUERY =
      GET_RUNNING_INSTANCES_QUERY_PREFIX
          + "WHERE workflow_id=? AND status='CREATED' AND instance_id>? AND execution_id IS NOT NULL "
          + "ORDER BY instance_id ASC LIMIT ?";

  private static final String GET_RUNNING_IN_PROGRESS_INSTANCES_QUERY =
      GET_RUNNING_INSTANCES_QUERY_PREFIX
          + "WHERE workflow_id=? AND status='IN_PROGRESS' AND instance_id>? ORDER BY instance_id ASC LIMIT ?";

  private static final String UPDATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE =
      "UPDATE maestro_workflow_instance SET (%s modify_ts) = (%s CURRENT_TIMESTAMP) "
          + "WHERE workflow_id=? AND instance_id=? AND run_id=? ";

  private static final String UPDATE_WORKFLOW_INSTANCE_START_QUERY =
      String.format(
          UPDATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE,
          "status,start_ts,runtime_overview,timeline,",
          QUAD_PLACE_HOLDER);

  private static final String UPDATE_WORKFLOW_INSTANCE_END_QUERY =
      String.format(
          UPDATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE,
          "status,end_ts,runtime_overview,timeline,",
          QUAD_PLACE_HOLDER);

  private static final String UPDATE_WORKFLOW_INSTANCE_TIMELINE_QUERY =
      String.format(
          UPDATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE,
          "runtime_overview,timeline,",
          DOUBLE_PLACE_HOLDER);

  private static final String UPDATE_WORKFLOW_INSTANCE_EXECUTION_QUERY =
      String.format(UPDATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE, "execution_id,", SINGLE_PLACE_HOLDER)
          + " AND status='CREATED' AND execution_id IS NULL";

  private static final String GET_WORKFLOW_INSTANCE_FIELDS_TEMPLATE =
      "SELECT %s FROM maestro_workflow_instance WHERE workflow_id=? AND instance_id=? %s";

  private static final String ALL_FIELDS =
      "instance,status,execution_id,start_ts,end_ts,modify_ts,runtime_overview,timeline ";

  private static final String RUN_ID_CONDITION = "AND run_id=?";
  private static final String LATEST_RUN_CONDITION = "ORDER BY run_id DESC LIMIT 1";

  private static final String GET_WORKFLOW_INSTANCE_QUERY =
      String.format(GET_WORKFLOW_INSTANCE_FIELDS_TEMPLATE, ALL_FIELDS, RUN_ID_CONDITION);

  private static final String GET_LATEST_WORKFLOW_INSTANCE_RUN_QUERY =
      String.format(GET_WORKFLOW_INSTANCE_FIELDS_TEMPLATE, ALL_FIELDS, LATEST_RUN_CONDITION);

  private static final String GET_WORKFLOW_INSTANCE_STATUS_QUERY =
      String.format(GET_WORKFLOW_INSTANCE_FIELDS_TEMPLATE, STATUS_COLUMN, RUN_ID_CONDITION);

  private static final String GET_LATEST_WORKFLOW_INSTANCE_STATUS_QUERY =
      String.format(GET_WORKFLOW_INSTANCE_FIELDS_TEMPLATE, STATUS_COLUMN, LATEST_RUN_CONDITION);

  private static final String UPDATE_INSTANCE_FAILED_STATUS =
      "UPDATE maestro_workflow_instance SET status='FAILED_2' "
          + "WHERE workflow_id=? AND status='FAILED' AND instance_id>=? AND instance_id<=?";

  private static final String UNBLOCK_INSTANCE_FAILED_STATUS =
      "UPDATE maestro_workflow_instance SET (status,modify_ts,timeline) "
          + "=('FAILED_1',CURRENT_TIMESTAMP,array_append(timeline,?)) "
          + "WHERE workflow_id=? AND instance_id=? AND run_id=? AND status='FAILED'";

  private static final String UNBLOCK_INSTANCES_FAILED_STATUS =
      "UPDATE maestro_workflow_instance SET "
          + "(status,modify_ts,timeline)=('FAILED_1',CURRENT_TIMESTAMP,array_append(timeline,?)) "
          + "WHERE workflow_id=? AND status='FAILED' AND "
          + INSTANCE_IN_SUBQUERY
          + "AND status='FAILED' order by instance_id ASC LIMIT ?)";

  private static final String FROM_WORKFLOW_INSTANCE_TABLE = "FROM maestro_workflow_instance ";
  private static final String FROM_FOREACH_WORKFLOW_INSTANCE_TABLE = FROM_WORKFLOW_INSTANCE_TABLE;

  private static final String ORDER_BY_INSTANCE_ID_RUN_ID_DESC =
      "ORDER BY instance_id DESC, run_id DESC";

  private static final String GET_BATCH_LATEST_RUN_FOREACH_ITERATIONS_ROLLUP_QUERY =
      "SELECT DISTINCT ON (instance_id) instance_id as id, runtime_overview->'rollup_overview' as payload "
          + FROM_FOREACH_WORKFLOW_INSTANCE_TABLE
          + "WHERE workflow_id=? AND instance_id=ANY(?) AND initiator_type='FOREACH' "
          + ORDER_BY_INSTANCE_ID_RUN_ID_DESC;

  private static final String GET_RUNNING_FOREACH_ITERATION_OVERVIEW_QUERY =
      "SELECT instance_id as id, status, runtime_overview->'rollup_overview' as payload "
          + FROM_FOREACH_WORKFLOW_INSTANCE_TABLE
          + "WHERE workflow_id=? AND run_id=? AND instance_id>=? AND initiator_type='FOREACH' "
          + "ORDER BY instance_id DESC";

  private static final String GET_RESTARTING_FOREACH_ITERATION_OVERVIEW_QUERY =
      "SELECT DISTINCT ON (instance_id) instance_id as id,run_id,status,runtime_overview->'rollup_overview' as payload "
          + FROM_FOREACH_WORKFLOW_INSTANCE_TABLE
          + "WHERE workflow_id=? AND run_id>? AND instance_id>=? AND initiator_type='FOREACH' "
          + ORDER_BY_INSTANCE_ID_RUN_ID_DESC;

  private static final String MIN_INSTANCE_ID = "min_instance_id";
  private static final String MAX_INSTANCE_ID = "max_instance_id";
  private static final String SELECT_MIN_MAX_INSTANCE_ID =
      "SELECT min(instance_id) as "
          + MIN_INSTANCE_ID
          + ", max(instance_id) as "
          + MAX_INSTANCE_ID
          + " ";

  private static final String GET_LARGEST_FOREACH_RUN_ID_QUERY =
      "SELECT run_id as id FROM maestro_workflow_instance "
          + "WHERE workflow_id=? AND initiator_type='FOREACH' ORDER BY run_id DESC LIMIT 1";

  private static final String GET_MIN_MAX_WORKFLOW_INSTANCE_IDS_QUERY =
      SELECT_MIN_MAX_INSTANCE_ID + FROM_WORKFLOW_INSTANCE_TABLE + "WHERE workflow_id=?";
  private static final String LATEST_RUN_WORKFLOW_INSTANCE_IDS_QUERY =
      "with inner_filtered as (SELECT instance_id,run_id,"
          + ALL_FIELDS
          + FROM_WORKFLOW_INSTANCE_TABLE
          + "WHERE workflow_id=? AND instance_id<=? AND instance_id>=? ORDER BY instance_id DESC), "
          + "inner_ranked as (SELECT *, ROW_NUMBER() OVER (PARTITION BY instance_id "
          + "ORDER BY run_id DESC) AS rank FROM inner_filtered) "
          + "SELECT * FROM inner_ranked WHERE rank = 1";

  private static final String CHECK_WORKFLOW_ID_IN_INSTANCES_QUERY =
      "SELECT 1 FROM maestro_workflow_instance WHERE workflow_id=? LIMIT 1";

  private static final String INSTANCE_ID_COLUMN = "instance_id";

  private static final String TERMINATION_MESSAGE_TEMPLATE =
      "Workflow instance status becomes [%s] due to reason [%s]";

  private final MaestroQueueSystem queueSystem;

  /**
   * Constructor for Maestro workflow instance DAO.
   *
   * @param dataSource database data source
   * @param objectMapper object mapper
   * @param config configuration
   */
  public MaestroWorkflowInstanceDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
    this.queueSystem = queueSystem;
  }

  private void updateInstances(List<WorkflowInstance> instances) {
    for (WorkflowInstance instance : instances) {
      instance.fillCorrelationIdIfNull();
      instance.setCreateTime(System.currentTimeMillis());
    }
  }

  /**
   * The instance list has already been sized to fit into the batch size limit. max insertion is 10.
   *
   * <p>It explicitly builds a batch query because executeBatch won't work in some cases
   */
  private Set<Long> insertMaestroWorkflowInstances(
      Connection conn,
      List<WorkflowInstance> instances,
      List<String> instanceStrs,
      TerminateThenRunJobEvent jobEvent,
      MessageDto[] message)
      throws SQLException {
    String sql =
        String.format(
            CREATE_WORKFLOW_INSTANCE_QUERY_TEMPLATE,
            String.join(",", Collections.nCopies(instances.size(), VALUE_PLACE_HOLDER)));
    try (PreparedStatement wfiStmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < instances.size(); ++i) {
        wfiStmt.setString(1 + 2 * i, instanceStrs.get(i));
        wfiStmt.setString(2 + 2 * i, instances.get(i).getStatus().name());
      }
      try (ResultSet result = wfiStmt.executeQuery()) {
        Set<Long> res = new HashSet<>(instances.size());
        while (result.next()) {
          res.add(result.getLong(1));
        }
        if (!res.isEmpty()) {
          List<InstanceRunUuid> instanceRunUuids = new ArrayList<>(res.size());
          for (WorkflowInstance instance : instances) {
            if (res.contains(instance.getWorkflowInstanceId())) {
              instanceRunUuids.add(
                  new InstanceRunUuid(
                      instance.getWorkflowInstanceId(),
                      instance.getWorkflowRunId(),
                      instance.getWorkflowUuid()));
            }
          }
          jobEvent.setRunAfter(instanceRunUuids);
          message[0] = queueSystem.enqueue(conn, jobEvent);
        }
        return res; // the inserted instance id
      }
    }
  }

  /**
   * Terminate queued workflow instance run if feasible. Otherwise, do nothing.
   *
   * @param instance workflow instance
   * @param status status to mark
   * @param reason reason to terminate
   * @return return true if terminated. Otherwise, return false.
   */
  public boolean tryTerminateQueuedInstance(
      WorkflowInstance instance, WorkflowInstance.Status status, String reason) {
    MessageDto[] message = new MessageDto[1];
    var ret =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      int res = terminateQueuedInstance(conn, instance, status, reason);
                      if (res == SUCCESS_WRITE_SIZE) {
                        WorkflowInstanceUpdateJobEvent jobEvent =
                            WorkflowInstanceUpdateJobEvent.create(
                                instance, status, System.currentTimeMillis());
                        message[0] = queueSystem.enqueue(conn, jobEvent);
                        return true;
                      }
                      return false;
                    }),
            "tryTerminateQueuedInstance",
            "Failed to terminate the queued workflow instance {}",
            instance.getIdentity());
    queueSystem.notify(message[0]);
    return ret;
  }

  private int terminateQueuedInstance(
      Connection conn, WorkflowInstance instance, WorkflowInstance.Status status, String reason)
      throws SQLException {
    TimelineEvent timelineEvent =
        TimelineLogEvent.warn(TERMINATION_MESSAGE_TEMPLATE, status.name(), reason);
    try (PreparedStatement stmt = conn.prepareStatement(TERMINATE_QUEUED_INSTANCE_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, status.name());
      stmt.setString(++idx, toJson(timelineEvent));
      stmt.setString(++idx, instance.getWorkflowId());
      stmt.setLong(++idx, instance.getWorkflowInstanceId());
      stmt.setLong(++idx, instance.getWorkflowRunId());
      return stmt.executeUpdate();
    }
  }

  /**
   * Terminate queued workflow instances run if possible. Otherwise, do nothing.
   *
   * @param workflowId workflow id to terminate
   * @param limit the size limit to terminate
   * @param status status to mark
   * @param reason reason to terminate
   * @return the number of terminated instances
   */
  public int terminateQueuedInstances(
      String workflowId, int limit, WorkflowInstance.Status status, String reason) {
    TimelineEvent timelineEvent =
        TimelineLogEvent.warn(TERMINATION_MESSAGE_TEMPLATE, status.name(), reason);
    String timelineEventStr = toJson(timelineEvent);
    List<WorkflowInstance> stoppedInstances = new ArrayList<>();
    MessageDto[] message = new MessageDto[1];
    int ret =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      try (PreparedStatement stmt =
                          conn.prepareStatement(TERMINATE_QUEUED_INSTANCES_QUERY)) {
                        int idx = 0;
                        stmt.setString(++idx, status.name());
                        stmt.setString(++idx, timelineEventStr);
                        stmt.setString(++idx, workflowId);
                        stmt.setString(++idx, workflowId);
                        stmt.setInt(++idx, limit);
                        try (ResultSet result = stmt.executeQuery()) {
                          while (result.next()) {
                            WorkflowInstance instance =
                                fromJson(result.getString(1), WorkflowInstance.class);
                            stoppedInstances.add(instance);
                          }
                        }
                        if (!stoppedInstances.isEmpty()) {
                          WorkflowInstanceUpdateJobEvent jobEvent =
                              WorkflowInstanceUpdateJobEvent.create(
                                  stoppedInstances, status, System.currentTimeMillis());
                          message[0] = queueSystem.enqueue(conn, jobEvent);
                        }
                      }
                      return stoppedInstances.size();
                    }),
            "terminateQueuedInstances",
            "Failed to terminate the queued workflow instances for workflow {}",
            workflowId);
    queueSystem.notify(message[0]);
    return ret;
  }

  /**
   * Send terminate events for all running workflow instances with a batch limit.
   *
   * @param workflowId workflow id
   * @param limit size limit for each event
   * @param action terminate action to take
   * @param caller caller info of the termination call
   * @param reason reason to terminate
   * @return the number of terminated running instances
   */
  public int terminateRunningInstances(
      String workflowId,
      int limit,
      Actions.WorkflowInstanceAction action,
      User caller,
      String reason) {
    return Stream.of(GET_RUNNING_CREATED_INSTANCES_QUERY, GET_RUNNING_IN_PROGRESS_INSTANCES_QUERY)
        .mapToInt(
            sql -> {
              AtomicLong lastInstanceId = new AtomicLong(0L);
              int stoppedRunning = 0;
              int stopped = limit;
              while (stopped == limit) {
                TerminateThenRunJobEvent jobEvent =
                    TerminateThenRunJobEvent.init(workflowId, action, caller, reason);
                withRetryableQuery(
                    sql,
                    wfiStmt -> {
                      int idx = 0;
                      wfiStmt.setString(++idx, workflowId);
                      wfiStmt.setLong(++idx, lastInstanceId.get());
                      wfiStmt.setInt(++idx, limit);
                    },
                    result -> {
                      while (result.next()) {
                        jobEvent.addOneRun(
                            result.getLong(INSTANCE_ID_COLUMN),
                            result.getLong("run_id"),
                            result.getString("uuid"));
                      }
                      return null;
                    });
                stopped = jobEvent.size();
                stoppedRunning += stopped;
                if (stopped > 0) {
                  queueSystem.enqueueOrThrow(jobEvent);
                  lastInstanceId.set(
                      jobEvent.getInstanceRunUuids().get(stopped - 1).getInstanceId());
                }
              }
              return stoppedRunning;
            })
        .sum();
  }

  /**
   * Create a list of new workflow instance runs (with run_id already set) in DB. It will skip if an
   * instance id is duplicated and fail if the instance uuid is duplicated, The instance list has
   * already been sized to fit into the batch size limit.
   *
   * @param workflowId workflow id
   * @param instances the list of workflow instances to create
   * @return the created workflow instance ids
   */
  public Optional<Details> runWorkflowInstances(
      String workflowId, List<WorkflowInstance> instances) {
    Checks.checkTrue(
        !ObjectHelper.isCollectionEmptyOrNull(instances),
        "cannot run null or empty workflow instances for %s",
        workflowId);
    updateInstances(instances);
    List<String> instanceStrs = instances.stream().map(this::toJson).toList();
    var jobEvent =
        TerminateThenRunJobEvent.init(
            workflowId,
            Actions.WorkflowInstanceAction.STOP,
            StepRuntime.SYSTEM_USER,
            "Run workflow instance");
    MessageDto[] message = new MessageDto[1];
    try {
      Set<Long> ret =
          withRetryableTransaction(
              conn -> {
                tryUpdateAncestorRunsStatus(
                    conn,
                    workflowId,
                    instances.getFirst().getWorkflowInstanceId(),
                    instances.getLast());
                return insertMaestroWorkflowInstances(
                    conn, instances, instanceStrs, jobEvent, message);
              });
      LOG.debug("Created workflow instances {} for workflow_id [{}]", ret, workflowId);
      queueSystem.notify(message[0]);
      LOG.info(
          "Created [{}]/[{}] workflow instances for workflow id [{}]",
          ret.size(),
          instances.size(),
          workflowId);
      return Optional.empty();
    } catch (MaestroInternalError error) { // non-retryable error
      throw error;
    } catch (MaestroRetryableError retryableError) {
      return Optional.of(retryableError.getDetails());
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to create workflow instance batch (starting at {}) for workflow [{}] due to",
          instances.getFirst().getWorkflowInstanceId(),
          workflowId,
          e);
      return Optional.of(
          Details.create(e, true, "ERROR: failed creating workflow instance batch with an error"));
    }
  }

  /**
   * Update a restarted failed foreach run's status to be `FAILED_2`, meaning FAILED but have been
   * restarted.
   */
  private boolean tryUpdateAncestorRunsStatus(
      Connection conn, String workflowId, long startInstanceId, WorkflowInstance lastInstance)
      throws SQLException {
    if (lastInstance.isFreshRun()) {
      return false; // noop if it is a fresh run.
    }
    try (PreparedStatement wfiStmt = conn.prepareStatement(UPDATE_INSTANCE_FAILED_STATUS)) {
      int idx = 0;
      wfiStmt.setString(++idx, workflowId);
      wfiStmt.setLong(++idx, startInstanceId);
      wfiStmt.setLong(++idx, lastInstance.getWorkflowInstanceId());
      return wfiStmt.executeUpdate() >= SUCCESS_WRITE_SIZE;
    }
  }

  /** Try to unblock failed workflow instance by set its status to FAILED_1. */
  public boolean tryUnblockFailedWorkflowInstance(
      String workflowId, long workflowInstanceId, long workflowRunId, TimelineEvent event) {
    var jobEvent = StartWorkflowJobEvent.create(workflowId);
    String eventStr = toJson(event);
    MessageDto[] message = new MessageDto[1];
    int updated =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      try (PreparedStatement stmt =
                          conn.prepareStatement(UNBLOCK_INSTANCE_FAILED_STATUS)) {
                        int idx = 0;
                        stmt.setString(++idx, eventStr);
                        stmt.setString(++idx, workflowId);
                        stmt.setLong(++idx, workflowInstanceId);
                        stmt.setLong(++idx, workflowRunId);
                        int res = stmt.executeUpdate();
                        if (res == SUCCESS_WRITE_SIZE) {
                          message[0] = queueSystem.enqueue(conn, jobEvent);
                        }
                        return res;
                      }
                    }),
            "tryUnblockFailedWorkflowInstance",
            "Failed to try to unblock the failed workflow instance [{}][{}][{}]",
            workflowId,
            workflowInstanceId,
            workflowRunId);
    queueSystem.notify(message[0]);
    return updated == SUCCESS_WRITE_SIZE;
  }

  /**
   * Try to unblock failed workflow instances for a given workflow id by set their status to
   * FAILED_1. It does the batch update. If there is any update, it will send a start workflow job
   * event within the last batch update. It is possible that the update the failed in the middle and
   * the caller should retry.
   */
  public int tryUnblockFailedWorkflowInstances(
      String workflowId, int batchLimit, TimelineEvent event) {
    var jobEvent = StartWorkflowJobEvent.create(workflowId);
    String eventStr = toJson(event);
    MessageDto[] message = new MessageDto[1];
    int[] totalUnblocked = new int[] {0};
    int unblocked = batchLimit;
    while (unblocked == batchLimit) {
      unblocked =
          withMetricLogError(
              () ->
                  withRetryableTransaction(
                      conn -> {
                        try (PreparedStatement stmt =
                            conn.prepareStatement(UNBLOCK_INSTANCES_FAILED_STATUS)) {
                          int idx = 0;
                          stmt.setString(++idx, eventStr);
                          stmt.setString(++idx, workflowId);
                          stmt.setString(++idx, workflowId);
                          stmt.setInt(++idx, batchLimit);
                          int res = stmt.executeUpdate();
                          if (res < batchLimit && totalUnblocked[0] + res > 0) {
                            message[0] = queueSystem.enqueue(conn, jobEvent);
                          }
                          return res;
                        }
                      }),
              "tryUnblockFailedWorkflowInstances",
              "Failed to try to unblock the failed workflow instances for workflow id[{}]",
              workflowId);
      totalUnblocked[0] += unblocked;
    }
    queueSystem.notify(message[0]);
    return totalUnblocked[0];
  }

  /**
   * Update workflow instance runtime info.
   *
   * @param summary workflow summary with primary key info
   * @param overview workflow instance runtime overview including step execution overview
   * @param status new workflow instance status to update
   * @param markTime new workflow instance time info to update (startTime or endTime)
   * @return empty if done, else with error details.
   */
  public Optional<Details> updateWorkflowInstance(
      WorkflowSummary summary,
      WorkflowRuntimeOverview overview,
      Timeline timeline,
      @Nullable WorkflowInstance.Status status,
      long markTime,
      @Nullable MaestroJobEvent jobEvent) {
    try {
      String sqlQuery = deriveSqlQuery(status);
      final String[] timelineArray =
          timeline == null
              ? null
              : timeline.getTimelineEvents().stream().map(this::toJson).toArray(String[]::new);
      MessageDto[] message = new MessageDto[1];
      int updated =
          withRetryableTransaction(
              conn -> {
                try (PreparedStatement stmt = conn.prepareStatement(sqlQuery)) {
                  int idx = 0;
                  if (status != null) {
                    stmt.setString(++idx, status.name());
                    stmt.setTimestamp(++idx, new Timestamp(markTime));
                  }
                  stmt.setString(++idx, toJson(overview));
                  stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, timelineArray));
                  stmt.setString(++idx, summary.getWorkflowId());
                  stmt.setLong(++idx, summary.getWorkflowInstanceId());
                  stmt.setLong(++idx, summary.getWorkflowRunId());
                  int ret = stmt.executeUpdate();
                  if (ret == SUCCESS_WRITE_SIZE && jobEvent != null) {
                    message[0] = queueSystem.enqueue(conn, jobEvent);
                  }
                  return ret;
                }
              });
      if (updated != SUCCESS_WRITE_SIZE) {
        return Optional.of(
            Details.create(
                "ERROR: updated [%s] (expecting 1) rows for workflow instance %s",
                updated, summary.getIdentity()));
      }
      queueSystem.notify(message[0]);
      return Optional.empty();
    } catch (RuntimeException e) {
      return Optional.of(
          Details.create(e, true, "ERROR: failed updating Runtime Maestro Workflow with an error"));
    }
  }

  private String deriveSqlQuery(WorkflowInstance.Status status) {
    if (status == null) {
      return UPDATE_WORKFLOW_INSTANCE_TIMELINE_QUERY;
    } else if (status == WorkflowInstance.Status.IN_PROGRESS) {
      return UPDATE_WORKFLOW_INSTANCE_START_QUERY;
    } else {
      return UPDATE_WORKFLOW_INSTANCE_END_QUERY;
    }
  }

  /** Mark the execution id for idempotency. */
  @SuppressWarnings({"PMD.AvoidCatchingNPE"})
  public Optional<Details> executeWorkflowInstance(WorkflowSummary summary, String executionId) {
    try {
      int updated =
          withRetryableUpdate(
              UPDATE_WORKFLOW_INSTANCE_EXECUTION_QUERY,
              stmt -> {
                int idx = 0;
                stmt.setString(++idx, executionId);
                stmt.setString(++idx, summary.getWorkflowId());
                stmt.setLong(++idx, summary.getWorkflowInstanceId());
                stmt.setLong(++idx, summary.getWorkflowRunId());
              });
      if (updated == SUCCESS_WRITE_SIZE) {
        return Optional.empty();
      } else {
        WorkflowInstance instance =
            getWorkflowInstanceRun(
                summary.getWorkflowId(),
                summary.getWorkflowInstanceId(),
                summary.getWorkflowRunId());
        if (!instance.getStatus().isTerminal()
            && Objects.equals(executionId, instance.getExecutionId())) {
          LOG.warn(
              "StartTask is executed in workflow [{}] again while maestro workflow instance is in status [{}]",
              executionId,
              instance.getStatus());
          return Optional.empty();
        } else {
          throw new MaestroInternalError(
              "This workflow instance %s either has already been [%s] in another run "
                  + "with an execution_id [%s] instead of this [%s] or has been stopped externally. Fail it.",
              summary.getIdentity(), instance.getStatus(), instance.getExecutionId(), executionId);
        }
      }
    } catch (MaestroInternalError | MaestroNotFoundException | NullPointerException e) {
      throw e;
    } catch (RuntimeException e) {
      return Optional.of(
          Details.create(
              e, true, "ERROR: failed updating Runtime Maestro Workflow with an error, retry it"));
    }
  }

  /**
   * Update workflow instance runtime overview.
   *
   * @param summary workflow summary with primary key info
   * @param overview workflow instance runtime overview including step execution overview
   * @return empty if done, else with error details.
   */
  public Optional<Details> updateRuntimeOverview(
      WorkflowSummary summary, WorkflowRuntimeOverview overview, Timeline timeline) {
    return updateWorkflowInstance(summary, overview, timeline, null, 0, null);
  }

  /**
   * Get workflow instance for a specific workflow instance run. If aggregated flag is true, it will
   * set the aggregated info by loading the latest workflow instance run and then adds the
   * aggregated info into it. So in this case, the caller can expect a not null aggregatedInfo field
   * with a not null status value.
   */
  public WorkflowInstance getWorkflowInstance(
      String workflowId, long workflowInstanceId, String workflowRun, boolean aggregated) {
    long runId = Constants.LATEST_ONE;
    if (!Constants.LATEST_INSTANCE_RUN.equalsIgnoreCase(workflowRun)) {
      runId = Long.parseLong(workflowRun);
    }
    WorkflowInstance instance = getWorkflowInstanceRun(workflowId, workflowInstanceId, runId);
    if (aggregated) {
      instance.setAggregatedInfo(AggregatedViewHelper.computeAggregatedView(instance, true));
    }
    return instance;
  }

  /**
   * Get the instance data of a specific workflow instance run, including the summary of its step
   * instances.
   *
   * @param workflowId workflow id
   * @param instanceId workflow instance id
   * @param runId workflow run id
   */
  public WorkflowInstance getWorkflowInstanceRun(String workflowId, long instanceId, long runId) {
    WorkflowInstance ret =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    runId == Constants.LATEST_ONE
                        ? GET_LATEST_WORKFLOW_INSTANCE_RUN_QUERY
                        : GET_WORKFLOW_INSTANCE_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, instanceId);
                      if (runId != Constants.LATEST_ONE) {
                        stmt.setLong(++idx, runId);
                      }
                    },
                    result -> {
                      if (result.next()) {
                        return workflowInstanceFromResult(result);
                      }
                      return null;
                    }),
            "getWorkflowInstance",
            "Failed to get the workflow instance for [{}][{}][{}]",
            workflowId,
            instanceId,
            runId);
    if (ret == null) {
      throw new MaestroNotFoundException(
          "workflow instance [%s][%s][%s] not found (either not created or deleted)",
          workflowId, instanceId, runId);
    }
    return ret;
  }

  /**
   * Get the instance data of the latest workflow instance run for a specific workflow instance,
   * including the summary of its step instances.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   */
  public WorkflowInstance getLatestWorkflowInstanceRun(String workflowId, long workflowInstanceId) {
    return getWorkflowInstanceRun(workflowId, workflowInstanceId, Constants.LATEST_ONE);
  }

  /**
   * Get the latest workflow instance run for a specific workflow id between an instance id range.
   *
   * @param workflowId workflow id
   * @param startInstanceId workflow start instance id for range
   * @param endInstanceId workflow end instance id for range
   * @return unordered list of workflow instances (not guaranteed to be ordered on instance id)
   */
  public List<WorkflowInstance> getWorkflowInstancesWithLatestRun(
      String workflowId, long startInstanceId, long endInstanceId, boolean aggregated) {

    List<WorkflowInstance> instances =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    LATEST_RUN_WORKFLOW_INSTANCE_IDS_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, endInstanceId);
                      stmt.setLong(++idx, startInstanceId);
                    },
                    result -> {
                      List<WorkflowInstance> rows = new ArrayList<>();
                      while (result.next()) {
                        rows.add(workflowInstanceFromResult(result));
                      }
                      return rows;
                    }),
            "getLatestWorkflowInstanceRuns",
            "Failed to get workflow instances for workflow id: {} between instance id : {} and {}",
            workflowId,
            startInstanceId,
            endInstanceId);
    if (aggregated) {

      instances.forEach(
          instance ->
              instance.setAggregatedInfo(
                  AggregatedViewHelper.computeAggregatedView(instance, true)));
    }
    return instances;
  }

  /**
   * Gets the min and the max workflow instance id for a particular workflow id.
   *
   * @param workflowId the workflow id
   * @return a long array which has length of 2 and the first element is the min workflow instance
   *     id and the second element is the max workflow instance id.
   */
  public long[] getMinMaxWorkflowInstanceIds(String workflowId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_MIN_MAX_WORKFLOW_INSTANCE_IDS_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                },
                result -> {
                  if (result.next()) {
                    return new long[] {
                      result.getLong(MIN_INSTANCE_ID), result.getLong(MAX_INSTANCE_ID)
                    };
                  }
                  return null;
                }),
        "getMinMaxWorkflowInstanceIds",
        "Failed to get the min and max workflow instance ids for workflow id: {}",
        workflowId);
  }

  private WorkflowInstance workflowInstanceFromResult(ResultSet rs) throws SQLException {
    WorkflowInstance instance =
        Checks.notNull(
            getJsonObjectIfPresent(rs, "instance", WorkflowInstance.class),
            "workflow instance column cannot be null");
    instance.setStatus(WorkflowInstance.Status.create(rs.getString(STATUS_COLUMN)));
    instance.setExecutionId(rs.getString("execution_id"));
    instance.setStartTime(getTimestampIfPresent(rs, "start_ts"));
    instance.setEndTime(getTimestampIfPresent(rs, "end_ts"));
    instance.setModifyTime(getTimestampIfPresent(rs, "modify_ts"));
    instance.setRuntimeOverview(
        getJsonObjectIfPresent(rs, "runtime_overview", WorkflowRuntimeOverview.class));
    instance.setTimeline(getTimelineIfPresent(rs));
    return instance;
  }

  private <T> T getJsonObjectIfPresent(ResultSet rs, String field, Class<T> clazz)
      throws SQLException {
    String json = rs.getString(field);
    if (json != null) {
      return fromJson(json, clazz);
    }
    return null;
  }

  private Timeline getTimelineIfPresent(ResultSet rs) throws SQLException {
    Array payload = rs.getArray("timeline");
    if (payload != null) {
      String[] json = (String[]) payload.getArray();
      if (json != null) {
        Timeline timeline = new Timeline(null);
        for (String event : json) {
          timeline.add(fromJson(event, TimelineEvent.class));
        }
        return timeline;
      }
    }
    return null;
  }

  /**
   * Get the latest status of a workflow instance.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   */
  public WorkflowInstance.Status getLatestWorkflowInstanceStatus(
      String workflowId, long workflowInstanceId) {
    return getWorkflowInstanceStatus(workflowId, workflowInstanceId, Constants.LATEST_ONE);
  }

  /**
   * Get the status of a specific workflow instance run.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param workflowRunId workflow run id
   */
  public WorkflowInstance.Status getWorkflowInstanceStatus(
      String workflowId, long workflowInstanceId, long workflowRunId) {
    String status = getWorkflowInstanceRawStatus(workflowId, workflowInstanceId, workflowRunId);
    return withMetricLogError(
        () -> {
          if (status == null) {
            return null;
          }
          return WorkflowInstance.Status.create(status);
        },
        "getWorkflowInstanceStatus",
        "Failed to parse the workflow instance status [{}] for [{}][{}][{}]",
        status,
        workflowId,
        workflowInstanceId,
        workflowRunId);
  }

  @VisibleForTesting
  String getWorkflowInstanceRawStatus(
      String workflowId, long workflowInstanceId, long workflowRunId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                Constants.LATEST_ONE == workflowRunId
                    ? GET_LATEST_WORKFLOW_INSTANCE_STATUS_QUERY
                    : GET_WORKFLOW_INSTANCE_STATUS_QUERY,
                wfiStmt -> {
                  int idx = 0;
                  wfiStmt.setString(++idx, workflowId);
                  wfiStmt.setLong(++idx, workflowInstanceId);
                  if (Constants.LATEST_ONE != workflowRunId) {
                    wfiStmt.setLong(++idx, workflowRunId);
                  }
                },
                result -> {
                  if (result.next()) {
                    return result.getString(STATUS_COLUMN);
                  }
                  return null;
                }),
        "getWorkflowInstanceRawStatus",
        "Failed to get the workflow instance status for [{}][{}][{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId);
  }

  /**
   * Get foreach run iteration instance status stats for a given workflow id, run_id, across all its
   * iterations/instances larger than the current checkpoint, which is a watermark to track the open
   * (not finalized) iterations.
   *
   * <p>Table scan is based on foreach_index and its performance cost for a given workflow id and
   * run id is acceptable for iterations/instances no greater than 125K. Therefore, the current
   * foreach iteration limit is 100K, we don't need to worry about the performance. Also note that
   * this select statement does not lock the table and the stats are just almost accurate.
   *
   * <p>If we need to support larger loop than 125K, should consider to either get partial stats or
   * stats using follower read mode.
   *
   * @param workflowId workflow id
   * @return the list of ForeachIterationOverview since the checkpoint. For the restart case, the
   *     result also includes the latest run_id of the restarted iteration.
   */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  public List<ForeachIterationOverview> getForeachIterationOverviewWithCheckpoint(
      String workflowId, long runId, long checkpoint, boolean isRestarting) {
    List<ForeachIterationOverview> overviews = new ArrayList<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                isRestarting
                    ? GET_RESTARTING_FOREACH_ITERATION_OVERVIEW_QUERY
                    : GET_RUNNING_FOREACH_ITERATION_OVERVIEW_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                  stmt.setLong(++idx, runId);
                  stmt.setLong(++idx, checkpoint);
                },
                result -> {
                  while (result.next()) {
                    long instanceId = result.getLong(ID_COLUMN);
                    WorkflowInstance.Status status =
                        WorkflowInstance.Status.create(result.getString(STATUS_COLUMN));
                    WorkflowRollupOverview rollup = null;
                    String payload = result.getString(PAYLOAD_COLUMN);
                    if (payload != null) {
                      rollup =
                          fromJson(result.getString(PAYLOAD_COLUMN), WorkflowRollupOverview.class);
                    }
                    var overview = new ForeachIterationOverview(instanceId, status, rollup);
                    if (isRestarting) {
                      overview.setRunId(result.getLong(2));
                    }
                    overviews.add(overview);
                  }
                  return overviews;
                }),
        "getForeachIterationOverviewWithCheckpoint",
        "Failed to get foreach iteration stats for [{}][{}] with checkpoint [{}] for isRestarting [{}]",
        workflowId,
        runId,
        checkpoint,
        isRestarting);
  }

  /**
   * Get rollups of the latest runs of foreach inline workflow instances for a given list of
   * iteration ids.
   *
   * @param workflowId foreach inline workflow id
   * @param instanceIds the workflow instance ids for iterations we want to query
   * @return a list of workflow rollups.
   */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  public List<WorkflowRollupOverview> getBatchForeachLatestRunRollupForIterations(
      String workflowId, List<Long> instanceIds) {
    List<WorkflowRollupOverview> rollups = new ArrayList<>();

    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt =
                      conn.prepareStatement(GET_BATCH_LATEST_RUN_FOREACH_ITERATIONS_ROLLUP_QUERY)) {
                    stmt.setString(1, workflowId);
                    stmt.setArray(2, conn.createArrayOf("INT8", instanceIds.toArray(new Long[0])));
                    try (ResultSet result = stmt.executeQuery()) {
                      while (result.next()) {
                        String payload = result.getString(PAYLOAD_COLUMN);
                        if (payload != null) {
                          WorkflowRollupOverview rollup =
                              fromJson(
                                  result.getString(PAYLOAD_COLUMN), WorkflowRollupOverview.class);
                          if (rollup != null) {
                            rollups.add(rollup);
                          }
                        }
                      }
                      return rollups;
                    }
                  }
                }),
        "getBatchForeachLatestRunRollupForIterations",
        "Failed to get rollups of the latest foreach inline instances for foreach iterations [{}] "
            + "for foreach workflow id of [{}]",
        instanceIds,
        workflowId);
  }

  /**
   * Get the largest foreach run_id from all previous runs.
   *
   * @param workflowId foreach inline workflow id
   * @return max run_id
   */
  public long getLargestForeachRunIdFromRuns(String workflowId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_LARGEST_FOREACH_RUN_ID_QUERY,
                stmt -> stmt.setString(1, workflowId),
                result -> {
                  if (result.next()) {
                    return result.getLong(ID_COLUMN);
                  }
                  return 0L;
                }),
        "getLargestForeachRunIdFromRuns",
        "Failed to get the largest foreach run_id for all runs of inline workflow [{}]",
        workflowId);
  }

  /** Check if there is any existing workflow instance for a given workflow id. */
  public boolean existWorkflowIdInInstances(String workflowId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                CHECK_WORKFLOW_ID_IN_INSTANCES_QUERY,
                stmt -> stmt.setString(1, workflowId),
                ResultSet::next),
        "existWorkflowIdInInstances",
        "Failed to check the existence of the workflow instance for workflow id [{}]",
        workflowId);
  }
}
