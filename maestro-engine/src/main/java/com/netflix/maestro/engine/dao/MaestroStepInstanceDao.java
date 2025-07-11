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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.utils.ResultProcessor;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.instance.StepAttemptState;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.signal.SignalDependencies;
import com.netflix.maestro.models.signal.SignalOutputs;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.models.MessageDto;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ObjectHelper;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;

/**
 * DAO for saving and retrieving Maestro step instance data model.
 *
 * <p>In the data model, we use `null` to indicate `unset`.
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
public class MaestroStepInstanceDao extends AbstractDatabaseDao {
  private static final TypeReference<Map<String, Artifact>> ARTIFACTS_REFERENCE =
      new TypeReference<>() {};

  private static final String ADD_STEP_INSTANCE_POSTFIX =
      "INTO maestro_step_instance (instance,runtime_state,dependencies,outputs,artifacts,timeline) "
          + "VALUES (?::json,?::jsonb,?::json,?::json,?::jsonb,?)";

  private static final String CREATE_STEP_INSTANCE_QUERY = "INSERT " + ADD_STEP_INSTANCE_POSTFIX;

  private static final String UPSERT_STEP_INSTANCE_QUERY =
      CREATE_STEP_INSTANCE_QUERY
          + " ON CONFLICT (workflow_id, workflow_instance_id, step_id, workflow_run_id, step_attempt_id)"
          + "DO UPDATE SET instance=EXCLUDED.instance,runtime_state=EXCLUDED.runtime_state,"
          + "dependencies=EXCLUDED.dependencies,outputs=EXCLUDED.outputs,"
          + "artifacts=EXCLUDED.artifacts,timeline=EXCLUDED.timeline";

  private static final String WHERE_CONDITION_BY_WORKFLOW_IDS =
      "WHERE workflow_id=? AND workflow_instance_id=? AND workflow_run_id=?";

  private static final String WHERE_CONDITION_BY_IDS =
      WHERE_CONDITION_BY_WORKFLOW_IDS + " AND step_id=? AND step_attempt_id=?";

  private static final String UPDATE_STEP_INSTANCE_QUERY =
      "UPDATE maestro_step_instance SET (runtime_state,dependencies,outputs,artifacts,timeline) "
          + "= (?::jsonb,?::json,?::json,?::jsonb,?) "
          + WHERE_CONDITION_BY_IDS;

  private static final String SELECT_STEP_FIELDS = "SELECT %s FROM maestro_step_instance ";

  private static final String GET_STEP_INSTANCE_FIELD_QUERY_TEMPLATE =
      SELECT_STEP_FIELDS + WHERE_CONDITION_BY_IDS;

  private static final String GET_LATEST_STEP_INSTANCE_FIELD_QUERY_TEMPLATE =
      SELECT_STEP_FIELDS
          + WHERE_CONDITION_BY_WORKFLOW_IDS
          + " AND step_id=? ORDER BY step_attempt_id DESC LIMIT 1";

  private static final String GET_LATEST_STEP_INSTANCE_ARTIFACTS_QUERY_FORMAT =
      "("
          + String.format(SELECT_STEP_FIELDS, StepInstanceField.ARTIFACTS)
          + WHERE_CONDITION_BY_WORKFLOW_IDS
          + " AND step_id=? ORDER BY step_attempt_id DESC LIMIT 1) ";

  private static final String GET_STEP_INSTANCES_FIELD_QUERY_TEMPLATE =
      SELECT_STEP_FIELDS + WHERE_CONDITION_BY_WORKFLOW_IDS + " AND step_id=?";

  private static final String GET_STEP_INSTANCES_QUERY_TEMPLATE =
      SELECT_STEP_FIELDS + WHERE_CONDITION_BY_WORKFLOW_IDS;

  private static final String GET_ALL_STEP_LAST_ATTEMPT_STATE_QUERY =
      "SELECT DISTINCT(step_id) as id, first_value(runtime_state) "
          + "OVER (PARTITION BY step_id ORDER BY step_attempt_id DESC) as payload FROM maestro_step_instance "
          + WHERE_CONDITION_BY_WORKFLOW_IDS;

  private static final String GET_STEP_LAST_ATTEMPT_STATE_QUERY =
      GET_ALL_STEP_LAST_ATTEMPT_STATE_QUERY + " AND step_id=ANY (?)";

  private static final String GET_STEP_LAST_ATTEMPT_OVERALL_DEPENDENCIES_QUERY =
      "SELECT DISTINCT(step_id), first_value(dependencies) OVER (PARTITION BY step_id "
          + "ORDER BY step_attempt_id DESC) as dependencies FROM maestro_step_instance "
          + WHERE_CONDITION_BY_WORKFLOW_IDS;

  private static final String GET_STEP_FIELD_QUERY_FROM =
      " FROM maestro_step_instance WHERE workflow_id=? AND workflow_instance_id=? ";

  private static final String GET_ALL_LATEST_ATTEMPT_STEP_STATUS_QUERY =
      "SELECT DISTINCT(step_id) as id, first_value(runtime_state->>'status') "
          + "OVER (PARTITION BY step_id ORDER BY workflow_run_id DESC, step_attempt_id DESC) as status "
          + GET_STEP_FIELD_QUERY_FROM;

  private static final String INNER_RANK_QUERY_ALL_FIELD_WITH =
      "WITH inner_ranked AS (SELECT " + StepInstanceField.ALL.field;

  private static final String GET_ALL_LATEST_ATTEMPT_STEP_QUERY =
      INNER_RANK_QUERY_ALL_FIELD_WITH
          + ", ROW_NUMBER() OVER (PARTITION BY step_id ORDER BY workflow_run_id DESC, step_attempt_id DESC) AS rank"
          + GET_STEP_FIELD_QUERY_FROM
          + "AND step_id=ANY(?)) SELECT * FROM inner_ranked WHERE rank=1";

  private static final String GET_LATEST_ARTIFACT_QUERY_TEMPLATE =
      "SELECT artifacts->'%s' as payload "
          + GET_STEP_FIELD_QUERY_FROM
          + "AND step_id=? AND artifacts->'%s' IS NOT NULL "
          + "ORDER BY workflow_run_id DESC, step_attempt_id DESC LIMIT 1";

  private static final String GET_STEP_INSTANCE_VIEW_QUERY =
      "SELECT "
          + StepInstanceField.ALL.field
          + GET_STEP_FIELD_QUERY_FROM
          + " AND step_id=? ORDER BY workflow_run_id DESC, step_attempt_id DESC LIMIT 1";

  private static final String GET_STEP_ATTEMPT_STATE_QUERY =
      "SELECT workflow_id, workflow_instance_id, workflow_run_id, step_id, step_attempt_id, runtime_state"
          + GET_STEP_FIELD_QUERY_FROM
          + " AND step_id=? ORDER BY workflow_run_id DESC, step_attempt_id DESC LIMIT "
          + Constants.STEP_ATTEMPT_STATE_LIMIT;

  private static final String GET_EVALUATED_RESULTS_FROM_FOREACH_TEMPLATE =
      "SELECT DISTINCT(workflow_instance_id) as id, first_value(instance->'params'->?->>'evaluated_result') "
          + "OVER (PARTITION BY workflow_instance_id ORDER BY workflow_run_id DESC, step_attempt_id DESC) as payload "
          + "FROM maestro_step_instance WHERE workflow_id=? and step_id=?";

  private static final String GET_PARAM_TYPE_FROM_FOREACH_TEMPLATE =
      "SELECT instance->'params'->?->>'type' as payload FROM maestro_step_instance "
          + "WHERE workflow_id=? and step_id=? limit 1";

  // generates a positive random unique number, which is different from `unique_rowid()`
  private static final String GET_UNIQUE_ROWID =
      "SELECT ('x' || translate(gen_random_uuid()::TEXT, '-', ''))::bit(63)::INT8 as id";

  private static final String BATCH_UNION_STATEMENT = "UNION ALL ";

  private static final String GET_STEP_INSTANCE_VIEWS_QUERY =
      INNER_RANK_QUERY_ALL_FIELD_WITH
          + ", ROW_NUMBER() OVER (PARTITION BY step_id ORDER BY step_attempt_id DESC) AS rank"
          + GET_STEP_FIELD_QUERY_FROM
          + "AND workflow_run_id=?) SELECT * FROM inner_ranked WHERE rank=1";

  private final MaestroQueueSystem queueSystem;

  /**
   * Constructor for Maestro step instance DAO.
   *
   * @param dataSource database data source
   * @param objectMapper object mapper
   * @param config configuration
   */
  public MaestroStepInstanceDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
    this.queueSystem = queueSystem;
  }

  /**
   * Create a new step instance data or update all of its data. Within the transaction, it also
   * sends the job event to the update processing queue with an exactly once guarantee.
   *
   * @param instance step instance
   */
  public void insertOrUpsertStepInstance(
      StepInstance instance, boolean inserted, @Nullable MaestroJobEvent jobEvent) {
    final StepRuntimeState runtimeState = instance.getRuntimeState();
    final SignalDependencies dependencies = instance.getSignalDependencies();
    final SignalOutputs outputs = instance.getSignalOutputs();
    final Map<String, Artifact> artifacts = instance.getArtifacts();
    final Timeline timeline = instance.getTimeline();
    try {
      instance.setRuntimeState(null);
      instance.setSignalDependencies(null);
      instance.setSignalOutputs(null);
      instance.setArtifacts(null);
      instance.setTimeline(null);
      final String stepInstanceStr = toJson(instance);
      final String runtimeStateStr = toJson(runtimeState);
      final String stepDependenciesSummariesStr = toJson(dependencies);
      final String outputsStr = outputs == null ? null : toJson(outputs);
      final String artifactsStr = toJson(artifacts);
      final String[] timelineArray =
          timeline == null
              ? null
              : timeline.getTimelineEvents().stream().map(this::toJson).toArray(String[]::new);
      MessageDto message =
          withMetricLogError(
              () ->
                  withRetryableTransaction(
                      conn -> {
                        try (PreparedStatement stmt =
                            conn.prepareStatement(
                                inserted
                                    ? UPSERT_STEP_INSTANCE_QUERY
                                    : CREATE_STEP_INSTANCE_QUERY)) {
                          int idx = 0;
                          stmt.setString(++idx, stepInstanceStr);
                          stmt.setString(++idx, runtimeStateStr);
                          stmt.setString(++idx, stepDependenciesSummariesStr);
                          stmt.setString(++idx, outputsStr);
                          stmt.setString(++idx, artifactsStr);
                          stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, timelineArray));
                          int res = stmt.executeUpdate();
                          if (res == SUCCESS_WRITE_SIZE && jobEvent != null) {
                            return queueSystem.enqueue(conn, jobEvent);
                          }
                          return null;
                        }
                      }),
              "insertOrUpsertStepInstance",
              "Failed to insert or upsert step instance {}[{}]",
              instance.getIdentity(),
              instance.getStepAttemptId());
      queueSystem.notify(message);
    } finally {
      instance.setTimeline(timeline);
      instance.setArtifacts(artifacts);
      instance.setSignalDependencies(dependencies);
      instance.setSignalOutputs(outputs);
      instance.setRuntimeState(runtimeState);
    }
  }

  /**
   * Update step instance table with runtime updates. Within the transaction, it also sends the job
   * event to the notification queue for external notification with an exactly once guarantee.
   *
   * @param workflowSummary workflow instance summary
   * @param stepSummary step instance runtime summary
   */
  public void updateStepInstance(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary stepSummary,
      @Nullable MaestroJobEvent jobEvent) {
    final String runtimeState = toJson(stepSummary.getRuntimeState());
    final String stepDependenciesSummariesStr = toJson(stepSummary.getSignalDependencies());
    final String artifacts = toJson(stepSummary.getArtifacts());
    final String stepOutputs =
        stepSummary.getSignalOutputs() == null ? null : toJson(stepSummary.getSignalOutputs());
    final String[] timelineArray =
        stepSummary.getTimeline() == null
            ? null
            : stepSummary.getTimeline().getTimelineEvents().stream()
                .map(this::toJson)
                .toArray(String[]::new);
    MessageDto message =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn -> {
                      try (PreparedStatement stmt =
                          conn.prepareStatement(UPDATE_STEP_INSTANCE_QUERY)) {
                        int idx = 0;
                        stmt.setString(++idx, runtimeState);
                        stmt.setString(++idx, stepDependenciesSummariesStr);
                        stmt.setString(++idx, stepOutputs);
                        stmt.setString(++idx, artifacts);
                        stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, timelineArray));
                        stmt.setString(++idx, workflowSummary.getWorkflowId());
                        stmt.setLong(++idx, workflowSummary.getWorkflowInstanceId());
                        stmt.setLong(++idx, workflowSummary.getWorkflowRunId());
                        stmt.setString(++idx, stepSummary.getStepId());
                        stmt.setLong(++idx, stepSummary.getStepAttemptId());
                        int res = stmt.executeUpdate();
                        if (res == SUCCESS_WRITE_SIZE && jobEvent != null) {
                          return queueSystem.enqueue(conn, jobEvent);
                        }
                        return null;
                      }
                    }),
            "updateStepInstance",
            "Failed to update workflow instance {}'s step instance {}",
            workflowSummary.getIdentity(),
            stepSummary.getIdentity());
    queueSystem.notify(message);
  }

  /** Get step instance from DB for a given step instance attempt. */
  public StepInstance getStepInstance(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    return getStepInstanceFieldByIds(
        StepInstanceField.ALL,
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        stepAttempt,
        this::maestroStepFromResult);
  }

  private StepInstance maestroStepFromResult(ResultSet rs) throws SQLException {
    StepInstance instance = getInstance(rs);
    instance.setRuntimeState(getRuntimeState(rs));
    instance.setSignalDependencies(getDependencies(rs));
    instance.setSignalOutputs(getOutputs(rs));
    instance.setArtifacts(getArtifacts(rs));
    instance.setTimeline(getTimeline(rs));
    return instance;
  }

  private StepAttemptState maestroStepAttemptStateFromResult(ResultSet rs) throws SQLException {
    StepAttemptState state = new StepAttemptState();
    state.setRuntimeState(getRuntimeState(rs));
    state.setWorkflowInstanceId(getWorkflowInstanceId(rs));
    state.setStepId(getStepId(rs));
    state.setStepAttemptId(getStepAttemptId(rs));
    state.setWorkflowId(getWorkflowId(rs));
    state.setWorkflowRunId(getWorkflowRunId(rs));
    return state;
  }

  private StepInstance getInstance(ResultSet rs) throws SQLException {
    return fromJson(
        Checks.notNull(
            rs.getString(StepInstanceField.INSTANCE.field), "step instance column cannot be null"),
        StepInstance.class);
  }

  private StepRuntimeState getRuntimeState(ResultSet rs) throws SQLException {
    return fromJson(
        Checks.notNull(
            rs.getString(StepInstanceField.RUNTIME_STATE.field),
            "step runtime_state column cannot be null"),
        StepRuntimeState.class);
  }

  private long getWorkflowRunId(ResultSet rs) throws SQLException {
    return Checks.notNull(
        rs.getLong(StepInstanceField.WORKFLOW_RUN_ID.field),
        "step workflow_run_id column cannot be null");
  }

  private long getStepAttemptId(ResultSet rs) throws SQLException {
    return Checks.notNull(
        rs.getLong(StepInstanceField.STEP_ATTEMPT_ID.field),
        "step_attempt_id column cannot be null");
  }

  private String getStepId(ResultSet rs) throws SQLException {
    return Checks.notNull(
        rs.getString(StepInstanceField.STEP_ID.field), "step_id column cannot be null");
  }

  private long getWorkflowInstanceId(ResultSet rs) throws SQLException {
    return Checks.notNull(
        rs.getLong(StepInstanceField.WORKFLOW_INSTANCE_ID.field),
        "step workflow_instance_id column cannot be null");
  }

  private String getWorkflowId(ResultSet rs) throws SQLException {
    return Checks.notNull(
        rs.getString(StepInstanceField.WORKFLOW_ID.field),
        "step workflow_id column cannot be null");
  }

  private Map<String, Artifact> getArtifacts(ResultSet rs) throws SQLException {
    String artifacts = rs.getString(StepInstanceField.ARTIFACTS.field);
    if (artifacts == null) {
      return Collections.emptyMap();
    }
    Map<String, Artifact> ret = fromJson(artifacts, ARTIFACTS_REFERENCE);
    if (ret == null) {
      return Collections.emptyMap();
    }
    return ret;
  }

  private Timeline getTimeline(ResultSet rs) throws SQLException {
    Array payload = rs.getArray(StepInstanceField.TIMELINE.field);
    if (payload == null) {
      return new Timeline(Collections.emptyList());
    }
    String[] json = (String[]) payload.getArray();
    if (json == null) {
      return new Timeline(Collections.emptyList());
    }
    Timeline timeline = new Timeline(null);
    for (String event : json) {
      timeline.add(fromJson(event, TimelineEvent.class));
    }
    return timeline;
  }

  private SignalOutputs getOutputs(ResultSet rs) throws SQLException {
    String outputs = rs.getString(StepInstanceField.OUTPUTS.field);
    if (outputs == null) {
      return null;
    }
    return fromJson(outputs, SignalOutputs.class);
  }

  private SignalDependencies getDependencies(ResultSet rs) throws SQLException {
    String summary = rs.getString(StepInstanceField.DEPENDENCIES.field);
    if (summary == null) {
      return null;
    }
    return fromJson(summary, SignalDependencies.class);
  }

  enum StepInstanceField {
    ALL("instance,runtime_state,dependencies,outputs,artifacts,timeline"),
    INSTANCE("instance"),
    RUNTIME_STATE("runtime_state"),
    DEPENDENCIES("dependencies"),

    OUTPUTS("outputs"),
    ARTIFACTS("artifacts"),
    TIMELINE("timeline"),
    WORKFLOW_RUN_ID("workflow_run_id"),
    WORKFLOW_ID("workflow_id"),
    STEP_ATTEMPT_ID("step_attempt_id"),
    WORKFLOW_INSTANCE_ID("workflow_instance_id"),
    STEP_ID("step_id");

    private final String field;

    StepInstanceField(String field) {
      this.field = field;
    }
  }

  private <T> T getStepInstanceFieldByIds(
      StepInstanceField fieldName,
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt,
      ResultProcessor<T> processor) {
    String sqlQuery;
    if (Constants.LATEST_INSTANCE_RUN.equalsIgnoreCase(stepAttempt)) {
      sqlQuery = String.format(GET_LATEST_STEP_INSTANCE_FIELD_QUERY_TEMPLATE, fieldName.field);
    } else {
      sqlQuery = String.format(GET_STEP_INSTANCE_FIELD_QUERY_TEMPLATE, fieldName.field);
    }
    T ret =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    sqlQuery,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, workflowInstanceId);
                      stmt.setLong(++idx, workflowRunId);
                      stmt.setString(++idx, stepId);
                      if (!Constants.LATEST_INSTANCE_RUN.equalsIgnoreCase(stepAttempt)) {
                        stmt.setLong(++idx, Long.parseLong(stepAttempt));
                      }
                    },
                    result -> {
                      if (result.next()) {
                        return processor.process(result);
                      }
                      return null;
                    }),
            "getStepInstanceFieldByIds",
            "Failed to get the field {} for workflow instance [{}][{}][{}]'s step [{}][{}]",
            fieldName.field,
            workflowId,
            workflowInstanceId,
            workflowRunId,
            stepId,
            stepAttempt);
    if (ret == null) {
      throw new MaestroNotFoundException(
          "workflow instance [%s][%s][%s]'s step instance [%s][%s] not found (either not created or deleted)",
          workflowId, workflowInstanceId, workflowRunId, stepId, stepAttempt);
    }
    return ret;
  }

  /** Get step instance runtime state from DB for a given step instance attempt. */
  public StepRuntimeState getStepInstanceRuntimeState(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    return getStepInstanceFieldByIds(
        StepInstanceField.RUNTIME_STATE,
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        stepAttempt,
        this::getRuntimeState);
  }

  /** Get step instance artifacts from DB for a given step instance attempt. */
  public Map<String, Artifact> getStepInstanceArtifacts(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    return getStepInstanceFieldByIds(
        StepInstanceField.ARTIFACTS,
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        stepAttempt,
        this::getArtifacts);
  }

  /** Get step instance tags from DB for a given step instance attempt. */
  public TagList getStepInstanceTags(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    return getStepInstanceFieldByIds(
        StepInstanceField.INSTANCE,
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        stepAttempt,
        rs -> ObjectHelper.valueOrDefault(getInstance(rs).getTags(), Defaults.DEFAULT_TAG_LIST));
  }

  /** Get step instance timeline from DB for a given step instance attempt. */
  public Timeline getStepInstanceTimeline(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    return getStepInstanceFieldByIds(
        StepInstanceField.TIMELINE,
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        stepAttempt,
        this::getTimeline);
  }

  /** Get step instance step dependencies from DB for a given step instance attempt. */
  public SignalDependencies getSignalDependencies(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    try {
      return getStepInstanceFieldByIds(
          StepInstanceField.DEPENDENCIES,
          workflowId,
          workflowInstanceId,
          workflowRunId,
          stepId,
          stepAttempt,
          this::getDependencies);
    } catch (MaestroNotFoundException ex) {
      // step dependency summary is not set
      return null;
    }
  }

  /** Get step instance output signals from DB for a given step instance attempt. */
  public SignalOutputs getSignalOutputs(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String stepAttempt) {
    try {
      return getStepInstanceFieldByIds(
          StepInstanceField.OUTPUTS,
          workflowId,
          workflowInstanceId,
          workflowRunId,
          stepId,
          stepAttempt,
          this::getOutputs);
    } catch (MaestroNotFoundException ex) {
      // step output signal summary is not set
      return null;
    }
  }

  /** Get all step attempts from DB for a given a workflow instance run. */
  public List<StepInstance> getAllStepInstances(
      String workflowId, long workflowInstanceId, long workflowRunId) {
    return getStepInstancesByIds(
        workflowId, workflowInstanceId, workflowRunId, null, this::maestroStepFromResult);
  }

  /** Get step attempts from DB for a given step id of a workflow instance run. */
  public List<StepInstance> getStepInstances(
      String workflowId, long workflowInstanceId, long workflowRunId, String stepId) {
    return getStepInstancesByIds(
        workflowId, workflowInstanceId, workflowRunId, stepId, this::maestroStepFromResult);
  }

  private List<StepAttemptState> stepAttemptStateFromResult(ResultSet rs) throws SQLException {
    List<StepAttemptState> ret = new ArrayList<>();
    while (rs.next()) {
      ret.add(maestroStepAttemptStateFromResult(rs));
    }
    return ret;
  }

  /**
   * Splits incoming map into X maps where each map has maximum of
   * BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY elements.
   *
   * @param map map to split
   * @return list of maps calculated by splitting passed map, where each map contains at most
   *     BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY elements
   * @param <K> map key
   * @param <V> map value
   */
  private static <K, V> List<Map<K, V>> splitMap(Map<K, V> map) {
    int numSplits =
        (int) Math.ceil((double) map.size() / Constants.BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY);
    return IntStream.range(0, numSplits)
        .mapToObj(
            i ->
                map.entrySet().stream()
                    .skip((long) i * Constants.BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY)
                    .limit(Constants.BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .collect(Collectors.toList());
  }

  /**
   * Each step instance is queried as a point query, every N queries are combined into one using a
   * UNION.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepIdToRunId Map of stepId to runId combination for every step instance we need to
   *     query
   * @return List of artifacts for every passed stepId:runId combination
   */
  public List<Map<String, Artifact>> getBatchStepInstancesArtifactsFromList(
      String workflowId, long workflowInstanceId, Map<String, Long> stepIdToRunId) {
    List<Map<String, Long>> batches = splitMap(stepIdToRunId);

    List<Map<String, Artifact>> results = new ArrayList<>();

    for (Map<String, Long> batch : batches) {
      results.addAll(
          getBatchStepInstancesArtifactsFromListLimited(workflowId, workflowInstanceId, batch));
    }

    return results;
  }

  /**
   * Queries step instances for each stepIdToRunId element, where each query is a point query and is
   * combined with a UNION. Do not use this method for many elements, use
   * getBatchStepInstancesArtifactsFromList that handles batching logic.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepIdToRunId Map of stepId to runId combination for every step instance we need to
   *     query
   * @return List of artifacts for every passed stepId:runId combination
   */
  @VisibleForTesting
  List<Map<String, Artifact>> getBatchStepInstancesArtifactsFromListLimited(
      String workflowId, long workflowInstanceId, Map<String, Long> stepIdToRunId) {
    String sql =
        String.join(
            BATCH_UNION_STATEMENT,
            Collections.nCopies(
                stepIdToRunId.size(), GET_LATEST_STEP_INSTANCE_ARTIFACTS_QUERY_FORMAT));

    List<Map<String, Artifact>> results =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    sql,
                    stmt -> {
                      int idx = 0;
                      for (Map.Entry<String, Long> stepIdRunId : stepIdToRunId.entrySet()) {
                        stmt.setString(++idx, workflowId);
                        stmt.setLong(++idx, workflowInstanceId);
                        stmt.setLong(++idx, stepIdRunId.getValue());
                        stmt.setString(++idx, stepIdRunId.getKey());
                      }
                    },
                    result -> {
                      List<Map<String, Artifact>> instances = new ArrayList<>();
                      while (result.next()) {
                        instances.add(getArtifacts(result));
                      }
                      return instances;
                    }),
            "getBatchStepInstancesArtifactsFromListLimited",
            "Failed to get step instances artifacts by query for workflow instance [{}][{}]",
            workflowId,
            workflowInstanceId);
    if (results == null || results.isEmpty()) {
      throw new MaestroNotFoundException(
          "Step instances artifacts [%s][%s] not found (either empty, not created, or deleted)",
          workflowId, workflowInstanceId);
    }
    return results;
  }

  private <T> List<T> getStepInstancesByIds(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      ResultProcessor<T> processor) {
    String queryTemplate =
        stepId == null
            ? GET_STEP_INSTANCES_QUERY_TEMPLATE
            : GET_STEP_INSTANCES_FIELD_QUERY_TEMPLATE;
    String sqlQuery = String.format(queryTemplate, StepInstanceField.ALL.field);
    List<T> ret =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    sqlQuery,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, workflowInstanceId);
                      stmt.setLong(++idx, workflowRunId);
                      if (stepId != null) {
                        stmt.setString(++idx, stepId);
                      }
                    },
                    result -> {
                      List<T> instances = new ArrayList<>();
                      while (result.next()) {
                        instances.add(processor.process(result));
                      }
                      return instances;
                    }),
            "getStepInstancesByIds",
            "Failed to get step instances for workflow instance [{}][{}][{}]"
                + (stepId == null ? "" : "'s step [{}]"),
            workflowId,
            workflowInstanceId,
            workflowRunId,
            stepId);
    if (ret == null || ret.isEmpty()) {
      throw new MaestroNotFoundException(
          "Step instances of workflow instance [%s][%s][%s]"
              + (stepId == null ? "" : "'s step [%s]")
              + " not found (either empty, not created, or deleted)",
          workflowId,
          workflowInstanceId,
          workflowRunId,
          stepId);
    }
    return ret;
  }

  /**
   * Get the step runtime states (only the last attempt) for a given workflow instance run.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param workflowRunId workflow instance run id
   * @return the latest step attempt states
   */
  public Map<String, StepRuntimeState> getAllStepStates(
      String workflowId, long workflowInstanceId, long workflowRunId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_ALL_STEP_LAST_ATTEMPT_STATE_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                  stmt.setLong(++idx, workflowInstanceId);
                  stmt.setLong(++idx, workflowRunId);
                },
                this::getStringStepRuntimeStateMap),
        "getAllStepStates",
        "Failed to get the overall step attempt stats for [{}][{}][{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId);
  }

  /**
   * Get the step runtime states (only the last attempt) for a given workflow instance run.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param workflowRunId workflow instance run id
   * @return the latest step attempt states
   */
  public Map<String, StepRuntimeState> getStepStates(
      String workflowId, long workflowInstanceId, long workflowRunId, List<String> stepIds) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt =
                      conn.prepareStatement(GET_STEP_LAST_ATTEMPT_STATE_QUERY)) {
                    int idx = 0;
                    stmt.setString(++idx, workflowId);
                    stmt.setLong(++idx, workflowInstanceId);
                    stmt.setLong(++idx, workflowRunId);
                    stmt.setArray(
                        ++idx, conn.createArrayOf(ARRAY_TYPE_NAME, stepIds.toArray(new String[0])));
                    try (ResultSet result = stmt.executeQuery()) {
                      return getStringStepRuntimeStateMap(result);
                    }
                  }
                }),
        "getStepStates",
        "Failed to get steps [{}] latest attempt stats for [{}][{}][{}]",
        stepIds,
        workflowId,
        workflowInstanceId,
        workflowRunId);
  }

  private Map<String, StepRuntimeState> getStringStepRuntimeStateMap(ResultSet result)
      throws SQLException {
    Map<String, StepRuntimeState> ret = new HashMap<>();
    while (result.next()) {
      String stepId = result.getString(ID_COLUMN);
      StepRuntimeState state = fromJson(result.getString(PAYLOAD_COLUMN), StepRuntimeState.class);
      ret.put(stepId, state);
    }
    return ret;
  }

  /**
   * Get all the step dependencies (only the last attempt) for a given workflow instance run.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param workflowRunId workflow instance run id
   * @return all last step dependencies.
   */
  public Map<String, SignalDependencies> getAllStepDependencies(
      String workflowId, long workflowInstanceId, long workflowRunId) {
    Map<String, SignalDependencies> allStepDependencies = new HashMap<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_STEP_LAST_ATTEMPT_OVERALL_DEPENDENCIES_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                  stmt.setLong(++idx, workflowInstanceId);
                  stmt.setLong(++idx, workflowRunId);
                },
                result -> {
                  while (result.next()) {
                    String stepId = result.getString(StepInstanceField.STEP_ID.field);
                    SignalDependencies dependencies = getDependencies(result);
                    if (dependencies != null) {
                      allStepDependencies.put(stepId, dependencies);
                    }
                  }
                  return allStepDependencies;
                }),
        "getAllStepDependencies",
        "Failed to get the step dependencies for [{}][{}][{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId);
  }

  /**
   * Get step ids to its status mapping from DB for previous run ids of a workflow instance, i.e.
   * run_id less than the current run_id. It only considers the latest run's latest step attempt.
   * For example, in run_1, step_A is failed, and in run_2, step_A is restarted and succeeded. Then
   * in the returned result, step_A is considered as succeeded.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @return step ids to its status mapping
   */
  public Map<String, StepInstance.Status> getAllLatestStepStatusFromRuns(
      String workflowId, long workflowInstanceId) {
    Map<String, StepInstance.Status> stepStatus = new HashMap<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_ALL_LATEST_ATTEMPT_STEP_STATUS_QUERY,
                stmt -> {
                  stmt.setString(1, workflowId);
                  stmt.setLong(2, workflowInstanceId);
                },
                result -> {
                  while (result.next()) {
                    stepStatus.put(
                        result.getString(ID_COLUMN),
                        StepInstance.Status.create(result.getString(STATUS_COLUMN)));
                  }
                  return stepStatus;
                }),
        "getAllLatestStepStatusFromAncestors",
        "Failed to get the step ids to latest status mapping from workflow instance [{}][{}]",
        workflowId,
        workflowInstanceId);
  }

  /**
   * Get step ids to its instances mapping from DB for ancestor run ids of a workflow instance run,
   * i.e. run_id less than the current run_id. It only considers the latest run's latest step
   * attempt.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepIds step ids to consider
   * @return step ids to its latest step attempt mapping
   */
  public Map<String, StepInstance> getAllLatestStepFromAncestors(
      String workflowId, long workflowInstanceId, Collection<String> stepIds) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt =
                      conn.prepareStatement(GET_ALL_LATEST_ATTEMPT_STEP_QUERY)) {
                    int idx = 0;
                    stmt.setString(++idx, workflowId);
                    stmt.setLong(++idx, workflowInstanceId);
                    stmt.setArray(
                        ++idx, conn.createArrayOf(ARRAY_TYPE_NAME, stepIds.toArray(new String[0])));
                    try (ResultSet result = stmt.executeQuery()) {
                      return getStepIdInstanceMap(result);
                    }
                  }
                }),
        "getAllLatestStepFromAncestors",
        "Failed to get steps [{}] latest attempt for [{}][{}]",
        stepIds,
        workflowId,
        workflowInstanceId);
  }

  private Map<String, StepInstance> getStepIdInstanceMap(ResultSet result) throws SQLException {
    Map<String, StepInstance> ret = new HashMap<>();
    while (result.next()) {
      StepInstance stepInstance = maestroStepFromResult(result);
      ret.put(stepInstance.getStepId(), stepInstance);
    }
    return ret;
  }

  /** Get the latest run's subworkflow step artifact from DB for a given instance. */
  public SubworkflowArtifact getLatestSubworkflowArtifact(
      String workflowId, long workflowInstanceId, String stepId) {
    Artifact artifact =
        getLatestArtifact(workflowId, workflowInstanceId, stepId, Artifact.Type.SUBWORKFLOW.key());
    return artifact != null ? artifact.asSubworkflow() : null;
  }

  /** Get the latest run's foreach step artifact from DB for a given instance. */
  public ForeachArtifact getLatestForeachArtifact(
      String workflowId, long workflowInstanceId, String stepId) {
    Artifact artifact =
        getLatestArtifact(workflowId, workflowInstanceId, stepId, Artifact.Type.FOREACH.key());
    return artifact != null ? artifact.asForeach() : null;
  }

  /** Get the latest run's step artifact from DB for a given instance. */
  private Artifact getLatestArtifact(
      String workflowId, long workflowInstanceId, String stepId, String artifactName) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                String.format(GET_LATEST_ARTIFACT_QUERY_TEMPLATE, artifactName, artifactName),
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                  stmt.setLong(++idx, workflowInstanceId);
                  stmt.setString(++idx, stepId);
                },
                result -> {
                  if (result.next()) {
                    return fromJson(result.getString(PAYLOAD_COLUMN), Artifact.class);
                  }
                  return null;
                }),
        "getLatestArtifact",
        "Failed to get the latest artifact from workflow instance [{}][{}]'s step [{}]",
        workflowId,
        workflowInstanceId,
        stepId);
  }

  /**
   * Get step instance view.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepId step id
   * @return the step instance view
   */
  public StepInstance getStepInstanceView(
      String workflowId, long workflowInstanceId, String stepId) {
    StepInstance ret =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    GET_STEP_INSTANCE_VIEW_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, workflowInstanceId);
                      stmt.setString(++idx, stepId);
                    },
                    result -> {
                      if (result.next()) {
                        return maestroStepFromResult(result);
                      }
                      return null;
                    }),
            "getStepInstanceView",
            "Failed to get the step instance view for [{}][{}][{}]",
            workflowId,
            workflowInstanceId,
            stepId);
    if (ret == null) {
      throw new MaestroNotFoundException(
          "step instance view for [%s][%s][%s] not found (either not created or deleted)",
          workflowId, workflowInstanceId, stepId);
    }
    return ret;
  }

  /**
   * Get step instance state list that contains view from all attempts of all runs for a given
   * workflow instance. Limits to top 100-step attempts.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepId step id
   * @return step instance state list
   */
  public List<StepAttemptState> getStepAttemptStates(
      String workflowId, long workflowInstanceId, String stepId) {
    List<StepAttemptState> ret =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    GET_STEP_ATTEMPT_STATE_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, workflowInstanceId);
                      stmt.setString(++idx, stepId);
                    },
                    this::stepAttemptStateFromResult),
            "getStepAttemptStateView",
            "Failed to get the step attempt state view for [{}][{}][{}]",
            workflowId,
            workflowInstanceId,
            stepId);
    if (ret == null || ret.isEmpty()) {
      throw new MaestroNotFoundException(
          "step attempt state view for [%s][%s][%s] not found (either not created or deleted)",
          workflowId, workflowInstanceId, stepId);
    }
    return ret;
  }

  /**
   * Get the parameter's type from the step in a foreach. If null, it throws an exception. Note that
   * This method accepts input from users and PreparedStatement is used here to make it free of SQL
   * injection attack. Additionally, it provides a faster execution due to pre compilation and
   * DB-side caching.
   */
  public ParamType getForeachParamType(
      String foreachInlineWorkflowId, String stepId, String paramName) {
    String ret =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    GET_PARAM_TYPE_FROM_FOREACH_TEMPLATE,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, paramName);
                      stmt.setString(++idx, foreachInlineWorkflowId);
                      stmt.setString(++idx, stepId);
                    },
                    result -> {
                      if (result.next()) {
                        return result.getString(PAYLOAD_COLUMN);
                      }
                      return null;
                    }),
            "getForeachParamType",
            "Failed to get the param type for [{}] from foreach step [{}][{}]",
            paramName,
            foreachInlineWorkflowId,
            stepId);
    if (ret == null) {
      throw new MaestroNotFoundException(
          "Parameter [%s] for foreach step [%s][%s] not found (either not created or deleted)",
          paramName, foreachInlineWorkflowId, stepId);
    }
    return ParamType.create(ret);
  }

  /**
   * Get the parameter's evaluate results as raw string from the step in a foreach. Note that This
   * method accepts input from users and PreparedStatement is used here to make it free of SQL
   * injection attack. Additionally, it provides a faster execution due to pre compilation and
   * DB-side caching.
   */
  public Map<Long, String> getEvaluatedResultsFromForeach(
      String foreachInlineWorkflowId, String stepId, String paramName) {
    Map<Long, String> idParams = new HashMap<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_EVALUATED_RESULTS_FROM_FOREACH_TEMPLATE,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, paramName);
                  stmt.setString(++idx, foreachInlineWorkflowId);
                  stmt.setString(++idx, stepId);
                },
                result -> {
                  while (result.next()) {
                    String val = result.getString(PAYLOAD_COLUMN);
                    if (val != null) {
                      idParams.put(result.getLong(ID_COLUMN), val);
                    }
                  }
                  return idParams;
                }),
        "getEvaluatedResultsFromForeach",
        "Failed to get the evaluated results of param [{}] from foreach step [{}][{}]",
        paramName,
        foreachInlineWorkflowId,
        stepId);
  }

  /** Get the next unique id by calling CRDB unique_rowid() method. */
  public Long getNextUniqueId() {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_UNIQUE_ROWID,
                stmt -> {},
                result -> {
                  if (result.next()) {
                    return result.getLong(ID_COLUMN);
                  }
                  throw new MaestroNotFoundException("crdb unique_rowid() does not return an id");
                }),
        "getNextUniqueId",
        "Failed to get the next unique id");
  }

  /**
   * Get the latest step attempts of all the steps for a given workflow instance run (workflow id,
   * instance id, run id).
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param workflowRunId workflow run id
   * @return latest step attempts of all the steps
   */
  public List<StepInstance> getStepInstanceViews(
      String workflowId, long workflowInstanceId, long workflowRunId) {
    List<StepInstance> instances = new ArrayList<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_STEP_INSTANCE_VIEWS_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                  stmt.setLong(++idx, workflowInstanceId);
                  stmt.setLong(++idx, workflowRunId);
                },
                result -> {
                  while (result.next()) {
                    instances.add(maestroStepFromResult(result));
                  }
                  return instances;
                }),
        "getStepInstanceViews",
        "Failed to get latest step attempts for workflow instance [{}][{}][{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId);
  }
}
