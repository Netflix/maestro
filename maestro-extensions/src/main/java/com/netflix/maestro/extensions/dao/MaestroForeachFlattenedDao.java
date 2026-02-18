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
package com.netflix.maestro.extensions.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedModel;
import com.netflix.maestro.extensions.models.StepIteration;
import com.netflix.maestro.extensions.models.StepIterationsSummary;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class MaestroForeachFlattenedDao extends AbstractDatabaseDao {
  private static final String IS_INSERTED_IN_NEW_ROOT_RUN = "is_inserted_in_new_run";

  // Common SQL fragments for INSERT/UPDATE queries
  private static final String ON_CONFLICT_CLAUSE =
      "ON CONFLICT (workflow_id,workflow_instance_id,step_id,workflow_run_id,iteration_rank) ";
  private static final String WHERE_ATTEMPT_SEQ_CLAUSE =
      "WHERE (excluded.step_attempt_seq > maestro_step_foreach_flattened.step_attempt_seq) ";
  private static final String OR_STATUS_ENCODED_CLAUSE =
      "OR (excluded.step_attempt_seq = maestro_step_foreach_flattened.step_attempt_seq AND excluded.step_status_encoded > maestro_step_foreach_flattened.step_status_encoded) ";
  private static final String RETURNING_CLAUSE = "RETURNING is_inserted_in_new_run";

  private static final String INSERT_OR_UPDATE_QUERY =
      "INSERT INTO maestro_step_foreach_flattened (workflow_id,workflow_instance_id,workflow_run_id,step_id,iteration_rank,initial_step_created_ms,instance,run_id_validity_end,step_attempt_seq,loop_parameters,step_status,step_status_encoded,step_status_priority,step_runtime_state,is_inserted_in_new_run) VALUES (?,?,?,?,?,?,?::jsonb,?,?,?::jsonb,?,?,?,?::jsonb,true) "
          + ON_CONFLICT_CLAUSE
          + "DO UPDATE SET run_id_validity_end=?,step_attempt_seq=?,loop_parameters=?::jsonb,step_status=?,step_status_encoded=?, step_status_priority=? ,step_runtime_state=?::jsonb,is_inserted_in_new_run=false "
          + WHERE_ATTEMPT_SEQ_CLAUSE
          + OR_STATUS_ENCODED_CLAUSE
          + RETURNING_CLAUSE;

  private static final String TABLE_REFERENCE_INFIX_FOR_UPDATE_ROOT_RUN =
      "FROM ( SELECT workflow_id, workflow_instance_id, step_id, workflow_run_id as workflow_run_id_ref, iteration_rank "
          + "FROM maestro_step_foreach_flattened WHERE workflow_id=? AND workflow_instance_id=? AND step_id=? AND iteration_rank=? ";

  private static final String UPDATE_PREVIOUS_ROOT_RUN_QUERY =
      "UPDATE maestro_step_foreach_flattened SET run_id_validity_end=? "
          + TABLE_REFERENCE_INFIX_FOR_UPDATE_ROOT_RUN
          + "  AND workflow_run_id<? ORDER BY workflow_run_id desc LIMIT 1 "
          + ") prev_root_run "
          + "WHERE maestro_step_foreach_flattened.workflow_id=prev_root_run.workflow_id AND "
          + "maestro_step_foreach_flattened.workflow_instance_id=prev_root_run.workflow_instance_id AND "
          + "maestro_step_foreach_flattened.step_id=prev_root_run.step_id AND "
          + "maestro_step_foreach_flattened.workflow_run_id=prev_root_run.workflow_run_id_ref AND "
          + "maestro_step_foreach_flattened.iteration_rank=prev_root_run.iteration_rank";
  private static final String UPDATE_CURRENT_ROOT_RUN_QUERY =
      "UPDATE maestro_step_foreach_flattened SET run_id_validity_end=next_root_run.workflow_run_id_ref "
          + TABLE_REFERENCE_INFIX_FOR_UPDATE_ROOT_RUN
          + "  AND workflow_run_id>? ORDER BY workflow_run_id asc LIMIT 1 "
          + ") next_root_run "
          + "WHERE maestro_step_foreach_flattened.workflow_id=next_root_run.workflow_id AND "
          + "maestro_step_foreach_flattened.workflow_instance_id=next_root_run.workflow_instance_id AND "
          + "maestro_step_foreach_flattened.step_id=next_root_run.step_id AND "
          + "maestro_step_foreach_flattened.workflow_run_id=? AND "
          + "maestro_step_foreach_flattened.iteration_rank=next_root_run.iteration_rank";

  private static final TypeReference<Map<String, String>> TYPE_REF =
      new TypeReference<Map<String, String>>() {};

  private static final TypeReference<Map<String, Object>> TYPE_REF_OBJ =
      new TypeReference<Map<String, Object>>() {};

  private static final String COMMON_WHERE_CLAUSE =
      " where workflow_id = ? "
          + "  and step_id = ? "
          + "  and workflow_instance_id = ? "
          + "  and workflow_run_id <= ? "
          + "  and run_id_validity_end > ? ";
  private static final String ALWAYS_TRUE = "and 1=1";
  private static final String LOOP_PARAMETER_FILTER_TEMPLATE = "and loop_parameters->>'%s' = '%s'";
  private static final String STATUS_FILTER_TEMPLATE = "and step_status in (%s)";

  private static final String STEP_ITERATION_QUERY =
      "select workflow_run_id, iteration_rank, loop_parameters, step_attempt_seq, step_runtime_state"
          + " from maestro_step_foreach_flattened"
          + COMMON_WHERE_CLAUSE
          + " and iteration_rank = ?";

  private static final String STEP_STATUS_QUERY =
      "select  step_status, count(*) as cnt from maestro_step_foreach_flattened "
          + COMMON_WHERE_CLAUSE
          + "group by step_status";

  private static final String REPRESENTATIVE_STEP_ITERATION_QUERY =
      "select workflow_run_id, iteration_rank, loop_parameters, step_attempt_seq, step_runtime_state "
          + "from maestro_step_foreach_flattened "
          + COMMON_WHERE_CLAUSE
          + "order by step_status_priority desc, iteration_rank COLLATE \"C\" asc limit 1";

  private static final String SAMPLE_LOOP_PARAMETER_QUERY =
      "select loop_parameters, iteration_rank from maestro_step_foreach_flattened "
          + COMMON_WHERE_CLAUSE
          + "order by iteration_rank COLLATE \"C\" asc limit 100"; // only 100 items needed for the
  // sampling

  private static final String QUOTE = "'";
  private static final String DELIMITER = "','";
  private static final String LOOP_PARAMETERS = "loop_parameters";
  private static final String MIMIMUM_ITERATION_RANK = "0";
  private static final String LARGEST_ITERATION_RANK = "~"; // last ASCII normal character.

  public MaestroForeachFlattenedDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  public void insertOrUpdateForeachFlattenedModel(ForeachFlattenedModel model) {
    final String instanceStr = toJson(model.getInstance());
    final String loopParametersStr = toJson(model.getLoopParameters());
    final String stepRuntimeStateStr = toJson(model.getStepRuntimeState());

    // Note: SERIALIZABLE isolation IS required here for Postgres. While the UPSERT itself
    // is atomic, the subsequent UPDATE_PREVIOUS_ROOT_RUN and UPDATE_CURRENT_ROOT_RUN queries
    // read other rows (via FROM subquery) to determine which row to update. Under READ COMMITTED,
    // concurrent inserts of adjacent run_ids can interleave such that the subquery sees stale data,
    // corrupting the validity_end chain and causing duplicate rows in queries.
    withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  markTransactionSerializable(conn, false);
                  try (PreparedStatement stmt = conn.prepareStatement(INSERT_OR_UPDATE_QUERY)) {
                    int idx = 0;

                    // INSERT parameters
                    stmt.setString(++idx, model.getInstance().getWorkflowId());
                    stmt.setLong(++idx, model.getInstance().getWorkflowInstanceId());
                    stmt.setLong(++idx, model.getInstance().getWorkflowRunId());
                    stmt.setString(++idx, model.getInstance().getStepId());
                    stmt.setString(++idx, model.getInstance().getIterationRank());
                    stmt.setLong(++idx, model.getInstance().getInitialStepCreatedMs());
                    stmt.setString(++idx, instanceStr);
                    stmt.setLong(++idx, model.getRunIdValidityEnd());
                    stmt.setString(++idx, model.getStepAttemptSeq());
                    stmt.setString(++idx, loopParametersStr);
                    stmt.setString(++idx, model.getStepRuntimeState().getStatus().toString());
                    stmt.setLong(++idx, model.getStepStatusEncoded());
                    stmt.setLong(++idx, model.getStatusPriority());
                    stmt.setString(++idx, stepRuntimeStateStr);

                    // UPDATE parameters
                    stmt.setLong(++idx, model.getRunIdValidityEnd());
                    stmt.setString(++idx, model.getStepAttemptSeq());
                    stmt.setString(++idx, loopParametersStr);
                    stmt.setString(++idx, model.getStepRuntimeState().getStatus().toString());
                    stmt.setLong(++idx, model.getStepStatusEncoded());
                    stmt.setLong(++idx, model.getStatusPriority());
                    stmt.setString(++idx, stepRuntimeStateStr);
                    try (ResultSet result = stmt.executeQuery()) {
                      if (result.next()) {
                        boolean isInsertedInNewRun = result.getBoolean(IS_INSERTED_IN_NEW_ROOT_RUN);
                        if (isInsertedInNewRun) {
                          if (model.getInstance().getWorkflowRunId() > 1) {
                            // Made assumption that run id starts from 1, so we save DB call to
                            // update previous run here
                            updateRootRunIdValidityEndPreviousRun(conn, model);
                          }
                          updateRootRunIdValidityEndCurrentRun(conn, model);
                        }
                      }
                    }
                  }
                  return null;
                }),
        "insertOrUpdateForeachFlattenedModel",
        "Failed to insert or update foreach flattened model");
  }

  private void updateRootRunIdValidityEndPreviousRun(Connection conn, ForeachFlattenedModel model)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UPDATE_PREVIOUS_ROOT_RUN_QUERY)) {
      int idx = 0;
      stmt.setLong(++idx, model.getInstance().getWorkflowRunId());
      stmt.setString(++idx, model.getInstance().getWorkflowId());
      stmt.setLong(++idx, model.getInstance().getWorkflowInstanceId());
      stmt.setString(++idx, model.getInstance().getStepId());
      stmt.setString(++idx, model.getInstance().getIterationRank());
      stmt.setLong(++idx, model.getInstance().getWorkflowRunId());
      stmt.executeUpdate();
    }
  }

  private void updateRootRunIdValidityEndCurrentRun(Connection conn, ForeachFlattenedModel model)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UPDATE_CURRENT_ROOT_RUN_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, model.getInstance().getWorkflowId());
      stmt.setLong(++idx, model.getInstance().getWorkflowInstanceId());
      stmt.setString(++idx, model.getInstance().getStepId());
      stmt.setString(++idx, model.getInstance().getIterationRank());
      stmt.setLong(++idx, model.getInstance().getWorkflowRunId());
      stmt.setLong(++idx, model.getInstance().getWorkflowRunId());
      stmt.executeUpdate();
    }
  }

  /**
   * Get a single step iteration for given iteration rank.
   *
   * @param workflowId id of workflow this step belongs to.
   * @param workflowInstanceId id of workflow instance.
   * @param workflowRunId run id of workflow instance.
   * @param stepId id of the step.
   * @param iterationRank encoded iteration id to locate step iteration.
   * @return found step iteration, null otherwise.
   */
  public StepIteration getStepIteration(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String iterationRank) {

    return withMetricLogError(
        () ->
            withReadOnlyQuery(
                STEP_ITERATION_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, workflowId);
                  stmt.setString(++idx, stepId);
                  stmt.setLong(++idx, workflowInstanceId);
                  stmt.setLong(++idx, workflowRunId);
                  stmt.setLong(++idx, workflowRunId);
                  stmt.setString(++idx, iterationRank);
                },
                result -> {
                  StepIteration stepIteration = null;
                  while (result.next()) {
                    stepIteration =
                        getStepIterationFromResultSet(
                            workflowId, workflowInstanceId, stepId, result);
                  }
                  return stepIteration;
                }),
        "getStepIteration",
        "Failed get the stepIteration workflowId:[{}], workflowInstanceId: [{}], workflowRunId: [{}], "
            + "stepId: [{}], iterationRank: [{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        iterationRank);
  }

  /**
   * Scans the stepIterations.
   *
   * @param workflowId
   * @param workflowInstanceId
   * @param workflowRunId
   * @param stepId
   * @param iterationRank
   * @param limit
   * @return
   */
  public List<StepIteration> scanStepIterations(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      @Nullable String iterationRank,
      long limit,
      boolean isForward,
      Map<String, String> loopParamsFilter,
      List<String> statuses) {
    String scanQueryTemplate = getScanQueryTemplate();
    String query =
        isForward
            ? String.format(
                scanQueryTemplate,
                ">",
                getLoopParamQuery(loopParamsFilter),
                getStatusQuery(statuses),
                "ASC")
            : String.format(
                scanQueryTemplate,
                "<",
                getLoopParamQuery(loopParamsFilter),
                getStatusQuery(statuses),
                "DESC");
    List<StepIteration> ret = new ArrayList<>();
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement ps = conn.prepareStatement(query)) {
                    int idx = 0;
                    ps.setString(++idx, workflowId);
                    ps.setString(++idx, stepId);
                    ps.setLong(++idx, workflowInstanceId);
                    ps.setLong(++idx, workflowRunId);
                    ps.setLong(++idx, workflowRunId);
                    if (iterationRank == null) {
                      ps.setString(
                          ++idx, isForward ? MIMIMUM_ITERATION_RANK : LARGEST_ITERATION_RANK);
                    } else {
                      ps.setString(++idx, iterationRank);
                    }
                    ps.setLong(++idx, limit);

                    try (ResultSet rs = ps.executeQuery()) {
                      while (rs.next()) {
                        ret.add(
                            getStepIterationFromResultSet(
                                workflowId, workflowInstanceId, stepId, rs));
                      }
                    }
                    return ret;
                  }
                }),
        "scanStepIterations",
        "Failed scan stepIterations workflowId:[{}], workflowInstanceId: [{}], workflowRunId: [{}], stepId: [{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId);
  }

  /**
   * Returns the summary for all the step iterations qualifying in the flattened view.
   *
   * @param workflowId
   * @param workflowInstanceId
   * @param workflowRunId
   * @param stepId
   * @return
   */
  public StepIterationsSummary getStepIterationsSummary(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      boolean includeStatsCount,
      boolean includeRepresentative,
      long loopValueCount) {

    StepIterationsSummary stepIterationsSummary = new StepIterationsSummary();
    // Note: SERIALIZABLE isolation IS required here for Postgres. This transaction executes
    // multiple queries (status counts, representative iteration, loop params) that must see a
    // consistent view of the data. Under READ COMMITTED, concurrent upserts could cause
    // inconsistent results (e.g., status counts showing only succeeded/running while
    // representative iteration shows failed).
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  markTransactionSerializable(conn, true);
                  if (includeStatsCount) {
                    try (PreparedStatement ps = conn.prepareStatement(STEP_STATUS_QUERY)) {
                      setPreparedStatementCommon(
                          ps, workflowId, workflowInstanceId, workflowRunId, stepId);
                      try (ResultSet rs = ps.executeQuery()) {
                        Map<StepInstance.Status, Long> counts =
                            new EnumMap<>(StepInstance.Status.class);
                        while (rs.next()) {
                          counts.put(
                              StepInstance.Status.create(rs.getString("step_status")),
                              rs.getLong("cnt"));
                        }
                        stepIterationsSummary.setCountByStatus(counts);
                      }
                    }
                  }
                  if (includeRepresentative) {
                    try (PreparedStatement ps =
                        conn.prepareStatement(REPRESENTATIVE_STEP_ITERATION_QUERY)) {
                      setPreparedStatementCommon(
                          ps, workflowId, workflowInstanceId, workflowRunId, stepId);
                      try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                          stepIterationsSummary.setRepresentativeIteration(
                              getStepIterationFromResultSet(
                                  workflowId, workflowInstanceId, stepId, rs));
                        }
                      }
                    }
                  }
                  if (loopValueCount > 0) {
                    try (PreparedStatement ps =
                        conn.prepareStatement(SAMPLE_LOOP_PARAMETER_QUERY)) {
                      setPreparedStatementCommon(
                          ps, workflowId, workflowInstanceId, workflowRunId, stepId);
                      try (ResultSet rs = ps.executeQuery()) {
                        List<Map<String, Object>> loopParameterHolder = new ArrayList<>();
                        while (rs.next()) {
                          loopParameterHolder.add(
                              fromJson(rs.getString(LOOP_PARAMETERS), TYPE_REF_OBJ));
                        }
                        Map<String, Set<String>> loopParams = new HashMap<>();
                        Map<String, List<String>> loopParamValues = new HashMap<>();
                        loopParameterHolder.stream()
                            .flatMap(l -> l.entrySet().stream())
                            .forEach(
                                e -> {
                                  String name = e.getKey();
                                  String value = e.getValue().toString();
                                  loopParams.putIfAbsent(name, new HashSet<>());
                                  loopParamValues.putIfAbsent(name, new ArrayList<>());
                                  Set<String> valueSet = loopParams.get(name);
                                  List<String> toAppend = loopParamValues.get(name);
                                  if (valueSet.add(value) && toAppend.size() < loopValueCount) {
                                    toAppend.add(value);
                                  }
                                });
                        stepIterationsSummary.setLoopParamValues(
                            loopParamValues.entrySet().stream()
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                      }
                    }
                  }

                  return stepIterationsSummary;
                }),
        "getStatusSummary",
        "Failed get status summary for workflowId:[{}], workflowInstanceId: [{}], workflowRunId: [{}], stepId: [{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId);
  }

  private StepIteration getStepIterationFromResultSet(
      String workflowId, long workflowInstanceId, String stepId, ResultSet rs) throws SQLException {
    return StepIteration.create(
        workflowId,
        workflowInstanceId,
        rs.getLong("workflow_run_id"),
        stepId,
        rs.getString("iteration_rank"),
        rs.getString("step_attempt_seq"),
        fromJson(rs.getString(LOOP_PARAMETERS), TYPE_REF),
        fromJson(rs.getString("step_runtime_state"), StepRuntimeState.class));
  }

  private void setPreparedStatementCommon(
      PreparedStatement ps,
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId)
      throws SQLException {
    int idx = 0;
    ps.setString(++idx, workflowId);
    ps.setString(++idx, stepId);
    ps.setLong(++idx, workflowInstanceId);
    ps.setLong(++idx, workflowRunId);
    ps.setLong(++idx, workflowRunId);
  }

  /** Returns the scan query template for Postgres (no CRDB index hints). */
  private String getScanQueryTemplate() {
    return "select *"
        + " from (select workflow_run_id, iteration_rank, loop_parameters, step_attempt_seq, step_runtime_state"
        + "      from maestro_step_foreach_flattened"
        + COMMON_WHERE_CLAUSE
        + "        and iteration_rank COLLATE \"C\" %s ? COLLATE \"C\""
        + "     %s %s"
        + "      order by iteration_rank COLLATE \"C\" %s"
        + "      limit ?)"
        + " order by iteration_rank COLLATE \"C\" asc";
  }

  @VisibleForTesting
  String getLoopParamQuery(Map<String, String> loopParams) {
    if (loopParams == null || loopParams.isEmpty()) {
      return ALWAYS_TRUE; // always true condition
    }
    StringBuilder ret = new StringBuilder();
    loopParams.forEach((k, v) -> ret.append(String.format(LOOP_PARAMETER_FILTER_TEMPLATE, k, v)));
    return ret.toString();
  }

  String getStatusQuery(List<String> statuses) {
    if (statuses == null || statuses.isEmpty()) {
      return ALWAYS_TRUE;
    }
    return String.format(
        STATUS_FILTER_TEMPLATE,
        new StringBuilder(QUOTE).append(String.join(DELIMITER, statuses)).append(QUOTE));
  }
}
