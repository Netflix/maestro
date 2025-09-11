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
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.engine.steps.ForeachStepRuntime;
import com.netflix.maestro.engine.steps.WhileStepRuntime;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WhileStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.stepruntime.PausedStepAttempt;
import com.netflix.maestro.models.stepruntime.StepBreakpoint;
import com.netflix.maestro.utils.IdHelper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * StepBreakpointDao to store user added breakpoints which are defined using step identifiers.
 *
 * <p>Breakpoints can be added in layered manner and all the following is supported.
 *
 * <p>(1) <WF_ID,STEP_ID,MATCH_ALL_VERSION,MATCH_ALL_INSTANCES,MATCH_ALL_RUNS,MATCH_ALL_ATTEMPTS>
 * breakpoint for step definition.
 *
 * <p>(2) <WF_ID,STEP_ID,SPECIFIC_VERSION,MATCH_ALL_INSTANCES,MATCH_ALL_RUNS,MATCH_ALL_ATTEMPTS>
 * breakpoint for step definition for specific workflow version.
 *
 * <p>(3) <WF_ID,STEP_ID,SPECIFIC_VERSION,SPECIFIC_INSTANCE,MATCH_ALL_RUNS,MATCH_ALL_ATTEMPTS>
 * breakpoint for specific step instance for all runs and attempts.
 *
 * <p>(4) <WF_ID,STEP_ID,SPECIFIC_VERSION,SPECIFIC_INSTANCE,SPECIFIC_RUN,MATCH_ALL_ATTEMPTS>
 * breakpoint for specific step instance run and all attempts.
 *
 * <p>(5) <WF_ID,STEP_ID,SPECIFIC_VERSION,SPECIFIC_INSTANCE,SPECIFIC_RUN,SPECIFIC_ATTEMPT>
 * breakpoint for specific step instance attempt.
 *
 * <p>Note: The following is not supported for e.g:
 * <WF_ID,STEP_ID,MATCH_ALL_VERSIONS,MATCH_ALL_INSTANCES,MATCH_ALL_RUNS,SPECIFIC_ATTEMPT> i.e. add
 * breakpoint for a specific step attempt for all versions,instances and runs and similarly other
 * combinations.
 *
 * <p>Store paused step attempts which additional column ("system_generated"=true) since the schema
 * of paused step attempt is same as that of breakpoint.
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
@Slf4j
public class MaestroStepBreakpointDao extends AbstractDatabaseDao {
  private static final String WORKFLOW_ID = "workflow_id";
  private static final String VERSION = "version";
  private static final String INSTANCE_ID = "instance_id";
  private static final String RUN_ID = "run_id";
  private static final String STEP_ID = "step_id";
  private static final String STEP_ATTEMPT_ID = "step_attempt_id";
  private static final String CREATE_TS = "create_ts";
  private static final String CONDITION_BY_DEFINITION_IDS = " AND workflow_id=? AND STEP_ID=?";
  private static final String CONDITION_BY_INTERNAL_WORKFLOW_ID_RANGES_STEP_ID =
      " AND workflow_id >= ? AND workflow_id < ? AND step_id = ?";
  private static final String ENTRY_LIMIT = " LIMIT ?";
  private static final String CONDITION_BY_SPECIFIC_OR_MATCH_ALL_VERSIONS =
      " AND (version=? OR version=0)";

  private static final String CONDITION_BY_SPECIFIC_OR_MATCH_ALL_INSTANCES =
      " AND (instance_id=? OR instance_id=0)";

  private static final String CONDITION_BY_SPECIFIC_OR_MATCH_ALL_RUNS =
      " AND (run_id=? OR run_id=0)";

  private static final String CONDITION_BY_SPECIFIC_OR_MATCH_ALL_STEP_INSTANCE_ATTEMPT_IDS =
      " AND (step_attempt_id=? OR step_attempt_id=0)";

  private static final String CONDITION_BY_SPECIFIC_VERSION = " AND version=?";

  private static final String CONDITION_BY_SPECIFIC_INSTANCE = " AND instance_id=?";

  private static final String CONDITION_BY_SPECIFIC_RUN = " AND run_id=?";

  private static final String CONDITION_BY_SPECIFIC_ATTEMPT_ID = " AND step_attempt_id=?";

  private static final String CONDITION_BY_ALL_STEP_IDENTIFIERS =
      " AND workflow_id=? AND version=? AND instance_id=? AND run_id=? AND step_id=? AND step_attempt_id=?";

  private static final String CONDITION_STEP_BREAKPOINT = " system_generated=FALSE ";

  private static final String CONDITION_PAUSED_STEP_ATTEMPT = " system_generated=TRUE ";

  private static final String CONDITION_BY_STEP_BREAKPOINT_IDENTIFIERS =
      CONDITION_STEP_BREAKPOINT + CONDITION_BY_ALL_STEP_IDENTIFIERS;

  private static final String CONDITION_BY_PAUSED_STEP_ATTEMPT_IDENTIFIERS =
      CONDITION_PAUSED_STEP_ATTEMPT + CONDITION_BY_ALL_STEP_IDENTIFIERS;

  private static final String ADD_STEP_BREAKPOINT =
      "INSERT INTO maestro_step_breakpoint (workflow_id,version,instance_id,run_id,step_id,step_attempt_id,"
          + "created_by,create_ts,system_generated) VALUES (?,?,?,?,?,?,?::json,CURRENT_TIMESTAMP,false) "
          + "ON CONFLICT (workflow_id, step_id, system_generated, version, instance_id, run_id, step_attempt_id) "
          + "DO UPDATE SET workflow_id=EXCLUDED.workflow_id,version=EXCLUDED.version,instance_id=EXCLUDED.instance_id,"
          + "run_id=EXCLUDED.run_id,step_id=EXCLUDED.step_id,step_attempt_id=EXCLUDED.step_attempt_id,"
          + "created_by=EXCLUDED.created_by,create_ts=CURRENT_TIMESTAMP,system_generated=false RETURNING *";

  private static final String SELECT_QUERY_PREFIX = "SELECT * FROM maestro_step_breakpoint WHERE";

  private static final String DELETE_QUERY_PREFIX = "DELETE FROM maestro_step_breakpoint WHERE";

  private static final String GET_STEP_BREAKPOINT_BASE_QUERY =
      SELECT_QUERY_PREFIX + CONDITION_STEP_BREAKPOINT + CONDITION_BY_DEFINITION_IDS;

  private static final String GET_PAUSED_STEP_ATTEMPT_BASE_QUERY =
      SELECT_QUERY_PREFIX + CONDITION_PAUSED_STEP_ATTEMPT + CONDITION_BY_DEFINITION_IDS;

  private static final String GET_PAUSED_INLINE_WORKFLOW_STEP_ATTEMPT_BASE_QUERY =
      SELECT_QUERY_PREFIX
          + CONDITION_PAUSED_STEP_ATTEMPT
          + CONDITION_BY_INTERNAL_WORKFLOW_ID_RANGES_STEP_ID;

  private static final String DELETE_STEP_BREAKPOINT_QUERY =
      DELETE_QUERY_PREFIX + CONDITION_BY_STEP_BREAKPOINT_IDENTIFIERS;

  private static final String DELETE_LIMIT_BY =
      " (workflow_id, step_id, system_generated, version, instance_id, run_id, step_attempt_id) IN ("
          + "SELECT workflow_id, step_id, system_generated, version, instance_id, run_id, step_attempt_id "
          + "FROM maestro_step_breakpoint WHERE ";

  private static final String DELETE_PAUSED_STEP_ATTEMPT_BASE_QUERY =
      DELETE_QUERY_PREFIX
          + DELETE_LIMIT_BY
          + CONDITION_PAUSED_STEP_ATTEMPT
          + CONDITION_BY_DEFINITION_IDS;

  private static final String DELETE_PAUSED_INLINE_WORKFLOW_STEP_ATTEMPT_BASE_QUERY =
      DELETE_QUERY_PREFIX
          + DELETE_LIMIT_BY
          + CONDITION_PAUSED_STEP_ATTEMPT
          + CONDITION_BY_INTERNAL_WORKFLOW_ID_RANGES_STEP_ID;

  private static final String CREATE_PAUSED_STEP_INSTANCE_ATTEMPT =
      "INSERT INTO maestro_step_breakpoint (system_generated,workflow_id,version,instance_id,run_id,step_id,"
          + "step_attempt_id,create_ts) VALUES (true,?,?,?,?,?,?,CURRENT_TIMESTAMP)";

  private static final String CHECK_PAUSED_STEP_ATTEMPT =
      "SELECT 1 FROM maestro_step_breakpoint WHERE " + CONDITION_BY_PAUSED_STEP_ATTEMPT_IDENTIFIERS;

  private final MaestroWorkflowDao workflowDao;

  private final Supplier<Integer> batchDeletionLimitSupplier;

  /**
   * Constructor for Maestro step instance DAO.
   *
   * @param dataSource database data source
   * @param objectMapper object mapper
   * @param config configuration
   */
  public MaestroStepBreakpointDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroWorkflowDao workflowDao,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
    this.workflowDao = workflowDao;
    this.batchDeletionLimitSupplier = () -> Constants.BATCH_DELETION_LIMIT;
  }

  // for unit test
  MaestroStepBreakpointDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroWorkflowDao workflowDao,
      Supplier<Integer> batchDeletionLimitSupplier,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
    this.workflowDao = workflowDao;
    this.batchDeletionLimitSupplier = batchDeletionLimitSupplier;
  }

  /**
   * Insert step breakpoint.
   *
   * @param workflowId workflow id for the breakpoint.
   * @param version workflow version for the breakpoint. Special value to denote match all ${@link
   *     Constants#MATCH_ALL_WORKFLOW_VERSIONS}
   * @param instanceId workflow instanceId for the breakpoint. Special value to denote match all
   *     ${@link Constants#MATCH_ALL_WORKFLOW_INSTANCES}
   * @param runId workflow runId for the breakpoint. Special value to denote match all ${@link
   *     Constants#MATCH_ALL_RUNS}
   * @param stepId identifier for the breakpoint.
   * @param stepAttemptId stepAttemptId for the breakpoint. Special value to denote match all
   *     ${@link Constants#MATCH_ALL_STEP_ATTEMPTS}
   * @param user user who created the breakpoint.
   * @return StepBreakpoint.
   */
  public StepBreakpoint addStepBreakpoint(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId,
      User user) {
    final String revisedWorkflowId = getRevisedWorkflowId(workflowId, stepId, true);
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt = conn.prepareStatement(ADD_STEP_BREAKPOINT)) {
                    int idx = 0;
                    stmt.setString(++idx, revisedWorkflowId);
                    stmt.setLong(++idx, version);
                    stmt.setLong(++idx, instanceId);
                    stmt.setLong(++idx, runId);
                    stmt.setString(++idx, stepId);
                    stmt.setLong(++idx, stepAttemptId);
                    stmt.setString(++idx, toJson(user));
                    try (ResultSet rs = stmt.executeQuery()) {
                      if (rs.next()) {
                        return stepBreakpointFromResultSet(rs);
                      } else {
                        throw new MaestroBadRequestException(
                            Collections.emptyList(),
                            "Breakpoint could not be set with identifier [%s][%d][%d][%d][%s][%d]",
                            workflowId,
                            version,
                            instanceId,
                            runId,
                            stepId,
                            stepAttemptId);
                      }
                    }
                  }
                }),
        "addStepBreakpointForStepIdentifier",
        "Failed to addStepBreakpointForStepIdentifier [{}][{}][{}][{}][{}][{}]",
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
  }

  /**
   * Get step breakpoints for step identifiers. It supports layered look up. For e.g. is user has
   * asked to provide all breakpoints for specific step attempt and there is also a generic
   * breakpoint setup for step definition, it will be returned as well.
   *
   * @param workflowId workflow id for the breakpoint.
   * @param version workflow version for the breakpoint. Special value to denote match all ${@link
   *     Constants#MATCH_ALL_WORKFLOW_VERSIONS}
   * @param instanceId workflow instanceId for the breakpoint. Special value to denote match all
   *     ${@link Constants#MATCH_ALL_WORKFLOW_INSTANCES}
   * @param runId workflow runId for the breakpoint. Special value to denote match all ${@link
   *     Constants#MATCH_ALL_RUNS}
   * @param stepId stepId identifier for the breakpoint.
   * @param stepAttemptId stepAttemptId for the breakpoint. Special value to denote match all
   *     ${@link Constants#MATCH_ALL_STEP_ATTEMPTS}
   * @return List of all the breakpoints.
   */
  public List<StepBreakpoint> getStepBreakPoints(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId) {
    final String revisedWorkflowId = getRevisedWorkflowId(workflowId, stepId, false);
    List<StepBreakpoint> stepBreakpoints =
        withMetricLogError(
            () ->
                withRetryableTransaction(
                    conn ->
                        getStepBreakPointsWithConn(
                            conn,
                            revisedWorkflowId,
                            version,
                            instanceId,
                            runId,
                            stepId,
                            stepAttemptId)),
            "getStepBreakPoints",
            "Failed to getStepBreakPoints [{}][{}][{}][{}][{}][{}]",
            workflowId,
            version,
            instanceId,
            runId,
            stepId,
            stepAttemptId);

    if (!revisedWorkflowId.equals(workflowId)) {
      stepBreakpoints =
          stepBreakpoints.stream()
              .map(breakpoint -> breakpoint.toBuilder().workflowId(workflowId).build())
              .collect(Collectors.toList());
    }
    return stepBreakpoints;
  }

  /**
   * Remove a specific breakpoint. Also remove all step attempts which are paused due to the
   * breakpoint if request specified. If the number of paused step attempts is large, it may take a
   * long time to finish the deletion process. The total number of paused step attempts is bounded
   * by {@link #batchDeletionLimitSupplier}.
   *
   * <p>A step attempt could be paused due to multiple breakpoints. Right now we remove the paused
   * step attempt even when a single applicable breakpoint is removed. In future if required, we can
   * continue to keep the step paused if there are other breakpoints applicable.
   *
   * @param workflowId workflow id for the breakpoint.
   * @param version workflow version for the breakpoint. Special value to denote match all ${@link
   *     Constants#MATCH_ALL_WORKFLOW_VERSIONS}
   * @param instanceId workflow instanceId for the breakpoint. Special value to denote match all
   *     ${@link Constants#MATCH_ALL_WORKFLOW_INSTANCES}
   * @param runId workflow runId for the breakpoint. Special value to denote match all ${@link
   *     Constants#MATCH_ALL_RUNS}
   * @param stepId stepId identifier for the breakpoint.
   * @param stepAttemptId stepAttemptId for the breakpoint. Special value to denote match all
   *     ${@link Constants#MATCH_ALL_STEP_ATTEMPTS}
   * @return Total steps resumed
   */
  public int removeStepBreakpoint(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId,
      boolean resume) {
    final String revisedWorkflowId = getRevisedWorkflowId(workflowId, stepId, false);
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  int stepsResumed = 0;
                  removeStepBreakpointWithConn(
                      conn, revisedWorkflowId, version, instanceId, runId, stepId, stepAttemptId);
                  if (resume) {
                    stepsResumed =
                        removePausedStepsWithConn(
                            conn,
                            revisedWorkflowId,
                            version,
                            instanceId,
                            runId,
                            stepId,
                            stepAttemptId);
                  }
                  return stepsResumed;
                }),
        "removeBreakpoint",
        "Failed on removeBreakpoint [{}][{}][{}][{}][{}][{}]",
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
  }

  /**
   * Create a paused step attempt due to a breakpoint. This is created when a step checks for paused
   * state - {@link MaestroTask}
   *
   * @param workflowId workflow id for the paused step.
   * @param version workflow version for the paused step.
   * @param instanceId workflow instanceId for the paused step.
   * @param runId workflow runId for the paused step.
   * @param stepId stepId for the paused step.
   * @param stepAttemptId stepAttemptId for the paused step.
   */
  public void insertPausedStepInstanceAttempt(
      Connection conn,
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(CREATE_PAUSED_STEP_INSTANCE_ATTEMPT)) {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setLong(++idx, version);
      stmt.setLong(++idx, instanceId);
      stmt.setLong(++idx, runId);
      stmt.setString(++idx, stepId);
      stmt.setLong(++idx, stepAttemptId);
      stmt.executeUpdate();
    }
  }

  /**
   * Resume a paused step instance attempt.Users can choose to resume specific attempt even when
   * breakpoint is set.
   *
   * @param workflowId workflow id for the paused step.
   * @param version workflow version for the paused step.
   * @param instanceId workflow instanceId for the paused step.
   * @param runId workflow runId for the paused step.
   * @param stepId stepId for the paused step.
   * @param stepAttemptId stepAttemptId for the paused step.
   */
  public void resumePausedStepInstanceAttempt(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId) {
    withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  int totalRemoved =
                      removePausedStepsWithConn(
                          conn, workflowId, version, instanceId, runId, stepId, stepAttemptId);
                  if (totalRemoved == 0) {
                    throw new MaestroNotFoundException(
                        "No paused attempt is resumed because step attempt with identifier [%s][%d][%d][%d][%s][%d]"
                            + " is not been created yet or already resumed",
                        workflowId, version, instanceId, runId, stepId, stepAttemptId);
                  }
                  return true;
                }),
        "resumeStepInstanceAttempt",
        "Failed to resumeStepInstanceAttempt with identifier [{}][{}][{}][{}][{}][{}]",
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
  }

  /**
   * Get all step attempts paused via breakpoint identifier.
   *
   * @param workflowId workflowId identifier for the breakpoint.
   * @param version workflow version identifier for the breakpoint.
   * @param instanceId workflow instanceId identifier for the breakpoint.
   * @param runId workflow runId identifier for the breakpoint.
   * @param stepId stepId identifier for the breakpoint.
   * @param stepAttemptId stepAttemptId identifier for the breakpoint.
   * @return all the paused step attempts due to this breakpoint.
   */
  public List<PausedStepAttempt> getPausedStepAttempts(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId) {
    List<PausedStepAttempt> toReturn = new ArrayList<>();
    final String revisedWorkflowId = getRevisedWorkflowId(workflowId, stepId, false);
    final boolean isInlineWorkflow = IdHelper.isInlineWorkflowId(revisedWorkflowId);
    final String query =
        isInlineWorkflow
            ? GET_PAUSED_INLINE_WORKFLOW_STEP_ATTEMPT_BASE_QUERY
            : GET_PAUSED_STEP_ATTEMPT_BASE_QUERY;
    final String workflowIdUpperBound =
        isInlineWorkflow
            ? revisedWorkflowId + Constants.INLINE_WORKFLOW_ID_LARGEST_CHAR_IN_USE
            : null;
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt =
                      getPreparedStatementForMatchSpecific(
                          conn,
                          revisedWorkflowId,
                          workflowIdUpperBound,
                          version,
                          instanceId,
                          runId,
                          stepId,
                          stepAttemptId,
                          null,
                          query)) {
                    ResultSet result = stmt.executeQuery();
                    while (result.next()) {
                      toReturn.add(pausedStepAttemptFromResultSet(result));
                    }
                    return toReturn;
                  }
                }),
        "getPausedStepAttempts",
        "Failed on getPausedStepAttempts for breakpointIdentifier [{}][{}][{}][{}][{}][{}] ",
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
  }

  /**
   * Check if a step should be resumed.
   *
   * @param workflowId workflow id for the paused step.
   * @param version workflow version for the paused step.
   * @param instanceId workflow instanceId for the paused step.
   * @param runId workflow runId for the paused step.
   * @param stepId stepId for the paused step.
   * @param stepAttemptId stepAttemptId for the paused step.
   * @return true if step can be resumed, false otherwise.
   */
  public boolean shouldStepResume(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt = conn.prepareStatement(CHECK_PAUSED_STEP_ATTEMPT)) {
                    int idx = 0;
                    stmt.setString(++idx, workflowId);
                    stmt.setLong(++idx, version);
                    stmt.setLong(++idx, instanceId);
                    stmt.setLong(++idx, runId);
                    stmt.setString(++idx, stepId);
                    stmt.setLong(++idx, stepAttemptId);
                    ResultSet result = stmt.executeQuery();
                    return !result.next();
                  }
                }),
        "shouldStepResume",
        "Failed on shouldStepResume for stepIdentifier [{}][{}][{}][{}][{}][{}] ",
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
  }

  /**
   * Decide if step should transition to the PAUSE state. Also create paused step attempt. Step can
   * only be paused if a breakpoint is created matching the step identifier.
   *
   * @param workflowId workflow id for the step attempt.
   * @param version workflow version for the step attempt.
   * @param instanceId workflow instanceId for the step attempt.
   * @param runId workflow runId for the step attempt.
   * @param stepId stepId for the step attempt.
   * @param stepAttemptId stepAttemptId for the step attempt.
   * @return If a step is paused or not.
   */
  public boolean createPausedStepAttemptIfNeeded(
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  String stepBreakPointWorkflowId = getStepBreakPointWorkflowId(workflowId);
                  List<StepBreakpoint> breakpoints =
                      getStepBreakPointsWithConn(
                          conn,
                          stepBreakPointWorkflowId,
                          version,
                          instanceId,
                          runId,
                          stepId,
                          stepAttemptId);
                  if (!breakpoints.isEmpty()) {
                    insertPausedStepInstanceAttempt(
                        conn, workflowId, version, instanceId, runId, stepId, stepAttemptId);
                  }
                  return !breakpoints.isEmpty();
                }),
        "createPausedStepAttemptIfNeeded",
        "Failed on createPausedStepAttemptIfNeeded for stepIdentifier [{}][{}][{}][{}][{}][{}]",
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
  }

  /**
   * Generates the workflow id portion of a step break point. If the #workflowId passed in
   * references to a non-inline-workflow, we simply use it. Otherwise, we use the prefix (maestro
   * prefix + internal id of the parent workflow). Example:
   * maestro_foreach_hashedInternalId_hashedInstanceId_hashedStepReference will be converted to
   * maestro_foreach_hashedInternalId_. Please reference to {@link ForeachStepRuntime} and @{@link
   * WhileStepRuntime} for inline workflow id format.
   */
  private String getStepBreakPointWorkflowId(String workflowId) {
    String revisedWorkflowId = workflowId;
    if (IdHelper.isInlineWorkflowId(workflowId)) {
      final String[] components = workflowId.split("_");
      if (components.length < Constants.INLINE_WORKFLOW_ID_SPLIT_COMPONENT_COUNT) {
        throw new MaestroBadRequestException(
            Collections.emptyList(),
            "workflowId [%s] is started as an inline id but is formatted incorrectly.",
            workflowId);
      }
      revisedWorkflowId = String.format("%s_%s_%s_", components[0], components[1], components[2]);
    }

    return revisedWorkflowId;
  }

  /** Removes a specific breakpoint. */
  private void removeStepBreakpointWithConn(
      Connection conn,
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(DELETE_STEP_BREAKPOINT_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setLong(++idx, version);
      stmt.setLong(++idx, instanceId);
      stmt.setLong(++idx, runId);
      stmt.setString(++idx, stepId);
      stmt.setLong(++idx, stepAttemptId);
      int totalRemoved = stmt.executeUpdate();
      if (totalRemoved == 0) {
        throw new MaestroNotFoundException(
            "No breakpoint is deleted because breakpoint with identifier [%s][%d][%d][%d][%s][%d] is not been created"
                + " yet or has already been deleted",
            workflowId, version, instanceId, runId, stepId, stepAttemptId);
      }
      LOG.debug(
          "Removed [{}] step breakpoint from DB related to breakpoint with identifier [{}][{}][{}][{}][{}][{}]",
          totalRemoved,
          workflowId,
          version,
          instanceId,
          runId,
          stepId,
          stepAttemptId);
    }
  }

  /**
   * Removes all paused step attempts which are paused due to a particular step breakpoint. Called
   * from two invocations
   *
   * <p>Called when removing breakpoints applicable to the group identifier. ${@link
   * #removeStepBreakpoint}
   *
   * <p>Called explicitly when user choose to resume the specific step attempt.
   */
  private int removePausedStepsWithConn(
      Connection conn,
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId)
      throws SQLException {
    final boolean isInlineWorkflow = IdHelper.isInlineWorkflowId(workflowId);
    final String statement =
        isInlineWorkflow
            ? DELETE_PAUSED_INLINE_WORKFLOW_STEP_ATTEMPT_BASE_QUERY
            : DELETE_PAUSED_STEP_ATTEMPT_BASE_QUERY;
    final String workflowIdRangeUpperBound =
        isInlineWorkflow ? workflowId + Constants.INLINE_WORKFLOW_ID_LARGEST_CHAR_IN_USE : null;
    int totalRemoved = 0;
    int removed;
    do {
      try (PreparedStatement stmt =
          getPreparedStatementForMatchSpecific(
              conn,
              workflowId,
              workflowIdRangeUpperBound,
              version,
              instanceId,
              runId,
              stepId,
              stepAttemptId,
              batchDeletionLimitSupplier.get(),
              statement)) {
        removed = stmt.executeUpdate();
        totalRemoved += removed;
      }
    } while (removed == batchDeletionLimitSupplier.get());
    LOG.debug(
        "Removed [{}] paused step attempts from DB related to breakpoint with identifier [{}][{}][{}][{}][{}][{}]",
        totalRemoved,
        workflowId,
        version,
        instanceId,
        runId,
        stepId,
        stepAttemptId);
    return totalRemoved;
  }

  /**
   * Get step breakpoints for step identifiers. It supports layered look up. For e.g. is user has
   * asked to provide all breakpoints for specific step attempt and there is also a generic
   * breakpoint setup for step definition, it will be returned as well.
   */
  private List<StepBreakpoint> getStepBreakPointsWithConn(
      Connection conn,
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId)
      throws SQLException {
    try (PreparedStatement stmt =
        getPreparedStatementForMatchAnyOrSpecific(
            conn, workflowId, version, instanceId, runId, stepId, stepAttemptId)) {
      List<StepBreakpoint> toReturn = new ArrayList<>();
      try (ResultSet result = stmt.executeQuery()) {
        while (result.next()) {
          toReturn.add(stepBreakpointFromResultSet(result));
        }
        return toReturn;
      }
    }
  }

  /**
   * Augment the query statement to support layered lookup.
   *
   * <p>Generic breakpoints are supported for e.g. any instance in a specific version. For e.g.,
   * this is supported <specific_version, {@link Constants#MATCH_ALL_WORKFLOW_INSTANCES}>,
   *
   * <p>Generic version for specific instance is not supported, e.g. this is not supported <{@link
   * Constants#MATCH_ALL_WORKFLOW_VERSIONS}, specific instance>.
   */
  private PreparedStatement getPreparedStatementForMatchAnyOrSpecific(
      Connection conn,
      String workflowId,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long attemptId)
      throws SQLException {
    StringBuilder query = new StringBuilder(GET_STEP_BREAKPOINT_BASE_QUERY);
    if (version != Constants.MATCH_ALL_WORKFLOW_VERSIONS) {
      query.append(CONDITION_BY_SPECIFIC_OR_MATCH_ALL_VERSIONS);
    }
    if (instanceId != Constants.MATCH_ALL_WORKFLOW_INSTANCES) {
      query.append(CONDITION_BY_SPECIFIC_OR_MATCH_ALL_INSTANCES);
    }
    if (runId != Constants.MATCH_ALL_RUNS) {
      query.append(CONDITION_BY_SPECIFIC_OR_MATCH_ALL_RUNS);
    }
    if (attemptId != Constants.MATCH_ALL_STEP_ATTEMPTS) {
      query.append(CONDITION_BY_SPECIFIC_OR_MATCH_ALL_STEP_INSTANCE_ATTEMPT_IDS);
    }

    PreparedStatement stmt = conn.prepareStatement(query.toString());
    updateQueryStatementHelper(
        stmt, workflowId, null, version, instanceId, runId, stepId, attemptId, null);
    return stmt;
  }

  /**
   * Augment the query statement to narrow down the conditions based on the <version, instance, run,
   * attempt>.
   */
  private PreparedStatement getPreparedStatementForMatchSpecific(
      Connection conn,
      String workflowId,
      @Nullable String workflowIdUpperBound,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long attemptId,
      @Nullable Integer entryLimit,
      String queryPrefix)
      throws SQLException {
    StringBuilder query = new StringBuilder(queryPrefix);
    if (version != Constants.MATCH_ALL_WORKFLOW_VERSIONS) {
      query.append(CONDITION_BY_SPECIFIC_VERSION);
    }
    if (instanceId != Constants.MATCH_ALL_WORKFLOW_INSTANCES) {
      query.append(CONDITION_BY_SPECIFIC_INSTANCE);
    }
    if (runId != Constants.MATCH_ALL_RUNS) {
      query.append(CONDITION_BY_SPECIFIC_RUN);
    }
    if (attemptId != Constants.MATCH_ALL_STEP_ATTEMPTS) {
      query.append(CONDITION_BY_SPECIFIC_ATTEMPT_ID);
    }
    if (entryLimit != null) {
      query.append(ENTRY_LIMIT);
      query.append(")");
    }

    PreparedStatement stmt = conn.prepareStatement(query.toString());
    updateQueryStatementHelper(
        stmt,
        workflowId,
        workflowIdUpperBound,
        version,
        instanceId,
        runId,
        stepId,
        attemptId,
        entryLimit);

    return stmt;
  }

  private void updateQueryStatementHelper(
      PreparedStatement stmt,
      String workflowId,
      @Nullable String workflowIdUpperBound,
      long version,
      long instanceId,
      long runId,
      String stepId,
      long stepAttemptId,
      @Nullable Integer entryLimit)
      throws SQLException {
    int idx = 0;
    stmt.setString(++idx, workflowId);
    if (workflowIdUpperBound != null) {
      stmt.setString(++idx, workflowIdUpperBound);
    }

    stmt.setString(++idx, stepId);
    if (version != Constants.MATCH_ALL_WORKFLOW_VERSIONS) {
      stmt.setLong(++idx, version);
    }
    if (instanceId != Constants.MATCH_ALL_WORKFLOW_INSTANCES) {
      stmt.setLong(++idx, instanceId);
    }
    if (runId != Constants.MATCH_ALL_RUNS) {
      stmt.setLong(++idx, runId);
    }
    if (stepAttemptId != Constants.MATCH_ALL_STEP_ATTEMPTS) {
      stmt.setLong(++idx, stepAttemptId);
    }
    if (entryLimit != null) {
      stmt.setInt(++idx, entryLimit);
    }
  }

  /**
   * validate if a stepId is within the workflow definition.
   *
   * @return if the step id is referencing a valid nested step (a step that is inside foreach step).
   */
  private Optional<StepType> getRootStepTypeForNestedStep(Workflow workflow, String stepId) {
    if (workflow.getSteps().stream().anyMatch(step -> step.getId().equals(stepId))) {
      return Optional.empty();
    }
    // Verify if step is present or not
    StepType rootStepType = getRootStepTypeForNestedStep(workflow.getSteps(), stepId);
    if (rootStepType == null) {
      throw new MaestroBadRequestException(
          Collections.emptyList(),
          "Breakpoint can't be set as stepId [%s] is not present for the workflowId [%s]",
          stepId,
          workflow.getId());
    }
    return Optional.of(rootStepType);
  }

  private StepType getRootStepTypeForNestedStep(List<Step> stepList, String stepId) {
    for (Step step : stepList) {
      if (step.getId().equals(stepId)) {
        return step.getType();
      }
      if (step.getType() == StepType.FOREACH) {
        StepType ret = getRootStepTypeForNestedStep(((ForeachStep) step).getSteps(), stepId);
        if (ret != null) {
          return step.getType();
        }
      }
      if (step.getType() == StepType.WHILE) {
        StepType ret = getRootStepTypeForNestedStep(((WhileStep) step).getSteps(), stepId);
        if (ret != null) {
          return step.getType();
        }
      }
    }
    return null;
  }

  private PausedStepAttempt pausedStepAttemptFromResultSet(ResultSet rs) throws SQLException {
    return PausedStepAttempt.builder()
        .workflowId(rs.getString(WORKFLOW_ID))
        .workflowVersionId(rs.getLong(VERSION))
        .workflowInstanceId(rs.getLong(INSTANCE_ID))
        .workflowRunId(rs.getLong(RUN_ID))
        .stepId(rs.getString(STEP_ID))
        .stepAttemptId(rs.getLong(STEP_ATTEMPT_ID))
        .createTime(getTimestampIfPresent(rs, CREATE_TS))
        .build();
  }

  private void updateStepBreakpointBuilderWithMatchAllHelper(
      ResultSet rs, StepBreakpoint.StepBreakpointBuilder builder) throws SQLException {
    long version = rs.getLong(VERSION);
    long instanceId = rs.getLong(INSTANCE_ID);
    long runId = rs.getLong(RUN_ID);
    long stepAttemptId = rs.getLong(STEP_ATTEMPT_ID);
    builder.workflowVersionId(version == Constants.MATCH_ALL_WORKFLOW_VERSIONS ? null : version);
    builder.workflowInstanceId(
        instanceId == Constants.MATCH_ALL_WORKFLOW_INSTANCES ? null : instanceId);
    builder.workflowRunId(runId == Constants.MATCH_ALL_RUNS ? null : runId);
    builder.stepAttemptId(
        stepAttemptId == Constants.MATCH_ALL_STEP_ATTEMPTS ? null : stepAttemptId);
  }

  private StepBreakpoint stepBreakpointFromResultSet(ResultSet rs) throws SQLException {
    StepBreakpoint.StepBreakpointBuilder builder = StepBreakpoint.builder();
    builder
        .workflowId(rs.getString(WORKFLOW_ID))
        .stepId(rs.getString(STEP_ID))
        .createTime(getTimestampIfPresent(rs, CREATE_TS))
        .createdBy(fromJson(rs.getString("created_by"), User.class));
    updateStepBreakpointBuilderWithMatchAllHelper(rs, builder);
    return builder.build();
  }

  private String getRevisedWorkflowId(
      String workflowId, String stepId, boolean latestVersionStepExistEnforced) {
    WorkflowDefinition workflowDefinition =
        workflowDao.getWorkflowDefinition(workflowId, Constants.WorkflowVersion.DEFAULT.name());
    try {
      Optional<StepType> rootStepType =
          getRootStepTypeForNestedStep(workflowDefinition.getWorkflow(), stepId);
      return rootStepType
          .map(
              stepType ->
                  IdHelper.getInlineWorkflowPrefixId(workflowDefinition.getInternalId(), stepType))
          .orElse(workflowId);
    } catch (MaestroBadRequestException e) {
      if (!latestVersionStepExistEnforced) {
        return workflowId;
      }
      throw e;
    }
  }
}
