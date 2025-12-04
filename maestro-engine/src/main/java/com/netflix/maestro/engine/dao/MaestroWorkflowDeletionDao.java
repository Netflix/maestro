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
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.engine.utils.TimeUtils;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroTimeoutException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.IdHelper;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * DAO for actually deleting Maestro workflow data from multiple tables. It also manages the status
 * in maestro_workflow_deleted table. Note that the workflow in `maestro_workflow` data has been
 * deleted by {@link MaestroWorkflowDao} already. This class handles the related workflow data
 * deletion asynchronously. It also handles the workflow data retention as well.
 *
 * <p>If deleting the whole workflow for a given workflow id, it will delete
 *
 * <ul>
 *   <li>1. workflow versions
 *   <li>2. workflow timeline
 *   <li>3. workflow properties
 *   <li>4. step breakpoints
 *   <li>5. workflow step actions
 *   <li>6. workflow instances
 *   <li>7. step instances
 *   <li>8. directly associated/launched inline workflow instances
 *   <li>9. directly associated/launched inline workflow step instances
 *   <li>10. directly associated/created tag permits for job concurrency control
 * </ul>
 *
 * <p>If deleting a range of workflow instances, it will delete item 6 to 9.
 */
@Slf4j
public class MaestroWorkflowDeletionDao extends AbstractDatabaseDao {
  private enum Stage {
    DELETING_VERSIONS(
        "DELETE FROM maestro_workflow_version WHERE workflow_id=? AND version_id IN ("
            + "SELECT version_id FROM maestro_workflow_version WHERE workflow_id=? LIMIT ?)"),
    DELETING_TIMELINE(
        "DELETE FROM maestro_workflow_timeline WHERE workflow_id=? AND (create_ts, hash_id) IN ("
            + "SELECT create_ts, hash_id FROM maestro_workflow_timeline WHERE workflow_id=? LIMIT ?)"),
    DELETING_PROPERTIES(
        "DELETE FROM maestro_workflow_properties WHERE workflow_id=? AND create_time IN ("
            + "SELECT create_time FROM maestro_workflow_properties WHERE workflow_id=? LIMIT ?)"),
    DELETING_BREAKPOINTS(
        "DELETE FROM maestro_step_breakpoint WHERE workflow_id=? "
            + "AND (step_id, system_generated, version, instance_id, run_id, step_attempt_id) IN ("
            + "SELECT step_id, system_generated, version, instance_id, run_id, step_attempt_id "
            + "FROM maestro_step_breakpoint WHERE workflow_id=? LIMIT ?)"),
    DELETING_ACTIONS(
        "DELETE FROM maestro_step_instance_action WHERE workflow_id=? "
            + "AND (workflow_instance_id, workflow_run_id, step_id) IN ("
            + "SELECT workflow_instance_id, workflow_run_id, step_id "
            + "FROM maestro_step_instance_action WHERE workflow_id=? LIMIT ?)"),
    DELETING_WORKFLOW_INSTANCES(
        "DELETE FROM maestro_workflow_instance WHERE workflow_id=? AND (instance_id, run_id) IN ("
            + "SELECT instance_id, run_id FROM maestro_workflow_instance WHERE workflow_id=? LIMIT ?)"),
    DELETING_STEP_INSTANCES(
        "DELETE FROM maestro_step_instance WHERE workflow_id=? "
            + "AND (workflow_instance_id, step_id, workflow_run_id, step_attempt_id) IN ("
            + "SELECT workflow_instance_id, step_id, workflow_run_id, step_attempt_id "
            + "FROM maestro_step_instance WHERE workflow_id=? LIMIT ?)"),
    DELETING_INLINE_INSTANCES(
        "DELETE FROM maestro_workflow_instance WHERE (workflow_id, instance_id, run_id) IN ("
            + "SELECT workflow_id, instance_id, run_id FROM maestro_workflow_instance WHERE "
            + "(workflow_id >= ? AND workflow_id < ?) OR (workflow_id >= ? AND workflow_id < ?) LIMIT ?)") {
      @Override
      void prepareQuery(PreparedStatement stmt, String workflowId, long internalId)
          throws SQLException {
        int idx = 0;
        stmt.setString(++idx, IdHelper.getInlineWorkflowPrefixId(internalId, StepType.FOREACH));
        stmt.setString(
            ++idx,
            IdHelper.getInlineWorkflowPrefixId(internalId, StepType.FOREACH)
                + Constants.INLINE_WORKFLOW_ID_LARGEST_CHAR_IN_USE); // upper bound
        stmt.setString(++idx, IdHelper.getInlineWorkflowPrefixId(internalId, StepType.WHILE));
        stmt.setString(
            ++idx,
            IdHelper.getInlineWorkflowPrefixId(internalId, StepType.WHILE)
                + Constants.INLINE_WORKFLOW_ID_LARGEST_CHAR_IN_USE); // upper bound
        stmt.setInt(++idx, Constants.BATCH_DELETION_LIMIT);
      }
    },
    DELETING_INLINE_STEP_INSTANCES(
        "DELETE FROM maestro_step_instance WHERE "
            + "(workflow_id, workflow_instance_id, step_id, workflow_run_id, step_attempt_id) IN ("
            + "SELECT workflow_id, workflow_instance_id, step_id, workflow_run_id, step_attempt_id FROM "
            + "maestro_step_instance WHERE (workflow_id >= ? AND workflow_id < ?) "
            + "OR (workflow_id >= ? AND workflow_id < ?) LIMIT ?)") {
      @Override
      void prepareQuery(PreparedStatement stmt, String workflowId, long internalId)
          throws SQLException {
        DELETING_INLINE_INSTANCES.prepareQuery(stmt, workflowId, internalId);
      }
    },
    DELETING_JOB_CONCURRENCY_TAG_PERMITS(
        "DELETE FROM maestro_tag_permit WHERE tag IN ("
            + "SELECT tag FROM maestro_tag_permit WHERE tag >= ? AND tag < ? LIMIT ?)") {
      @Override
      void prepareQuery(PreparedStatement stmt, String workflowId, long internalId)
          throws SQLException {
        String prefix = Constants.MAESTRO_PREFIX + workflowId + ":";
        int idx = 0;
        stmt.setString(++idx, prefix);
        stmt.setString(++idx, prefix + LARGEST_CHAR_IN_USE); // upper bound
        stmt.setInt(++idx, Constants.BATCH_DELETION_LIMIT);
      }
    },
    DELETION_DONE("");

    @Getter private final String query;

    void prepareQuery(PreparedStatement stmt, String workflowId, long internalId)
        throws SQLException {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setString(++idx, workflowId);
      stmt.setInt(++idx, Constants.BATCH_DELETION_LIMIT);
    }

    Stage(String query) {
      this.query = query;
    }

    static Stage create(String stageName) {
      return Stage.valueOf(stageName.toUpperCase(Locale.US));
    }
  }

  private static final int BATCH_DELAY_IN_MILLIS = 100;
  private static final String LARGEST_CHAR_IN_USE = "~";
  private static final Stage[] DELETION_STAGES = Stage.values();

  private static final String EXIST_IN_PROGRESS_DELETION_QUERY =
      "SELECT stage FROM maestro_workflow_deleted WHERE workflow_id=? AND stage != 'DELETION_DONE' ";

  private static final String GET_WORKFLOW_DELETION_STAGE_QUERY =
      "SELECT stage FROM maestro_workflow_deleted WHERE workflow_id=? AND internal_id=?";

  private static final String UPDATE_WORKFLOW_DELETION_STAGE_QUERY =
      "UPDATE maestro_workflow_deleted SET (stage,timeline,modify_ts) "
          + "= (?,array_cat(timeline,?),CURRENT_TIMESTAMP) WHERE workflow_id=? AND internal_id=?";

  public MaestroWorkflowDeletionDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /** Check if there is pending deletion for a given workflow id. */
  public boolean isDeletionInProgress(String workflowId) {
    return withRetryableQuery(
        EXIST_IN_PROGRESS_DELETION_QUERY, stmt -> stmt.setString(1, workflowId), ResultSet::next);
  }

  /**
   * Delete all workflow data for a given workflow id or unique internal id with a timeout in nanos.
   *
   * @param workflowId workflow id
   * @param internalId internal workflow unique id
   * @param timeoutInNanos timeout in nanoseconds
   */
  public void deleteWorkflowData(String workflowId, long internalId, long timeoutInNanos) {
    final long startNanos = System.nanoTime();
    Stage stage = getWorkflowDeletionStage(workflowId, internalId);

    int totalDeleted = 0;
    int deleted = 0;
    long stageTime = startNanos;
    Timeline timeline = new Timeline(null);

    while (stage != Stage.DELETION_DONE) {
      int cnt = deleteDataForStage(workflowId, internalId, stage);
      TimeUtils.sleep(BATCH_DELAY_IN_MILLIS)
          .ifPresent(
              details ->
                  LOG.info(
                      "Thread is interrupted, ignore it and just continue without sleeping: {}",
                      details));
      totalDeleted += cnt;
      deleted += cnt;
      long curTime = System.nanoTime();
      if (cnt < Constants.BATCH_DELETION_LIMIT || curTime - startNanos >= timeoutInNanos) {
        TimelineEvent event =
            TimelineLogEvent.info(
                "Deleted [%s] items in the stage of [%s], taking [%s] millis",
                deleted, stage.name(), TimeUnit.NANOSECONDS.toMillis(curTime - stageTime));
        LOG.info(event.getMessage());
        timeline.add(event);
        if (cnt < Constants.BATCH_DELETION_LIMIT) {
          deleted = 0;
          stage = DELETION_STAGES[stage.ordinal() + 1]; // move to the next stage
          stageTime = curTime;
        }
        if (curTime - startNanos >= timeoutInNanos) {
          break;
        }
      }
    }

    LOG.info(
        "Totally deleted {} rows while deleting workflow [{}][{}] and then "
            + "complete this round of deletion at stage [{}], taking [{}] millis.",
        totalDeleted,
        workflowId,
        internalId,
        stage.name(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));

    int updated = updateWorkflowDeletionStage(workflowId, internalId, stage, timeline);
    if (updated != SUCCESS_WRITE_SIZE) {
      throw new MaestroRetryableError(
          "Failed to update workflow deletion data for workflow [%s][%s], retry it later",
          workflowId, internalId);
    }
    if (stage != Stage.DELETION_DONE) {
      LOG.info(
          "Workflow [{}][{}] deletion is timed out and will retry it later",
          workflowId,
          internalId);
      throw new MaestroTimeoutException(
          "Workflow [%s][%s] deletion is timed out", workflowId, internalId);
    }
  }

  private Stage getWorkflowDeletionStage(String workflowId, long internalId) {
    Stage res =
        withRetryableQuery(
            GET_WORKFLOW_DELETION_STAGE_QUERY,
            stmt -> {
              stmt.setString(1, workflowId);
              stmt.setLong(2, internalId);
            },
            result -> {
              if (result.next()) {
                return Stage.create(result.getString(1));
              }
              return null;
            });
    if (res == null) {
      throw new MaestroNotFoundException(
          "Cannot get the deletion status for workflow [%s] with internalId [%s]. Please make sure it is deleted.",
          workflowId, internalId);
    }
    return res;
  }

  /**
   * Batch delete all workflow data for a given workflow id or unique internal id based on the
   * stage.
   */
  private int deleteDataForStage(String workflowId, long internalId, Stage stage) {
    return withMetricLogError(
        () ->
            withRetryableUpdate(
                stage.getQuery(), stmt -> stage.prepareQuery(stmt, workflowId, internalId)),
        "deleteDataForStage-" + stage.name(),
        "Failed to delete data for a workflow [{}][{}] in stage [{}]",
        workflowId,
        internalId,
        stage.name());
  }

  private int updateWorkflowDeletionStage(
      String workflowId, long internalId, Stage stage, Timeline timeline) {
    final String[] timelineArray =
        timeline == null
            ? null
            : timeline.getTimelineEvents().stream().map(this::toJson).toArray(String[]::new);
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  try (PreparedStatement stmt =
                      conn.prepareStatement(UPDATE_WORKFLOW_DELETION_STAGE_QUERY)) {
                    int idx = 0;
                    stmt.setString(++idx, stage.name());
                    stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, timelineArray));
                    stmt.setString(++idx, workflowId);
                    stmt.setLong(++idx, internalId);
                    return stmt.executeUpdate();
                  }
                }),
        "updateWorkflowDeletionStage",
        "Failed to update workflow deletion stage for a workflow [{}][{}] in stage [{}]",
        workflowId,
        internalId,
        stage.name());
  }
}
