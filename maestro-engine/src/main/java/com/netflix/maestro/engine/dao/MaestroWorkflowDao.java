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
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.cockroachdb.dao.CockroachDBBaseDAO;
import com.netflix.conductor.cockroachdb.util.StatementPreparer;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.db.PropertiesUpdate;
import com.netflix.maestro.engine.db.PropertiesUpdate.Type;
import com.netflix.maestro.engine.dto.MaestroWorkflow;
import com.netflix.maestro.engine.dto.MaestroWorkflowVersion;
import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.engine.utils.TriggerSubscriptionClient;
import com.netflix.maestro.exceptions.InvalidWorkflowVersionException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroPreconditionFailedException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.WorkflowOverviewResponse;
import com.netflix.maestro.models.definition.Metadata;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.models.timeline.WorkflowTimeline;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.IdHelper;
import jakarta.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * DAO for saving and retrieving Maestro workflow data model.
 *
 * <p>In the data model, we use `null` to indicate `unset`.
 */
// mute the false positive error due to https://github.com/spotbugs/spotbugs/issues/293
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@Slf4j
public class MaestroWorkflowDao extends CockroachDBBaseDAO {
  private static final String WORKFLOW_ID_COLUMN = "workflow_id";
  private static final String PROPERTIES_COLUMN = "properties_snapshot";
  private static final String METADATA_COLUMN = "metadata";
  private static final String DEFINITION_COLUMN = "definition";
  private static final String TRIGGER_UUIDS_COLUMN = "trigger_uuids";
  private static final String ACTIVE_VERSION_COLUMN = "active_version_id";
  private static final String LATEST_VERSION_COLUMN = "latest_version_id";
  private static final String LATEST_INSTANCE_COLUMN = "latest_instance_id";
  private static final String MODIFY_TS_COLUMN = "modify_ts";
  private static final String INTERNAL_ID_COLUMN = "internal_id";
  private static final long INITIAL_ID = 1L;
  private static final long EMPTY_SIZE_CASE = 1L;
  private static final int MORE_THAN_ONE_CONDITION = 2;

  private static final String CREATE_WORKFLOW_VERSION_QUERY =
      "INSERT INTO maestro_workflow_version (metadata,definition,trigger_uuids) VALUES (?,?,?) ";
  private static final String CREATE_WORKFLOW_PROPS_QUERY =
      "INSERT INTO maestro_workflow_properties "
          + "(workflow_id,create_time,author,properties_changes,previous_snapshot) VALUES (?,?,?,?,?)";

  private static final String UPSERT_WORKFLOW_QUERY_TEMPLATE =
      "UPSERT INTO maestro_workflow (%s,modify_ts) VALUES (%s,CURRENT_TIMESTAMP) RETURNING modify_ts, internal_id";

  private static final String INSERT_TIMELINE_QUERY =
      "INSERT INTO maestro_workflow_timeline (workflow_id,change_event) VALUES (?,?) ON CONFLICT DO NOTHING";

  private static final String GET_CURRENT_WORKFLOW_INFO_FOR_UPDATE =
      "SELECT active_version_id,latest_version_id,properties_snapshot "
          + "FROM maestro_workflow WHERE workflow_id=? FOR UPDATE";

  private static final String GET_CURRENT_PROPERTIES_SNAPSHOT =
      "SELECT properties_snapshot FROM maestro_workflow WHERE workflow_id=?";

  private static final String GET_MAESTRO_WORKFLOW =
      "SELECT workflow_id,internal_id,active_version_id,activate_ts,activated_by,properties_snapshot,"
          + "latest_version_id,modify_ts FROM maestro_workflow WHERE workflow_id=?";

  private static final String GET_MAESTRO_WORKFLOW_VERSION =
      "SELECT metadata,definition,trigger_uuids FROM maestro_workflow_version WHERE workflow_id=? AND version_id=?";

  // if needed, we can keep a copy of trigger_uuids in maestro_workflow table to save this query
  private static final String GET_MAESTRO_WORKFLOW_VERSION_TRIGGER_UUIDS =
      "SELECT trigger_uuids FROM maestro_workflow_version WHERE workflow_id=? AND version_id=?";

  private static final String DELETE_MAESTRO_WORKFLOW_QUERY =
      "WITH deleted_wf AS ("
          + "DELETE FROM maestro_workflow WHERE workflow_id=? AND NOT EXISTS "
          + "(SELECT workflow_id FROM maestro_workflow_instance@workflow_status_index "
          + "WHERE workflow_id=? AND status=ANY('CREATED','IN_PROGRESS') LIMIT 1) RETURNING *)"
          + "INSERT INTO maestro_workflow_deleted (workflow, timeline) "
          + "SELECT row_to_json(deleted_wf), ARRAY[?] FROM deleted_wf RETURNING internal_id";

  private static final String DEACTIVATE_WORKFLOW_QUERY =
      "UPDATE maestro_workflow SET (active_version_id,activate_ts,modify_ts,activated_by)=(0,"
          + "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?) WHERE workflow_id=? RETURNING "
          + "(SELECT active_version_id FROM maestro_workflow WHERE workflow_id=?)";

  private static final String ACTIVATE_WORKFLOW_VERSION_QUERY =
      "UPDATE maestro_workflow SET (active_version_id,activate_ts,modify_ts,activated_by)=(?,"
          + "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?) WHERE workflow_id=?";

  private static final String GET_WORKFLOW_OVERVIEW_QUERY =
      "SELECT active_version_id,latest_version_id,latest_instance_id,properties_snapshot,"
          + "(SELECT JSONB_OBJECT_AGG(status, cnt) FROM (SELECT status, count(*) as cnt "
          + "FROM maestro_workflow_instance@workflow_status_index WHERE workflow_id=? "
          + "AND status IN ('CREATED','IN_PROGRESS','PAUSED','FAILED') GROUP BY status)) as status "
          + "FROM maestro_workflow WHERE workflow_id=?";

  private static final TypeReference<Map<String, Long>> STATUS_STATS_REFERENCE =
      new TypeReference<Map<String, Long>>() {};

  private static final String GET_INSTANCE_COUNT_BY_STATUS_QUERY_PREFIX =
      "SELECT status, count(*) as cnt FROM maestro_workflow_instance@workflow_status_index WHERE workflow_id=? ";
  private static final String GET_NONTERMINAL_INSTANCE_COUNT_QUERY =
      GET_INSTANCE_COUNT_BY_STATUS_QUERY_PREFIX
          + "AND status=ANY('CREATED','IN_PROGRESS') GROUP BY status";
  private static final String GET_NONTERMINAL_FAILED_INSTANCE_COUNT_QUERY =
      GET_INSTANCE_COUNT_BY_STATUS_QUERY_PREFIX
          + "AND status=ANY('CREATED','IN_PROGRESS','FAILED') GROUP BY status";

  private static final String GET_WORKFLOW_PARAM_FOR_PREFIX_QUERY =
      "SELECT mwv.workflow_id as id, definition->'params'->>? as payload FROM maestro_workflow_version mwv "
          + "JOIN (SELECT workflow_id, latest_version_id FROM maestro_workflow WHERE workflow_id LIKE ? limit 20) tmp "
          + "ON mwv.workflow_id=tmp.workflow_id and mwv.version_id=tmp.latest_version_id";

  private static final String GET_WORKFLOW_TIMELINE_QUERY =
      "SELECT change_event as payload FROM maestro_workflow_timeline WHERE workflow_id=? ORDER BY create_ts DESC "
          + "LIMIT "
          + Constants.TIMELINE_EVENT_SIZE_LIMIT;

  private static final String GET_INLINE_WORKFLOW_DEFINITION =
      "SELECT instance as payload FROM maestro_workflow_instance WHERE workflow_id=? AND instance_id=1 AND run_id=1";

  /** === below are batch queries ===. */
  private static final String WORKFLOW_VERSION_SCAN_QUERY_COMMON_SUFFIX =
      " ORDER BY workflow_id, version_id DESC LIMIT ? ";

  private static final String WORKFLOW_VERSION_SCAN_QUERY_COMMON_PREFIX =
      "SELECT metadata, definition FROM maestro_workflow_version ";
  private static final String WORKFLOW_VERSION_SCAN_WITH_WORKFLOW_VERSION_FILTER_QUERY =
      WORKFLOW_VERSION_SCAN_QUERY_COMMON_PREFIX
          + "WHERE workflow_id=? AND version_id<?"
          + WORKFLOW_VERSION_SCAN_QUERY_COMMON_SUFFIX;

  private static final String WORKFLOW_VERSION_SCAN_WITH_WORKFLOW_FILTER_QUERY =
      WORKFLOW_VERSION_SCAN_QUERY_COMMON_PREFIX
          + " WHERE workflow_id=?"
          + WORKFLOW_VERSION_SCAN_QUERY_COMMON_SUFFIX;

  private static final String GET_LATEST_WORKFLOW_INSTANCE_ID_FROM_WORKFLOW_DELETED_QUERY =
      "SELECT MAX((workflow->'latest_instance_id')::INT) as id FROM maestro_workflow_deleted WHERE workflow_id=?";
  private final MaestroJobEventPublisher publisher;
  private final TriggerSubscriptionClient subscriptionClient;

  /**
   * Constructor for Maestro workflow DAO.
   *
   * @param dataSource database data source
   * @param objectMapper object mapper
   * @param config configuration
   * @param subscriptionClient trigger service(s) client.
   */
  public MaestroWorkflowDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      CockroachDBConfiguration config,
      MaestroJobEventPublisher publisher,
      TriggerSubscriptionClient subscriptionClient) {
    super(dataSource, objectMapper, config);
    this.publisher = publisher;
    this.subscriptionClient = subscriptionClient;
  }

  /**
   * Return created workflow definition {@link WorkflowDefinition} posted by users.
   *
   * @param workflowDef WorkflowDefinition with incomplete data. Filling it during version creation
   *     {@link WorkflowDefinition}
   * @return WorkflowDefinition the persisted workflow definition with complete info
   */
  public WorkflowDefinition addWorkflowDefinition(
      WorkflowDefinition workflowDef, Properties changes) {
    LOG.info("Adding a new workflow definition with an id [{}]", workflowDef.getWorkflow().getId());
    final Workflow workflow = workflowDef.getWorkflow();
    final Metadata metadata = workflowDef.getMetadata();
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  WorkflowInfo workflowInfo = getWorkflowInfoForUpdate(conn, workflow.getId());
                  final long nextVersionId = workflowInfo.getLatestVersionId() + 1;
                  // update the metadata with version info and then metadata is complete.
                  metadata.setWorkflowVersionId(nextVersionId);
                  TriggerUuids triggerUuids =
                      insertMaestroWorkflowVersion(conn, metadata, workflow);
                  PropertiesSnapshot snapshot =
                      updateWorkflowProps(
                          conn,
                          workflow.getId(),
                          metadata.getVersionAuthor(),
                          metadata.getCreateTime(),
                          workflowInfo.getPrevPropertiesSnapshot(),
                          changes,
                          new PropertiesUpdate(Type.ADD_WORKFLOW_DEFINITION));
                  // add new snapshot to workflowDef
                  if (snapshot != null) {
                    workflowDef.setPropertiesSnapshot(snapshot);
                  } else {
                    workflowDef.setPropertiesSnapshot(workflowInfo.getPrevPropertiesSnapshot());
                  }

                  final long[] upsertRes = upsertMaestroWorkflow(conn, workflowDef);
                  Checks.notNull(
                      upsertRes,
                      "the upsert result should not be null for workflow [%s]",
                      workflow.getId());
                  workflowDef.setIsLatest(true); // a new version will always be latest
                  // add default flag and modified_time and then workflowDef is complete
                  workflowDef.setIsDefault(
                      workflowInfo.getPrevActiveVersionId() == Constants.INACTIVE_VERSION_ID
                          || workflowDef.getIsActive());
                  workflowDef.setModifyTime(upsertRes[0]);
                  workflowDef.setInternalId(upsertRes[1]);

                  if (workflowDef.getIsActive()) {
                    workflowInfo.setNextActiveWorkflow(
                        MaestroWorkflowVersion.builder()
                            .definition(workflow)
                            .triggerUuids(triggerUuids)
                            .metadata(metadata)
                            .build(),
                        workflowDef.getPropertiesSnapshot());
                  } else if (workflowInfo.getPrevActiveVersionId()
                      != Constants.INACTIVE_VERSION_ID) {
                    // getting an inactive new version but having an active old version
                    updateWorkflowInfoForNextActiveWorkflow(
                        conn,
                        workflow.getId(),
                        workflowInfo.getPrevActiveVersionId(),
                        workflowInfo,
                        workflowDef.getPropertiesSnapshot());
                  }
                  if (workflowInfo.withWorkflow()) {
                    addWorkflowTriggersIfNeeded(conn, workflowInfo);
                  }

                  MaestroJobEvent jobEvent =
                      logToTimeline(
                          conn, workflowDef, snapshot, workflowInfo.getPrevActiveVersionId());
                  publisher.publishOrThrow(
                      jobEvent, "Failed to publish maestro definition change job event.");
                  return workflowDef;
                }),
        "addWorkflowDefinition",
        "Failed creating a new workflow definition {}",
        workflow.getId());
  }

  /**
   * Update the workflow properties posted by users.
   *
   * <p>Note: if no wf exists, cannot push properties.
   *
   * @param workflowId workflow id
   * @param props properties including new values for update
   * @return the persisted properties
   */
  public PropertiesSnapshot updateWorkflowProperties(
      String workflowId, User author, Properties props, PropertiesUpdate update) {
    LOG.debug("Updating workflow properties for workflow id [{}]", workflowId);
    Checks.notNull(
        props, "properties changes to apply cannot be null for workflow [%s]", workflowId);
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  WorkflowInfo workflowInfo = getWorkflowInfoForUpdate(conn, workflowId);
                  Checks.notNull(
                      workflowInfo.getPrevPropertiesSnapshot(),
                      "Cannot update workflow properties while the workflow [%s] does not exist",
                      workflowId);
                  PropertiesSnapshot snapshot =
                      updateWorkflowProps(
                          conn,
                          workflowId,
                          author,
                          System.currentTimeMillis(),
                          workflowInfo.getPrevPropertiesSnapshot(),
                          props,
                          update);

                  List<StatementPreparer> preparers = new ArrayList<>();
                  StringBuilder fields = prepareProperties(preparers, workflowId, snapshot);

                  long[] updateRes = executeTemplateUpdate(conn, fields, preparers);

                  if (updateRes != null) {
                    if (workflowInfo.getPrevActiveVersionId() != Constants.INACTIVE_VERSION_ID) {
                      updateWorkflowInfoForNextActiveWorkflow(
                          conn,
                          workflowId,
                          workflowInfo.getPrevActiveVersionId(),
                          workflowInfo,
                          snapshot);
                      addWorkflowTriggersIfNeeded(conn, workflowInfo);
                    }

                    MaestroJobEvent jobEvent = logToTimeline(conn, workflowId, snapshot);
                    publisher.publishOrThrow(
                        jobEvent, "Failed to publish maestro properties change job event.");
                  }
                  return snapshot;
                }),
        "updateWorkflowProperties",
        "Failed updating the properties for workflow [{}]",
        workflowId);
  }

  private TriggerUuids getTriggerUuids(Connection conn, String workflowId, long versionId)
      throws SQLException {
    try (PreparedStatement stmt =
        conn.prepareStatement(GET_MAESTRO_WORKFLOW_VERSION_TRIGGER_UUIDS)) {
      stmt.setString(1, workflowId);
      stmt.setLong(2, versionId);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return convertJson(result.getString(TRIGGER_UUIDS_COLUMN), TriggerUuids.class);
        }
        return null;
      }
    }
  }

  private interface SupplierWithSQLException<T> {
    T get() throws SQLException;
  }

  private TriggerUuids adjustTriggerUuids(
      long versionId, PropertiesSnapshot snapshot, SupplierWithSQLException<TriggerUuids> getUuids)
      throws SQLException {
    boolean isEmpty = true;
    if (versionId > Constants.INACTIVE_VERSION_ID
        && snapshot != null
        && (snapshot.getTimeTriggerDisabled() != Boolean.TRUE
            || snapshot.getSignalTriggerDisabled() != Boolean.TRUE)) {
      TriggerUuids.TriggerUuidsBuilder builder = TriggerUuids.builder();
      TriggerUuids uuids = getUuids.get();
      if (uuids != null) {
        if (snapshot.getTimeTriggerDisabled() != Boolean.TRUE
            && uuids.getTimeTriggerUuid() != null) {
          builder.timeTriggerUuid(uuids.getTimeTriggerUuid());
          isEmpty = false;
        }
        if (snapshot.getSignalTriggerDisabled() != Boolean.TRUE
            && uuids.getSignalTriggerUuids() != null) {
          builder.signalTriggerUuids(uuids.getSignalTriggerUuids());
          isEmpty = false;
        }
      }
      return isEmpty ? null : builder.build();
    }
    return null;
  }

  private void addWorkflowTriggersIfNeeded(Connection conn, WorkflowInfo workflowInfo)
      throws SQLException {
    TriggerUuids currTriggerUuids =
        adjustTriggerUuids(
            workflowInfo.getActiveVersionId(),
            workflowInfo.getPropertiesSnapshot(),
            workflowInfo::getTriggerUuids);

    if (currTriggerUuids == null) {
      return;
    }

    TriggerUuids prevTriggerUuids =
        adjustTriggerUuids(
            workflowInfo.getPrevActiveVersionId(),
            workflowInfo.getPrevPropertiesSnapshot(),
            () ->
                getTriggerUuids(
                    conn,
                    workflowInfo.getWorkflow().getId(),
                    workflowInfo.getPrevActiveVersionId()));

    subscriptionClient.upsertTriggerSubscription(
        workflowInfo.getWorkflow(), currTriggerUuids, prevTriggerUuids);
  }

  private void updateWorkflowInfoForNextActiveWorkflow(
      Connection conn,
      String workflowId,
      long activeVersionId,
      WorkflowInfo workflowInfo,
      PropertiesSnapshot snapshot)
      throws SQLException {
    MaestroWorkflowVersion mwv =
        queryMaestroWorkflowVersionWithConn(conn, workflowId, activeVersionId);
    workflowInfo.setNextActiveWorkflow(mwv, snapshot);
  }

  /**
   * Get workflow definition for a given workflow id and version id.
   *
   * <p>For inline foreach workflow, it will return a definition by combining the parent workflow
   * properties with the first run of the first iteration of the inline foreach workflow instance
   * runtime workflow.
   *
   * @param workflowId workflow id
   * @param version workflow version, i.e. active, latest, default, or a version number
   * @return Maestro workflow DB data model
   */
  public WorkflowDefinition getWorkflowDefinition(String workflowId, String version) {
    if (IdHelper.isInlineWorkflowId(workflowId)) {
      return getInlineWorkflowDefinitionInternal(workflowId);
    } else {
      return getMaestroWorkflowInternal(workflowId, version).toDefinition();
    }
  }

  /**
   * Build a definition for inline workflow by combining the parent workflow properties with the
   * first run of the first iteration of the inline foreach workflow instance runtime workflow.
   */
  private WorkflowDefinition getInlineWorkflowDefinitionInternal(String workflowId) {
    return withMetricLogError(
        () -> {
          WorkflowInstance instance =
              withRetryableQuery(
                  GET_INLINE_WORKFLOW_DEFINITION,
                  stmt -> stmt.setString(1, workflowId),
                  result -> {
                    if (result.next()) {
                      return convertJson(result.getString(PAYLOAD_COLUMN), WorkflowInstance.class);
                    }
                    throw new MaestroNotFoundException(
                        "Cannot find inline workflow [%s], which is either not created or has been deleted.",
                        workflowId);
                  });
          String nonInlineWorkflowId =
              ((ForeachInitiator) instance.getInitiator()).getNonInlineParent().getWorkflowId();
          MaestroWorkflow maestroWorkflow = getMaestroWorkflow(nonInlineWorkflowId);
          if (maestroWorkflow == null) {
            throw new MaestroNotFoundException(
                "Parent workflow [%s] of inline workflow [%s] has not been created yet or has been deleted.",
                workflowId, nonInlineWorkflowId);
          }
          Metadata metadata = new Metadata();
          metadata.setWorkflowVersionId(instance.getWorkflowVersionId());
          metadata.setWorkflowId(workflowId);
          maestroWorkflow.setMetadata(metadata);
          maestroWorkflow.setDefinition(instance.getRuntimeWorkflow());
          return maestroWorkflow.toDefinition();
        },
        "getInlineWorkflowDefinitionInternal",
        "Failed to get the inline workflow definition for [{}]",
        workflowId);
  }

  private long deriveVersionId(String workflowId, String version, WorkflowInfo workflowInfo) {
    long currActiveVersionId = workflowInfo.getPrevActiveVersionId();
    long versionId;
    switch (Constants.WorkflowVersion.of(version)) {
      case ACTIVE:
        if (currActiveVersionId <= Constants.INACTIVE_VERSION_ID) {
          throw new MaestroNotFoundException(
              "Cannot find an active version for workflow [%s]", workflowId);
        }
        versionId = currActiveVersionId;
        break;
      case LATEST:
        versionId = workflowInfo.getLatestVersionId();
        break;
      case DEFAULT:
        versionId =
            currActiveVersionId == Constants.INACTIVE_VERSION_ID
                ? workflowInfo.getLatestVersionId()
                : currActiveVersionId;
        break;
      case EXACT:
      default:
        versionId =
            Checks.toNumeric(version)
                .orElseThrow(() -> new InvalidWorkflowVersionException(workflowId, version));
        break;
    }

    if (versionId < INITIAL_ID) {
      throw new InvalidWorkflowVersionException(workflowId, version);
    }
    return versionId;
  }

  private MaestroWorkflow getMaestroWorkflowInternal(String workflowId, String version) {
    return withMetricLogError(
        () -> {
          MaestroWorkflow maestroWorkflow = getMaestroWorkflow(workflowId);
          if (maestroWorkflow == null) {
            throw new MaestroNotFoundException(
                "Workflow [%s] has not been created yet or has been deleted.", workflowId);
          }

          long versionId = deriveVersionId(workflowId, version, new WorkflowInfo(maestroWorkflow));
          MaestroWorkflowVersion mwv =
              withRetryableTransaction(
                  conn -> queryMaestroWorkflowVersionWithConn(conn, workflowId, versionId));
          maestroWorkflow.setMetadata(mwv.getMetadata());
          maestroWorkflow.setDefinition(mwv.getDefinition());
          maestroWorkflow.setTriggerUuids(mwv.getTriggerUuids());
          return maestroWorkflow;
        },
        "getWorkflowDefinition",
        "Failed to get the workflow definition for workflow [{}] with version [{}]",
        workflowId,
        version);
  }

  private MaestroWorkflowVersion queryMaestroWorkflowVersionWithConn(
      Connection conn, String workflowId, long versionId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(GET_MAESTRO_WORKFLOW_VERSION)) {
      stmt.setString(1, workflowId);
      stmt.setLong(2, versionId);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return maestroWorkflowVersionFromResult(result);
        }
        throw new MaestroNotFoundException(
            "Cannot find workflow [%s] with version [%s]", workflowId, versionId);
      }
    }
  }

  /**
   * Delete the workflow from the workflow table. All associated workflow data (e.g. definitions and
   * instances) will be deleted asynchronously.
   *
   * @param workflowId workflow id
   */
  public TimelineEvent deleteWorkflow(String workflowId, User author) {
    if (IdHelper.isInlineWorkflowId(workflowId)) {
      throw new MaestroUnprocessableEntityException(
          "Cannot delete an inline foreach workflow [%s], please delete its parent concrete workflow instead.",
          workflowId);
    }

    final int nonTerminalCnt = Arrays.stream(getIndexedInstanceCount(workflowId, false)).sum();
    Checks.checkTrue(
        nonTerminalCnt == 0,
        "Cannot delete the workflow [%s] "
            + "because there are still [%s] number of queued or running workflow instances."
            + "Please delete it after taking care of them, e.g. stopping all of them.",
        workflowId,
        nonTerminalCnt);

    TimelineEvent info =
        TimelineLogEvent.info(
            "The workflow is deleted by [%s]. "
                + "All associated workflow data (e.g. versions and instances) will be deleted shortly.",
            author.getName());

    final String infoString = toJson(info);
    Long res =
        withRetryableStatement(
            DELETE_MAESTRO_WORKFLOW_QUERY,
            stmt -> {
              int idx = 0;
              stmt.setString(++idx, workflowId);
              stmt.setString(++idx, workflowId);
              stmt.setString(++idx, infoString);
              try (ResultSet result =
                  stmt.executeQuery()) { // unnecessary, to avoid PMD false positive
                if (result.next()) {
                  long internalId = result.getLong(1);
                  Checks.checkTrue(
                      !result.next(),
                      "Aborting the deletion as there is already a deletion task in progress for workflow [%s]",
                      workflowId);
                  publisher.publishOrThrow(
                      DeleteWorkflowJobEvent.create(workflowId, internalId, author),
                      "Failed to publish maestro delete job event for workflow: " + workflowId);
                  return internalId;
                }
                return null;
              }
            });
    if (res == null) {
      throw new MaestroNotFoundException(
          "No workflow is deleted because workflow [%s] is non-existing or has queued or running instances.",
          workflowId);
    }
    LOG.info(
        "User [{}] deleted workflow [{}] with a unique internalId [{}]. Send a delete job event to remove data",
        author.getName(),
        workflowId,
        res);
    return info;
  }

  @VisibleForTesting
  MaestroWorkflow getMaestroWorkflow(String workflowId) {
    return withRetryableQuery(
        GET_MAESTRO_WORKFLOW,
        stmt -> stmt.setString(1, workflowId),
        result -> {
          if (result.next()) {
            return maestroWorkflowFromResult(result);
          }
          return null;
        });
  }

  private MaestroWorkflow maestroWorkflowFromResult(ResultSet rs) throws SQLException {
    final long activeVersionId = rs.getLong(ACTIVE_VERSION_COLUMN);
    final String workflowId =
        Checks.notNull(rs.getString(WORKFLOW_ID_COLUMN), "workflow_id cannot be null");
    MaestroWorkflow.MaestroWorkflowBuilder builder =
        MaestroWorkflow.builder().workflowId(workflowId).activeVersionId(activeVersionId);
    if (activeVersionId > Constants.INACTIVE_VERSION_ID) {
      builder
          .activateTime(
              Checks.notNull(
                      rs.getTimestamp("activate_ts"),
                      "activate_ts cannot be null if there is an active version")
                  .getTime())
          .activatedBy(fromJson(rs.getString("activated_by"), User.class));
    }

    return builder
        .propertiesSnapshot(propertiesSnapshotFromResult(rs, workflowId))
        .latestVersionId(rs.getLong(LATEST_VERSION_COLUMN))
        .modifyTime(rs.getTimestamp(MODIFY_TS_COLUMN).getTime())
        .internalId(rs.getLong(INTERNAL_ID_COLUMN))
        .build();
  }

  private PropertiesSnapshot propertiesSnapshotFromResult(ResultSet rs, String workflowId)
      throws SQLException {
    return fromJson(
        Checks.notNull(
            rs.getString(PROPERTIES_COLUMN),
            "Properties in DB cannot be null for workflow [%s]",
            workflowId),
        PropertiesSnapshot.class);
  }

  private MaestroWorkflowVersion maestroWorkflowVersionFromResult(ResultSet rs)
      throws SQLException {
    return MaestroWorkflowVersion.builder()
        .metadata(convertJson(rs.getString(METADATA_COLUMN), Metadata.class))
        .definition(convertJson(rs.getString(DEFINITION_COLUMN), Workflow.class))
        .triggerUuids(convertJson(rs.getString(TRIGGER_UUIDS_COLUMN), TriggerUuids.class))
        .build();
  }

  private <T> T convertJson(String val, Class<T> clazz) {
    if (val == null) {
      return null;
    }
    return fromJson(val, clazz);
  }

  private int[] getIndexedInstanceCount(String workflowId, boolean withFailed) {
    return withRetryableQuery(
        withFailed
            ? GET_NONTERMINAL_FAILED_INSTANCE_COUNT_QUERY
            : GET_NONTERMINAL_INSTANCE_COUNT_QUERY,
        stmt -> stmt.setString(1, workflowId),
        result -> {
          int[] ret = {0, 0, 0}; // int[0]: created, int[1]: in_progress, int[2]: failed
          while (result.next()) {
            WorkflowInstance.Status status =
                WorkflowInstance.Status.create(result.getString(STATUS_COLUMN));
            int idx = 0;
            switch (status) {
              case IN_PROGRESS:
                idx = 1;
                break;
              case FAILED:
                idx = 2;
                break;
              case CREATED:
              default:
                break;
            }
            ret[idx] += result.getInt("cnt");
          }
          return ret;
        });
  }

  /**
   * Here is the current design about run strategy change.
   *
   * <p>1. when a run strategy is changed to first_only, last_only, or strict_sequential, maestro
   * will check if there are more than 1 non-terminal workflow instance runs. If no, will accept the
   * new run strategy, otherwise, it will reject the call and return a response including details
   * and instructions, e.g. how many running instances there and please inspect and manually take
   * some actions (stop) before pushing it again. This decision is based on the fact that this kind
   * of run strategy means users have a different expectation on the workflow and usually the
   * business logic might need to be changed as well. So we would like to ask users to take an
   * action instead automatically stopping or cancelling those workflow instances.
   *
   * <p>2. when a run strategy is changed to strict_sequential, if there are old failure instances
   * in the history, maestro will reject the call with a response including how many failures in the
   * history and instruct users to call UNBLOCK endpoint to clear up.
   *
   * <p>3. when checking strict_sequential, maestro will check the failed workflow instance runs in
   * the history. If a workflow instance is blocked due to a failed instance, maestro will keep that
   * info in the timeline.
   *
   * <p>Maestro provide an endpoint to unblock by workflow id to clean up the internal blocking
   * flags for those instances (it won’t change the instance status).
   */
  private PropertiesSnapshot updateWorkflowProps(
      Connection conn,
      String workflowId,
      User author,
      Long createTime,
      PropertiesSnapshot snapshot,
      Properties props,
      PropertiesUpdate update)
      throws SQLException {
    if (props == null) {
      return null;
    }
    PropertiesSnapshot newSnapshot =
        PropertiesSnapshot.create(
            workflowId,
            createTime,
            author,
            snapshot == null ? props : update.getNewProperties(props, snapshot));

    if (snapshot != null
        && props.getRunStrategy() != null
        && !Objects.equals(snapshot.getRunStrategy(), newSnapshot.getRunStrategy())) {
      boolean isStrict = props.getRunStrategy().getRule() == RunStrategy.Rule.STRICT_SEQUENTIAL;
      if (isStrict
          || props.getRunStrategy().getRule() == RunStrategy.Rule.FIRST_ONLY
          || props.getRunStrategy().getRule() == RunStrategy.Rule.LAST_ONLY) {
        int[] statusCount = getIndexedInstanceCount(workflowId, isStrict);
        if (!isStrict && statusCount[0] + statusCount[1] >= MORE_THAN_ONE_CONDITION) {
          throw new MaestroPreconditionFailedException(
              "Cannot update run strategy from %s to %s because there are %s nonterminal workflow instances. "
                  + "As at most ONE nonterminal workflow instance is allowed by %s, please review them "
                  + "and either manually stop them or wait for them to complete and then update the run strategy.",
              snapshot.getRunStrategy(),
              props.getRunStrategy(),
              statusCount[0] + statusCount[1],
              props.getRunStrategy());
        }
        if (isStrict && statusCount[2] > 0) {
          throw new MaestroPreconditionFailedException(
              "Cannot update run strategy from %s to %s because there are %s FAILED workflow instances. "
                  + "%s will be blocked if there is any failure in the history due to %s. Please review them "
                  + "and either manually UNBLOCK or RESTART those failed instances and then update the run strategy.",
              snapshot.getRunStrategy(),
              props.getRunStrategy(),
              statusCount[2],
              workflowId,
              props.getRunStrategy());
        }
      }
    }

    try (PreparedStatement stmt = conn.prepareStatement(CREATE_WORKFLOW_PROPS_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, workflowId);
      stmt.setLong(++idx, createTime);
      stmt.setString(++idx, toJson(author));
      stmt.setString(++idx, toJson(props));
      stmt.setString(++idx, toJson(snapshot));
      stmt.executeUpdate();
      return newSnapshot;
    }
  }

  /** this is used to lock the row to make sure only one update can happen. */
  private WorkflowInfo getWorkflowInfoForUpdate(Connection conn, String workflowId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(GET_CURRENT_WORKFLOW_INFO_FOR_UPDATE)) {
      stmt.setString(1, workflowId);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return new WorkflowInfo(
              result.getLong(ACTIVE_VERSION_COLUMN),
              propertiesSnapshotFromResult(result, workflowId),
              result.getLong(LATEST_VERSION_COLUMN));
        }
        // workflow does not exist
        return new WorkflowInfo(Constants.INACTIVE_VERSION_ID, null, Constants.INACTIVE_VERSION_ID);
      }
    }
  }

  /** Get the properties snapshot for a given workflow id and snapshot version. */
  public PropertiesSnapshot getWorkflowPropertiesSnapshot(String workflowId, String snapshotId) {
    if (Constants.LATEST_INSTANCE_RUN.equalsIgnoreCase(snapshotId)) {
      return getLatestPropertiesSnapshot(workflowId);
    } else {
      throw new UnsupportedOperationException("Specific snapshot version is not implemented.");
    }
  }

  /**
   * Get the latest properties-snapshot. If it is an inline workflow id, it returns its parent
   * workflow's latest properties-snapshot.
   */
  private PropertiesSnapshot getLatestPropertiesSnapshot(String workflowId) {
    if (IdHelper.isInlineWorkflowId(workflowId)) {
      return getInlineWorkflowDefinitionInternal(workflowId).getPropertiesSnapshot();
    }
    return getCurrentPropertiesSnapshot(workflowId);
  }

  /** Get the current properties snapshot from maestro_workflow table. */
  private PropertiesSnapshot getCurrentPropertiesSnapshot(String workflowId) {
    PropertiesSnapshot propertiesSnapshot =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    GET_CURRENT_PROPERTIES_SNAPSHOT,
                    stmt -> stmt.setString(1, workflowId),
                    result -> {
                      if (result.next()) {
                        return propertiesSnapshotFromResult(result, workflowId);
                      }
                      return null;
                    }),
            "getCurrentPropertiesSnapshot",
            "Failed getting current properties-snapshot for workflow [{}]",
            workflowId);
    if (propertiesSnapshot == null) {
      throw new MaestroNotFoundException(
          "Cannot find workflow [%s]'s current properties-snapshot. It has not been created yet or has been deleted.",
          workflowId);
    }
    return propertiesSnapshot;
  }

  /** Get the current run strategy from maestro_workflow table. */
  public RunStrategy getRunStrategy(String workflowId) {
    PropertiesSnapshot properties = getCurrentPropertiesSnapshot(workflowId);
    LOG.debug("Properties for workflow [{}] are {}", workflowId, properties);
    return ObjectHelper.valueOrDefault(properties.getRunStrategy(), Defaults.DEFAULT_RUN_STRATEGY);
  }

  private TriggerUuids insertMaestroWorkflowVersion(
      Connection conn, Metadata metadata, Workflow workflow) throws SQLException {
    final TriggerUuids triggerUuids = IdHelper.toTriggerUuids(workflow);
    final String metadataJson = toJson(metadata);
    final String workflowJson = toJson(workflow);
    final String triggerUuidsJson = toJson(triggerUuids);
    try (PreparedStatement stmt = conn.prepareStatement(CREATE_WORKFLOW_VERSION_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, metadataJson);
      stmt.setString(++idx, workflowJson);
      stmt.setString(++idx, triggerUuidsJson);
      int cnt = stmt.executeUpdate();
      Checks.checkTrue(
          cnt == SUCCESS_WRITE_SIZE, "insertMaestroWorkflowVersion expects to always return 1.");
    }
    return triggerUuids;
  }

  private long[] upsertMaestroWorkflow(Connection conn, WorkflowDefinition workflowDef)
      throws SQLException {
    final long nextVersionId = workflowDef.getMetadata().getWorkflowVersionId();
    final String workflowId = workflowDef.getWorkflow().getId();
    final PropertiesSnapshot snapshot = workflowDef.getPropertiesSnapshot();
    Checks.checkTrue(
        nextVersionId != INITIAL_ID || (snapshot != null && snapshot.getOwner() != null),
        "workflow properties and also owner must be set when creating the first version of a workflow [%s]",
        workflowId);

    List<StatementPreparer> preparers = new ArrayList<>();
    StringBuilder fields = prepareProperties(preparers, workflowId, snapshot);

    // if it is an active version, update the active_version_id
    if (workflowDef.getIsActive()) {
      prepareLongField(fields, ",active_version_id", preparers, nextVersionId);
      prepareTimestampField(fields, ",activate_ts", preparers, workflowDef.getActivateTime());
      prepareJsonbField(fields, ",activated_by", preparers, workflowDef.getActivatedBy());
    }
    prepareLongField(fields, ",latest_version_id", preparers, nextVersionId);

    // this is a new workflow creation.
    if (nextVersionId == Constants.INACTIVE_VERSION_ID + 1) {
      final long instanceId = getLatestWorkflowInstanceId(conn, workflowId);
      prepareLongField(fields, ",latest_instance_id", preparers, instanceId);
    }

    return executeTemplateUpdate(conn, fields, preparers);
  }

  /**
   * Execute DB update query. If the execution successfully updates DB, it returns the DB generated
   * modify time and internal id. If nothing is written into DB, it returns null.
   *
   * @param conn DB connection.
   * @param fields prepared fields.
   * @param preparers statement preparers.
   * @return DB generated modify time and internal id if execution updates DB successfully. Null if
   *     nothing is written to DB
   * @throws SQLException sql exception
   */
  private long[] executeTemplateUpdate(
      Connection conn, StringBuilder fields, List<StatementPreparer> preparers)
      throws SQLException {
    if (preparers.size() <= EMPTY_SIZE_CASE) {
      return null;
    }
    try (PreparedStatement stmt =
        conn.prepareStatement(getUpsertWorkflowQuery(fields, preparers))) {
      for (StatementPreparer preparer : preparers) {
        preparer.prepare(stmt);
      }
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return new long[] {
            result.getTimestamp(MODIFY_TS_COLUMN).getTime(), result.getLong(INTERNAL_ID_COLUMN)
          };
        }
        return null;
      }
    }
  }

  private String getUpsertWorkflowQuery(StringBuilder fields, List<StatementPreparer> preparers) {
    return String.format(
        UPSERT_WORKFLOW_QUERY_TEMPLATE,
        fields,
        String.join(",", Collections.nCopies(preparers.size(), "?")));
  }

  private MaestroJobEvent logToTimeline(
      Connection conn,
      WorkflowDefinition workflowDef,
      PropertiesSnapshot snapshot,
      long activeVersionId)
      throws SQLException {
    MaestroJobEvent jobEvent =
        WorkflowVersionUpdateJobEvent.create(workflowDef, snapshot, activeVersionId);
    try (PreparedStatement stmt = conn.prepareStatement(INSERT_TIMELINE_QUERY)) {
      stmt.setString(1, workflowDef.getWorkflow().getId());
      stmt.setString(2, toJson(jobEvent));
      stmt.executeUpdate();
    }
    return jobEvent;
  }

  private MaestroJobEvent logToTimeline(
      Connection conn, String workflowId, PropertiesSnapshot snapshot) throws SQLException {
    MaestroJobEvent jobEvent = WorkflowVersionUpdateJobEvent.create(workflowId, snapshot);
    try (PreparedStatement stmt = conn.prepareStatement(INSERT_TIMELINE_QUERY)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, toJson(jobEvent));
      stmt.executeUpdate();
    }
    return jobEvent;
  }

  private MaestroJobEvent logToTimeline(
      Connection conn,
      String workflowId,
      Long curActiveId,
      Long prevActiveId,
      User author,
      String log)
      throws SQLException {
    MaestroJobEvent jobEvent =
        WorkflowVersionUpdateJobEvent.create(workflowId, curActiveId, prevActiveId, author, log);
    try (PreparedStatement stmt = conn.prepareStatement(INSERT_TIMELINE_QUERY)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, toJson(jobEvent));
      stmt.executeUpdate();
    }
    return jobEvent;
  }

  private StringBuilder prepareProperties(
      List<StatementPreparer> preparers, String workflowId, PropertiesSnapshot snapshot) {
    StringBuilder fields = new StringBuilder();
    prepareStringField(fields, WORKFLOW_ID_COLUMN, preparers, workflowId);
    if (snapshot != null) {
      prepareJsonbField(fields, ",properties_snapshot", preparers, snapshot);
    }
    return fields;
  }

  private int getIndex(
      StringBuilder fields, String fieldName, List<StatementPreparer> preparers, Object data) {
    if (data == null) {
      return 0;
    }
    fields.append(fieldName);
    return preparers.size() + 1;
  }

  private void prepareJsonbField(
      StringBuilder fields, String fieldName, List<StatementPreparer> preparers, Object data) {
    final int idx = getIndex(fields, fieldName, preparers, data);
    if (idx > 0) {
      preparers.add(stmt -> stmt.setString(idx, toJson(data)));
    }
  }

  private void prepareStringField(
      StringBuilder fields, String fieldName, List<StatementPreparer> preparers, String data) {
    final int idx = getIndex(fields, fieldName, preparers, data);
    if (idx > 0) {
      preparers.add(stmt -> stmt.setString(idx, data));
    }
  }

  private void prepareLongField(
      StringBuilder fields, String fieldName, List<StatementPreparer> preparers, Long data) {
    final int idx = getIndex(fields, fieldName, preparers, data);
    if (idx > 0) {
      preparers.add(stmt -> stmt.setLong(idx, data));
    }
  }

  private void prepareTimestampField(
      StringBuilder fields, String fieldName, List<StatementPreparer> preparers, Long data) {
    final int idx = getIndex(fields, fieldName, preparers, data);
    if (idx > 0) {
      preparers.add(stmt -> stmt.setTimestamp(idx, new Timestamp(data)));
    }
  }

  /**
   * Deactivate the workflow with a provided workflow id.
   *
   * @param workflowId workflow id
   * @param caller caller info
   * @return timeline info
   */
  public String deactivate(String workflowId, User caller) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  long versionId = deactivate(conn, workflowId, caller);
                  String timeline;
                  if (versionId == Constants.INACTIVE_VERSION_ID) {
                    timeline =
                        String.format(
                            "Caller [%s] do nothing as there is no active workflow version for [%s]",
                            caller.getName(), workflowId);
                  } else {
                    timeline =
                        String.format(
                            "Caller [%s] deactivated workflow [%s], whose last active version is [%s]",
                            caller.getName(), workflowId, versionId);
                  }
                  MaestroJobEvent jobEvent =
                      logToTimeline(conn, workflowId, null, versionId, caller, timeline);
                  if (versionId != Constants.INACTIVE_VERSION_ID) {
                    // no need to inform signal service or cron service about it
                    publisher.publishOrThrow(
                        jobEvent, "Failed to publish maestro deactivation job event.");
                  }
                  return timeline;
                }),
        "deactivate",
        "Failed to activate workflow [{}]",
        workflowId);
  }

  private long deactivate(Connection conn, String workflowId, User caller) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(DEACTIVATE_WORKFLOW_QUERY)) {
      int idx = 0;
      stmt.setString(++idx, toJson(caller));
      stmt.setString(++idx, workflowId);
      stmt.setString(++idx, workflowId);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return result.getLong(ACTIVE_VERSION_COLUMN);
        }
        throw new MaestroNotFoundException(
            "Cannot deactivate workflow [%s] as it is not found", workflowId);
      }
    }
  }

  /**
   * Activate a specific workflow version if feasible. It won't change the owner info.
   *
   * @param workflowId workflow id
   * @param version workflow version to activate
   * @param caller caller info
   * @return the timeline info
   */
  public MaestroJobEvent activate(String workflowId, String version, User caller) {
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                conn -> {
                  WorkflowInfo activatedResult = activate(conn, workflowId, version, caller);
                  String timeline;
                  if (activatedResult.withWorkflow()) {
                    timeline =
                        String.format(
                            "Caller [%s] activates workflow version [%s][%s], previous active version is [%s]",
                            caller.getName(),
                            workflowId,
                            activatedResult.getActiveVersionId(),
                            activatedResult.getPrevActiveVersionId());
                  } else {
                    timeline =
                        String.format(
                            "Caller [%s] do nothing as workflow version [%s][%s] is already active",
                            caller.getName(), workflowId, activatedResult.getPrevActiveVersionId());
                  }

                  MaestroJobEvent jobEvent =
                      logToTimeline(
                          conn,
                          workflowId,
                          activatedResult.getActiveVersionId(),
                          activatedResult.getPrevActiveVersionId(),
                          caller,
                          timeline);

                  if (activatedResult.withWorkflow()) {
                    addWorkflowTriggersIfNeeded(conn, activatedResult);

                    publisher.publishOrThrow(
                        jobEvent, "Failed to publish maestro activation job event.");
                  }
                  return jobEvent;
                }),
        "activate",
        "Failed to activate workflow version [{}][{}]",
        workflowId,
        version);
  }

  @Getter
  private static final class WorkflowInfo {
    private final long prevActiveVersionId;
    @Nullable private final PropertiesSnapshot prevPropertiesSnapshot;
    private final long latestVersionId;

    private long activeVersionId;
    private Workflow workflow;
    private PropertiesSnapshot propertiesSnapshot;
    private TriggerUuids triggerUuids;

    private WorkflowInfo(
        long activeVersionId, PropertiesSnapshot propertiesSnapshot, long latestVersionId) {
      this.prevActiveVersionId = activeVersionId;
      this.prevPropertiesSnapshot = propertiesSnapshot;
      this.latestVersionId = latestVersionId;
    }

    private WorkflowInfo(MaestroWorkflow workflow) {
      this(
          workflow.getActiveVersionId(),
          workflow.getPropertiesSnapshot(),
          workflow.getLatestVersionId());
    }

    private void setNextActiveWorkflow(MaestroWorkflowVersion mwv, PropertiesSnapshot snapshot) {
      this.activeVersionId = mwv.getMetadata().getWorkflowVersionId();
      this.workflow = mwv.getDefinition();
      this.propertiesSnapshot = snapshot;
      this.triggerUuids = mwv.getTriggerUuids();
    }

    private boolean withWorkflow() {
      return workflow != null;
    }
  }

  private WorkflowInfo activate(Connection conn, String workflowId, String version, User caller)
      throws SQLException {
    WorkflowInfo workflowInfo = getWorkflowInfoForUpdate(conn, workflowId);
    if (workflowInfo.getLatestVersionId() == Constants.INACTIVE_VERSION_ID) {
      throw new MaestroNotFoundException("Cannot find workflow [%s]", workflowId);
    }

    long versionId = deriveVersionId(workflowId, version, workflowInfo);
    if (workflowInfo.getPrevActiveVersionId() == versionId) {
      return workflowInfo;
    }

    try (PreparedStatement stmt = conn.prepareStatement(ACTIVATE_WORKFLOW_VERSION_QUERY)) {
      int idx = 0;
      stmt.setLong(++idx, versionId);
      stmt.setString(++idx, toJson(caller));
      stmt.setString(++idx, workflowId);
      int cnt = stmt.executeUpdate();
      Checks.checkTrue(
          cnt == SUCCESS_WRITE_SIZE,
          "activate workflow [%s][%s] expects to always return 1.",
          workflowId,
          versionId);
    }

    updateWorkflowInfoForNextActiveWorkflow(
        conn, workflowId, versionId, workflowInfo, workflowInfo.getPrevPropertiesSnapshot());
    return workflowInfo;
  }

  /**
   * Get workflow overview including version info and all instance status (partial) stats for a
   * specific workflow.
   *
   * <p>As stats is directly queried from a partial index for all instance status in
   * ('CREATED','IN_PROGRESS','PAUSED','FAILED'), its performance cost should be acceptable in most
   * of the cases.
   *
   * @param workflowId workflow id
   * @return WorkflowOverviewResponse for the workflow.
   */
  public WorkflowOverviewResponse getWorkflowOverview(@NotNull String workflowId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_WORKFLOW_OVERVIEW_QUERY,
                stmt -> {
                  stmt.setString(1, workflowId);
                  stmt.setString(2, workflowId);
                },
                result -> {
                  if (result.next()) {
                    return workflowOverviewFromResult(workflowId, result);
                  }
                  throw new MaestroNotFoundException(
                      "Cannot find workflow [%s], which is either not created or has been deleted.",
                      workflowId);
                }),
        "getWorkflowOverview",
        "Failed to get the workflow overview for [{}]",
        workflowId);
  }

  private WorkflowOverviewResponse workflowOverviewFromResult(
      @NotNull String workflowId, ResultSet result) throws SQLException {
    WorkflowOverviewResponse.WorkflowOverviewResponseBuilder builder =
        WorkflowOverviewResponse.builder().workflowId(workflowId);
    long activeVersion = result.getLong(ACTIVE_VERSION_COLUMN);
    if (activeVersion >= INITIAL_ID) {
      builder.activeVersionId(activeVersion);
      builder.defaultVersionId(activeVersion);
    }
    long latestVersion = result.getLong(LATEST_VERSION_COLUMN);
    if (latestVersion >= INITIAL_ID) {
      builder.latestVersionId(latestVersion);
      if (activeVersion < INITIAL_ID) {
        builder.defaultVersionId(latestVersion);
      }
    }
    long latestInstanceId = result.getLong(LATEST_INSTANCE_COLUMN);
    if (latestInstanceId >= INITIAL_ID) {
      builder.latestInstanceId(latestInstanceId);
    }
    PropertiesSnapshot snapshot = propertiesSnapshotFromResult(result, workflowId);
    Checks.checkTrue(
        workflowId.equals(snapshot.getWorkflowId()),
        "properties workflow id [%s] does not match the workflow [%s]",
        snapshot.getWorkflowId(),
        workflowId);
    if (snapshot.getRunStrategy() == null) {
      // enrich it with default run strategy.
      snapshot = snapshot.toBuilder().runStrategy(Defaults.DEFAULT_RUN_STRATEGY).build();
    }
    builder.propertiesSnapshot(snapshot);
    builder.stepConcurrency(snapshot.getStepConcurrency());

    String statsString = result.getString(STATUS_COLUMN);
    Map<String, Long> stats;
    if (statsString != null) {
      stats = fromJson(statsString, STATUS_STATS_REFERENCE);
    } else {
      stats = Collections.emptyMap();
    }
    EnumMap<WorkflowInstance.Status, Long> statusStats =
        new EnumMap<>(WorkflowInstance.Status.class);
    statusStats.put(
        WorkflowInstance.Status.CREATED,
        stats.getOrDefault(WorkflowInstance.Status.CREATED.name(), 0L));
    statusStats.put(
        WorkflowInstance.Status.IN_PROGRESS,
        stats.getOrDefault(WorkflowInstance.Status.IN_PROGRESS.name(), 0L));
    statusStats.put(
        WorkflowInstance.Status.PAUSED,
        stats.getOrDefault(WorkflowInstance.Status.PAUSED.name(), 0L));
    builder.nonterminalInstances(statusStats);
    builder.failedInstances(stats.getOrDefault(WorkflowInstance.Status.FAILED.name(), 0L));
    return builder.build();
  }

  /**
   * Get param definition for a given param name of workflows with the given id prefix. To ensure
   * the performance, we put hard limit of 20 workflow ids at maximum to be returned by the prefix
   * search.
   *
   * @param workflowIdPrefix workflow id prefix to search
   * @param paramName param name
   * @return a map with workflow id as key and param definition as value
   */
  public Map<String, ParamDefinition> getParamFromWorkflows(
      String workflowIdPrefix, String paramName) {
    Map<String, ParamDefinition> workflowParams = new HashMap<>();
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_WORKFLOW_PARAM_FOR_PREFIX_QUERY,
                stmt -> {
                  stmt.setString(1, paramName);
                  stmt.setString(2, workflowIdPrefix + "%");
                },
                result -> {
                  while (result.next()) {
                    String val = result.getString(PAYLOAD_COLUMN);
                    if (val != null) {
                      workflowParams.put(
                          result.getString(ID_COLUMN), fromJson(val, ParamDefinition.class));
                    }
                  }
                  return workflowParams;
                }),
        "getParamFromWorkflows",
        "Failed to get the workflow param [{}] from workflows with the prefix [{}]",
        paramName,
        workflowIdPrefix);
  }

  /**
   * Get workflow timeline's most recent events (max {@link Constants#TIMELINE_EVENT_SIZE_LIMIT}).
   *
   * @param workflowId workflow id
   * @return the workflow timeline
   */
  public WorkflowTimeline getWorkflowTimeline(String workflowId) {
    List<WorkflowVersionUpdateJobEvent> jobEvents =
        withMetricLogError(
            () ->
                getPayloads(
                    GET_WORKFLOW_TIMELINE_QUERY,
                    stmt -> {
                      stmt.setString(1, workflowId);
                    },
                    WorkflowVersionUpdateJobEvent.class),
            "getWorkflowTimeline",
            "Failed getting timeline events for workflow id [{}]",
            workflowId);

    return new WorkflowTimeline(
        workflowId,
        jobEvents.stream()
            .map(
                e ->
                    new WorkflowTimeline.WorkflowTimelineEvent(
                        e.getAuthor(), e.getLog(), e.getEventTime()))
            .collect(Collectors.toList()));
  }

  /**
   * Batch scanner for workflow Definition.
   *
   * @param workflowId workflow id
   * @param versionId version id
   * @param limit limit
   * @return a list of workflow definitions
   */
  public List<WorkflowDefinition> scanWorkflowDefinition(
      String workflowId, long versionId, int limit) {
    List<WorkflowDefinition> toRet = new ArrayList<>();
    List<MaestroWorkflowVersion> workflowVersions =
        scanWorkflowVersions(workflowId, versionId, limit);
    Map<String, MaestroWorkflow> idToWorkflow = new HashMap<>();
    // pick the first and last workflow version to limit the scanner on workflow's.
    if (!workflowVersions.isEmpty()) {
      String firstWorkflowID = workflowVersions.get(0).getMetadata().getWorkflowId();
      withRetryableQuery(
          GET_MAESTRO_WORKFLOW,
          stmt -> stmt.setString(1, firstWorkflowID),
          rs -> {
            while (rs.next()) {
              MaestroWorkflow wf = maestroWorkflowFromResult(rs);
              idToWorkflow.put(wf.getWorkflowId(), wf);
            }
            return idToWorkflow;
          });

      for (MaestroWorkflowVersion mwv : workflowVersions) {
        MaestroWorkflow mw = idToWorkflow.get(mwv.getMetadata().getWorkflowId());
        // clone the MaestroWorkflow.
        toRet.add(
            MaestroWorkflow.builder()
                .propertiesSnapshot(mw.getPropertiesSnapshot())
                .activatedBy(mw.getActivatedBy())
                .activeVersionId(mw.getActiveVersionId())
                .latestVersionId(mw.getLatestVersionId())
                .activateTime(mw.getActivateTime())
                .workflowId(mw.getWorkflowId())
                .internalId(mw.getInternalId())
                .definition(mwv.getDefinition())
                .metadata(mwv.getMetadata())
                .modifyTime(mw.getModifyTime())
                .build()
                .toDefinition());
      }
    }
    return toRet;
  }

  /**
   * Batch Scanner for the workflow versions.
   *
   * @param workflowId workflow id
   * @param versionId version id
   * @param limit limit
   * @return a list of workflow versions
   */
  private List<MaestroWorkflowVersion> scanWorkflowVersions(
      String workflowId, long versionId, int limit) {
    Checks.checkTrue(
        workflowId != null, "workflowId can not be null while scanning workflow versions");
    List<MaestroWorkflowVersion> toReturn = new ArrayList<>();
    if (versionId > 1) {
      withRetryableQuery(
          WORKFLOW_VERSION_SCAN_WITH_WORKFLOW_VERSION_FILTER_QUERY,
          stmt -> {
            int idx = 0;
            stmt.setString(++idx, workflowId);
            stmt.setLong(++idx, versionId);
            stmt.setInt(++idx, limit);
          },
          result -> processWorkflowVersionResultSet(result, toReturn));
      return toReturn;
    } else {
      if (versionId == 1) {
        return toReturn; // no need to scan further as consumer has already got the last record.
      }
      return withRetryableQuery(
          WORKFLOW_VERSION_SCAN_WITH_WORKFLOW_FILTER_QUERY,
          stmt -> {
            int idx = 0;
            stmt.setString(++idx, workflowId);
            stmt.setInt(++idx, limit);
          },
          result -> processWorkflowVersionResultSet(result, toReturn));
    }
  }

  private List<MaestroWorkflowVersion> processWorkflowVersionResultSet(
      ResultSet rs, List<MaestroWorkflowVersion> appendTo) throws SQLException {
    while (rs.next()) {
      try {
        MaestroWorkflowVersion.MaestroWorkflowVersionBuilder builder =
            MaestroWorkflowVersion.builder();
        builder.metadata(convertJson(rs.getString(METADATA_COLUMN), Metadata.class));
        builder.definition(convertJson(rs.getString(DEFINITION_COLUMN), Workflow.class));
        // batch endpoint does not need TriggerUuids.
        appendTo.add(builder.build());
      } catch (Exception e) {
        // swallow any exception during the deserialization. This is added to keep batch api to
        // tolerate incompatible data model changes.
        LOG.warn("unable to deserialize MaestroWorkflowVersion", e);
      }
    }
    return appendTo;
  }

  /**
   * If the workflow is brand new, consider the latestInstanceId in the deleted workflow with the
   * same workflowId and choose the largest one.
   */
  private long getLatestWorkflowInstanceId(Connection conn, String workflowId) throws SQLException {
    try (PreparedStatement stmt =
        conn.prepareStatement(GET_LATEST_WORKFLOW_INSTANCE_ID_FROM_WORKFLOW_DELETED_QUERY)) {
      stmt.setString(1, workflowId);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          return result.getLong(ID_COLUMN);
        }
        return Constants.LATEST_ONE;
      }
    }
  }
}
