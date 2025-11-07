/*
 * Copyright 2025 Netflix, Inc.
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
import com.netflix.maestro.engine.dto.StepTagPermit;
import com.netflix.maestro.engine.dto.StepUuidSeq;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineActionEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

/** DAO for managing tag permit. */
public class MaestroTagPermitDao extends AbstractDatabaseDao {
  private static final String UPSERT_TAG_PERMIT_QUERY =
      "INSERT INTO maestro_tag_permit (tag,max_allowed,timeline) VALUES (?,?,ARRAY[?]) ON CONFLICT (tag) DO UPDATE "
          + "SET status=0,max_allowed=EXCLUDED.max_allowed,timeline = CASE "
          + "WHEN array_length(maestro_tag_permit.timeline, 1)>=32 "
          + "THEN array_append(maestro_tag_permit.timeline[2:32],?) "
          + "ELSE array_append(maestro_tag_permit.timeline,?) END";
  private static final String MARK_REMOVE_TAG_PERMIT_QUERY =
      "UPDATE maestro_tag_permit SET status=1 WHERE tag=?";
  private static final String GET_TAG_PERMIT_QUERY =
      "SELECT max_allowed,timeline FROM maestro_tag_permit WHERE tag=?";

  private static final String GET_TAG_PERMITS_QUERY =
      "SELECT tag,max_allowed FROM maestro_tag_permit where TAG>? AND status>=6 ORDER BY tag ASC LIMIT ?";
  private static final String REMOVE_MARKED_TAG_PERMITS_QUERY =
      "DELETE from maestro_tag_permit WHERE tag IN "
          + "(SELECT tag FROM maestro_tag_permit WHERE status=1 LIMIT ?) RETURNING tag";
  private static final String MARK_TAG_PERMITS_SYNCED_QUERY =
      "UPDATE maestro_tag_permit SET status=6 WHERE tag IN "
          + "(SELECT tag FROM maestro_tag_permit WHERE status=0 LIMIT ?) RETURNING tag,max_allowed";

  private static final String GET_STEP_TAG_PERMIT_QUERY =
      "SELECT status from maestro_step_tag_permit WHERE uuid=?";
  private static final String ADD_STEP_TAG_PERMIT_QUERY =
      "INSERT INTO maestro_step_tag_permit (uuid,tags,limits,timeline) VALUES (?,?,?,ARRAY[?]) ON CONFLICT DO NOTHING";
  private static final String RELEASE_STEP_TAG_PERMIT_QUERY =
      "UPDATE maestro_step_tag_permit SET status=1 WHERE uuid=?";

  private static final String GET_STEP_TAG_PERMITS_PREFIX =
      "SELECT uuid,seq_num,status,tags,limits,timeline[1] FROM maestro_step_tag_permit WHERE status>=6 ";
  private static final String GET_FIRST_STEP_TAG_PERMITS_QUERY =
      GET_STEP_TAG_PERMITS_PREFIX + "ORDER BY uuid ASC LIMIT ?";
  private static final String GET_STEP_TAG_PERMITS_QUERY =
      GET_STEP_TAG_PERMITS_PREFIX + "AND uuid>? ORDER BY uuid ASC LIMIT ?";
  private static final String REMOVE_RELEASED_STEP_TAG_PERMITS_QUERY =
      "DELETE from maestro_step_tag_permit WHERE uuid IN "
          + "(SELECT uuid FROM maestro_step_tag_permit WHERE status=1 LIMIT ?) RETURNING uuid,seq_num";

  private static final String MARK_STEP_TAG_PERMITS_SYNCED_QUERY =
      "UPDATE maestro_step_tag_permit SET (status,seq_num)=(6,ranked.sn) FROM "
          + "(SELECT uuid,ROW_NUMBER() OVER (ORDER BY create_ts ASC)+? as sn FROM maestro_step_tag_permit "
          + " WHERE status=0 order by create_ts ASC LIMIT ?) ranked WHERE maestro_step_tag_permit.uuid = ranked.uuid "
          + "RETURNING maestro_step_tag_permit.uuid,seq_num,status,tags,limits,timeline[1]";
  private static final String MARK_STEP_TAG_PERMITS_ACQUIRED_QUERY =
      "UPDATE maestro_step_tag_permit SET (status,acquire_ts)=(7,NOW()) WHERE uuid=? AND status=6";

  private record TagPermitRecord(int maxAllowed, String[] events) {}

  /** Constructor for OutputDataDAO. */
  public MaestroTagPermitDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
  }

  /**
   * Upsert a tag permit with the given limit. If the tag permit already exists, update it to the
   * new limit
   *
   * @param tag tag name
   * @param limit max allowed limit
   * @param user user who made the change
   */
  public void upsertTagPermit(String tag, int limit, User user) {
    String timelineForInsert =
        toJson(
            TimelineLogEvent.info(
                "User [%s] created the tag permit with limit [%s]", user.getName(), limit));
    String timelineForUpdate =
        toJson(
            TimelineLogEvent.info(
                "User [%s] updated the tag permit to the new limit [%s]", user.getName(), limit));
    withMetricLogError(
        () ->
            withRetryableUpdate(
                UPSERT_TAG_PERMIT_QUERY,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, tag);
                  stmt.setInt(++idx, limit);
                  stmt.setString(++idx, timelineForInsert);
                  stmt.setString(++idx, timelineForUpdate);
                  stmt.setString(++idx, timelineForUpdate);
                }),
        "upsertTagPermit",
        "Failed upserting tag permit for tag: [{}] with limit: [{}]",
        tag,
        limit);
  }

  /**
   * Mark tag permit as removed. It will be deleted async by the tag permit task.
   *
   * @param tag tag name
   */
  public boolean markRemoveTagPermit(String tag) {
    return withMetricLogError(
            () -> withRetryableUpdate(MARK_REMOVE_TAG_PERMIT_QUERY, stmt -> stmt.setString(1, tag)),
            "markRemoveTagPermit",
            "Failed marking to-be-removed tag permit for tag: [{}]",
            tag)
        == SUCCESS_WRITE_SIZE;
  }

  /**
   * Get tag permit.
   *
   * @param tag tag name
   */
  public TagPermit getTagPermit(String tag) {
    var res =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    GET_TAG_PERMIT_QUERY,
                    stmt -> stmt.setString(1, tag),
                    rs -> {
                      if (rs.next()) {
                        Array array = rs.getArray(2);
                        try {
                          return new TagPermitRecord(rs.getInt(1), (String[]) array.getArray());
                        } finally {
                          array.free();
                        }
                      } else {
                        return null;
                      }
                    }),
            "getTagPermit",
            "Failed get tag permit for tag: [{}]",
            tag);
    if (res != null) {
      return new TagPermit(
          tag,
          res.maxAllowed(),
          new Timeline(
              Arrays.stream(res.events()).map(s -> fromJson(s, TimelineEvent.class)).toList()));
    }
    return null;
  }

  /**
   * Return the step tag permit status code of a step instance.
   *
   * @param uuid step uuid
   * @return status, -1 if not found, 7 is acquired, otherwise, still waiting
   */
  public int getStepTagPermitStatus(UUID uuid) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_STEP_TAG_PERMIT_QUERY,
                stmt -> stmt.setObject(1, uuid),
                rs -> {
                  if (rs.next()) {
                    return rs.getInt(1);
                  } else {
                    return -1;
                  }
                }),
        "getStepTagPermitStatus",
        "Failed getting step tag permit acquisition status for uuid: [{}]",
        uuid);
  }

  /**
   * Insert a tag permit status record with the given uuid, with initial status of 0 (unknown).
   *
   * @param uuid step uuid
   */
  public boolean insertStepTagPermit(List<Tag> tagsList, UUID uuid, TimelineEvent event) {
    String[] tags = tagsList.stream().map(Tag::getName).toArray(String[]::new);
    Integer[] limits = tagsList.stream().map(Tag::getPermit).toArray(Integer[]::new);
    String payload = toJson(event);
    return withMetricLogError(
        () ->
            withRetryableTransaction(
                (conn) -> {
                  try (PreparedStatement stmt = conn.prepareStatement(ADD_STEP_TAG_PERMIT_QUERY)) {
                    int idx = 0;
                    stmt.setObject(++idx, uuid);
                    stmt.setArray(++idx, conn.createArrayOf(ARRAY_TYPE_NAME, tags));
                    stmt.setArray(++idx, conn.createArrayOf("INT4", limits));
                    stmt.setString(++idx, payload);
                    return stmt.executeUpdate() == SUCCESS_WRITE_SIZE;
                  }
                }),
        "insertStepTagPermit",
        "Failed to insert the tags [{}] for step uuid [{}]",
        tagsList,
        uuid);
  }

  /**
   * Mark tag permit status to be released. It will be removed async by the tag permit task.
   *
   * @param uuid step uuid
   */
  public boolean releaseStepTagPermit(UUID uuid) {
    return withMetricLogError(
            () ->
                withRetryableUpdate(RELEASE_STEP_TAG_PERMIT_QUERY, stmt -> stmt.setObject(1, uuid)),
            "releaseStepTagPermit",
            "Failed releasing step tag permit status for uuid: [{}]",
            uuid)
        == SUCCESS_WRITE_SIZE;
  }

  /**
   * Get synced tag permits starting from the given cursor (exclusive) with the given limit.
   *
   * @param cursor the last tag from previous batch, pass empty string for the first batch
   * @param limit maximum number of records to return
   * @return list of tag permits
   */
  public List<TagPermit> getSyncedTagPermits(String cursor, int limit) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_TAG_PERMITS_QUERY,
                stmt -> {
                  stmt.setString(1, cursor);
                  stmt.setInt(2, limit);
                },
                rs -> {
                  List<TagPermit> res = new ArrayList<>();
                  while (rs.next()) {
                    int idx = 0;
                    res.add(new TagPermit(rs.getString(++idx), rs.getInt(++idx), null));
                  }
                  return res;
                }),
        "getSyncedTagPermits",
        "Failed getting synced tag permits starting from cursor [{}] with limit [{}]",
        cursor,
        limit);
  }

  /**
   * Remove all tag permits marked as removed.
   *
   * @param limit maximum number of records to remove
   * @return the tags of deleted tag permits
   */
  public List<String> removeTagPermits(int limit) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                REMOVE_MARKED_TAG_PERMITS_QUERY,
                stmt -> stmt.setInt(1, limit),
                rs -> {
                  List<String> res = new ArrayList<>();
                  while (rs.next()) {
                    res.add(rs.getString(1));
                  }
                  return res;
                }),
        "removeTagPermits",
        "Failed removing marked tag permit with a limit: [{}]",
        limit);
  }

  /**
   * Mark and load a batch of tag permits to be synced, up to the given limit.
   *
   * @param limit maximum number of records to mark and load
   * @return list of tag permits
   */
  public List<TagPermit> markAndLoadTagPermits(int limit) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                MARK_TAG_PERMITS_SYNCED_QUERY,
                stmt -> stmt.setInt(1, limit),
                rs -> {
                  List<TagPermit> res = new ArrayList<>();
                  while (rs.next()) {
                    res.add(new TagPermit(rs.getString(1), rs.getInt(2), null));
                  }
                  return res;
                }),
        "markAndLoadTagPermits",
        "Failed marking and loading updated tag permit with a limit: [{}]",
        limit);
  }

  /**
   * Load step tag permit records starting from the given cursor (exclusive) with the given limit.
   *
   * @param cursor the last uuid from previous batch, pass null for the first batch
   * @param limit maximum number of records to return
   * @return list of step tag permits
   */
  public List<StepTagPermit> loadStepTagPermits(UUID cursor, int limit) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                cursor == null ? GET_FIRST_STEP_TAG_PERMITS_QUERY : GET_STEP_TAG_PERMITS_QUERY,
                stmt -> {
                  int idx = 0;
                  if (cursor != null) {
                    stmt.setObject(++idx, cursor);
                  }
                  stmt.setInt(++idx, limit);
                },
                rs -> {
                  List<StepTagPermit> res = new ArrayList<>();
                  stepTagPermitsFromResult(rs, res);
                  return res;
                }),
        "loadStepTagPermits",
        "Failed loading synced tag permits starting from cursor [{}] with limit [{}]",
        cursor,
        limit);
  }

  /**
   * remove all already-released step tag permit records.
   *
   * @return the uuids of deleted records.
   */
  public List<StepUuidSeq> removeReleasedStepTagPermits(int limit) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                REMOVE_RELEASED_STEP_TAG_PERMITS_QUERY,
                stmt -> stmt.setInt(1, limit),
                rs -> {
                  List<StepUuidSeq> res = new ArrayList<>();
                  while (rs.next()) {
                    res.add(new StepUuidSeq(rs.getObject(1, UUID.class), rs.getLong(2)));
                  }
                  return res;
                }),
        "removeReleasedStepTagPermits",
        "Failed removing released step tag permit records with a limit: [{}]",
        limit);
  }

  /**
   * Mark and load a batch of step tag permits to be synced, up to the given limit.
   *
   * @param maxSeqNum the maximum known seq_num
   * @param limit maximum number of records to mark and load
   * @return list of step tag permits
   */
  public List<StepTagPermit> markAndLoadStepTagPermits(long maxSeqNum, int limit) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                MARK_STEP_TAG_PERMITS_SYNCED_QUERY,
                stmt -> {
                  stmt.setLong(1, maxSeqNum);
                  stmt.setInt(2, limit);
                },
                rs -> {
                  List<StepTagPermit> res = new ArrayList<>();
                  stepTagPermitsFromResult(rs, res);
                  return res;
                }),
        "markAndLoadStepTagPermits",
        "Failed marking and loading step tag permits from seq [{}] with a limit [{}]",
        maxSeqNum,
        limit);
  }

  private void stepTagPermitsFromResult(ResultSet rs, List<StepTagPermit> res) throws SQLException {
    while (rs.next()) {
      int idx = 0;
      UUID uuid = rs.getObject(++idx, UUID.class);
      long seqNum = rs.getLong(++idx);
      int status = rs.getInt(++idx);
      Array tagArray = rs.getArray(++idx);
      Array limitArray = rs.getArray(++idx);
      TimelineActionEvent event = fromJson(rs.getString(++idx), TimelineActionEvent.class);
      try {
        res.add(
            new StepTagPermit(
                uuid,
                seqNum,
                status,
                (String[]) tagArray.getArray(),
                (Integer[]) limitArray.getArray(),
                event));
      } finally {
        tagArray.free();
        limitArray.free();
      }
    }
  }

  /**
   * Mark step tag permit status to be acquired.
   *
   * @param uuid step uuid
   * @return whether there is an update on a record
   */
  public boolean markStepTagPermitAcquired(UUID uuid) {
    return withMetricLogError(
            () ->
                withRetryableUpdate(
                    MARK_STEP_TAG_PERMITS_ACQUIRED_QUERY, stmt -> stmt.setObject(1, uuid)),
            "markStepTagPermitAcquired",
            "Failed marking step tag permit acquisition status for uuid: [{}]",
            uuid)
        == SUCCESS_WRITE_SIZE;
  }
}
