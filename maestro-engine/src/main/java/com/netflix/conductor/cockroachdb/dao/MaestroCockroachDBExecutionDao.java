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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.maestro.engine.compression.JsonConverter;
import com.netflix.maestro.engine.compression.StringCodec;
import com.netflix.maestro.engine.properties.MaestroConductorProperties;
import com.netflix.maestro.utils.HashHelper;
import java.util.AbstractMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** CockroachDB implementation of Execution DAO optimized for Maestro use case. */
@Slf4j
public class MaestroCockroachDBExecutionDao extends CockroachDBExecutionDAO {
  /**
   * Here, this implementation uses an un-used task field `workerId` to store the `checksum` string.
   * Ideally, we should have a separate column and data field in the class for that. Doing this
   * hacky way to reduce the amount of effort for this throw-away work.
   */
  private static final String GET_TASK_CHECKSUM_UPDATE_TIME_QUERY =
      "SELECT payload->>'workerId' as id, payload->>'updateTime' as payload FROM task where task_id=?";

  private final long maxTaskUpdateInterval;
  private final JsonConverter jsonConverter;

  public MaestroCockroachDBExecutionDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      StringCodec stringCodec,
      IndexDAO indexDAO,
      MaestroConductorProperties properties) {
    super(dataSource, indexDAO, objectMapper, properties);
    this.jsonConverter =
        new JsonConverter(
            objectMapper,
            stringCodec,
            properties.isCompressionEnabled(),
            properties.getCompressorName());
    this.maxTaskUpdateInterval = properties.getMaxTaskUpdateIntervalInMillis();
  }

  /**
   * Improve update task for maestro use case by separating polling interval and DB update interval.
   * By checking checksum from DB, it can avoid unnecessary DB update.
   */
  @Override
  public void updateTask(Task task) {
    Map.Entry<String, Long> taskInDb = getTaskChecksumAndUpdateTime(task.getTaskId());
    String taskCheckSum = computeChecksum(task);
    if (taskInDb != null) {
      long updateInterval = task.getUpdateTime() - taskInDb.getValue();
      if (taskCheckSum.equals(taskInDb.getKey()) && updateInterval < maxTaskUpdateInterval) {
        LOG.debug(
            "task has the same checksum and update interval {} is less than max interval {} millis and skip update",
            updateInterval,
            maxTaskUpdateInterval);
        return;
      }
      LOG.info(
          "update task [{}] with checksum=[{}] with an update interval=[{}]",
          task.getTaskId(),
          taskCheckSum,
          updateInterval);
    }

    task.setWorkerId(taskCheckSum);
    super.updateTask(task);
  }

  @Override
  @SneakyThrows(JsonProcessingException.class)
  protected <T> T fromJson(String json, Class<T> clazz) {
    return jsonConverter.fromJson(json, clazz);
  }

  @Override
  @SneakyThrows(JsonProcessingException.class)
  protected <T> T fromJson(String json, TypeReference<T> valueTypeRef) {
    return jsonConverter.fromJson(json, valueTypeRef);
  }

  @Override
  @SneakyThrows(JsonProcessingException.class)
  protected String toJson(Object value) {
    return jsonConverter.toJson(value);
  }

  private String computeChecksum(Task task) {
    int pollCount = task.getPollCount();
    long updateTime = task.getUpdateTime();
    String original = task.getWorkerId();
    task.setPollCount(0);
    task.setUpdateTime(0);
    task.setWorkerId(null);
    String checkSum = HashHelper.md5(toJson(task));
    task.setPollCount(pollCount + 1); // increment poll count
    task.setUpdateTime(updateTime);
    task.setWorkerId(original);
    return checkSum;
  }

  private Map.Entry<String, Long> getTaskChecksumAndUpdateTime(String taskId) {
    return withMetricLogError(
        () ->
            withRetryableQuery(
                GET_TASK_CHECKSUM_UPDATE_TIME_QUERY,
                stmt -> stmt.setString(1, taskId),
                result -> {
                  if (result.next()) {
                    return new AbstractMap.SimpleEntry<>(
                        result.getString(ID_COLUMN), result.getLong(PAYLOAD_COLUMN));
                  }
                  return null;
                }),
        "getTaskChecksumAndUpdateTime",
        "Failed getting checksum and update time of a task with id {}",
        taskId);
  }
}
