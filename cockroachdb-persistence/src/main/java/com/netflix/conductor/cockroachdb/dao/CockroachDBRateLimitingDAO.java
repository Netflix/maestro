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
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.dao.RateLimitingDAO;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CockroachDBRateLimitingDAO extends CockroachDBBaseDAO implements RateLimitingDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBRateLimitingDAO.class);

  private static final String COUNT_COLUMN = "cnt";

  private static final String GET_RUNNING_TASK_COUNT_BY_NAME_STATEMENT =
      "SELECT count(task_name) AS cnt FROM task "
          + "WHERE task_name = ? AND status = 'IN_PROGRESS' AND start_time >= ? ";

  public CockroachDBRateLimitingDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    super(dataSource, objectMapper, config);
  }

  /**
   * Evaluate if the {@link Task} is rate limited or not based on {@link TaskDef} or {@link Task}.
   *
   * <p>It search running tasks in CockroachDB task table using the secondary index.
   *
   * @param task task to be evaluated whether it is rateLimited or not
   * @param taskDef task definition with rate limit settings
   * @return true if the {@link Task} is rateLimited, otherwise false.
   */
  @Override
  public boolean exceedsRateLimitPerFrequency(Task task, TaskDef taskDef) {
    int rateLimit =
        taskDef == null ? task.getRateLimitPerFrequency() : taskDef.getRateLimitPerFrequency();
    if (rateLimit <= 0) {
      return false;
    }
    int bucketSize =
        taskDef == null
            ? task.getRateLimitFrequencyInSeconds()
            : taskDef.getRateLimitFrequencyInSeconds();
    String taskName = task.getTaskDefName();
    try {
      return withRetryableQuery(
          GET_RUNNING_TASK_COUNT_BY_NAME_STATEMENT,
          statement -> {
            statement.setString(1, taskName);
            statement.setLong(2, System.currentTimeMillis() - 1000 * bucketSize);
          },
          result -> {
            if (result.next()) {
              int cnt = result.getInt(COUNT_COLUMN);
              if (cnt > rateLimit) {
                LOG.info(
                    "Got {} running instance for the task name {} in the past {} second exceeding a limit {}",
                    cnt,
                    taskName,
                    bucketSize,
                    rateLimit);
                return true;
              } else {
                LOG.debug(
                    "Got {} running instance for the task name {} in the past {} second within a limit {}",
                    cnt,
                    taskName,
                    bucketSize,
                    rateLimit);
              }
            }
            return false;
          });
    } catch (Exception e) {
      LOG.warn("Failed checking rate limit for task {} due to {}", taskName, e.getMessage());
      return true;
    }
  }
}
