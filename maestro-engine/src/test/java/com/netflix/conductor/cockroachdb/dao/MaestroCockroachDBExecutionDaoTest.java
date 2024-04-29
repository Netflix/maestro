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

import static com.netflix.conductor.cockroachdb.dao.CockroachDBBaseDAO.PAYLOAD_COLUMN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.maestro.engine.compression.GZIPCompressor;
import com.netflix.maestro.engine.compression.StringCodec;
import com.netflix.maestro.engine.dao.MaestroDaoBaseTest;
import com.netflix.maestro.engine.properties.MaestroConductorProperties;
import com.netflix.maestro.models.Constants;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaestroCockroachDBExecutionDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_TASK_ID = "test-task-id";
  private static final long MAX_UPDATE_INTERVAL = 3600000;

  private MaestroCockroachDBExecutionDao maestroExecutionDao;
  private CockroachDBExecutionDAO executionDao;
  private Task task;
  private ObjectMapper mapper;

  private static class MaestroTestProperties extends MaestroConductorProperties {
    @Override
    public long getMaxTaskUpdateIntervalInMillis() {
      return MAX_UPDATE_INTERVAL;
    }

    @Override
    public boolean isCompressionEnabled() {
      return true;
    }

    @Override
    public String getCompressorName() {
      return "gzip";
    }
  }

  @Before
  public void setUp() {
    MaestroTestProperties testProperties = new MaestroTestProperties();
    mapper = new JsonMapperProvider().get();

    executionDao =
        new CockroachDBExecutionDAO(
            dataSource,
            new CockroachDBIndexDAO(dataSource, mapper, config),
            mapper,
            testProperties);

    CockroachDBIndexDAO indexDAO = new CockroachDBIndexDAO(dataSource, mapper, config);
    StringCodec stringCodec =
        new StringCodec(Stream.of(new GZIPCompressor()).collect(Collectors.toList()));
    maestroExecutionDao =
        new MaestroCockroachDBExecutionDao(
            dataSource, mapper, stringCodec, indexDAO, testProperties);

    task = new Task();
    task.setTaskId(TEST_TASK_ID);
    task.setTaskType(Constants.MAESTRO_TASK_NAME);
    task.setTaskDefName("test-def-name");
    task.setStatus(Task.Status.IN_PROGRESS);
    task.setWorkflowInstanceId("test-workflow-id");
    task.setOutputData(twoItemMap("foo", 123L, "bar", twoItemMap("abc", "", "def", null)));
  }

  @Test
  public void testUpdateTask() {
    // should update DB if this is new
    maestroExecutionDao.updateTask(task);
    Task actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("df52193d782fd8e02292a75504ce6eb7", actual.getWorkerId());
    assertEquals(1, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());

    // should update DB if there is a change
    task.getOutputData().put("bat", true);
    maestroExecutionDao.updateTask(task);
    actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("6292ff0c02a892ed0990e872c210ce7d", actual.getWorkerId());
    assertEquals(2, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());

    // no real DB update if only poll count is updated
    task.setPollCount(10);
    maestroExecutionDao.updateTask(task);
    actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("6292ff0c02a892ed0990e872c210ce7d", actual.getWorkerId());
    assertEquals(2, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());

    // no real DB update if task updates workerId
    task.setPollCount(10);
    task.setWorkerId("foo");
    maestroExecutionDao.updateTask(task);
    actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("6292ff0c02a892ed0990e872c210ce7d", actual.getWorkerId());
    assertEquals(2, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());

    // no real DB update if only update time is updated within the max update interval
    task.setPollCount(10);
    task.setUpdateTime(MAX_UPDATE_INTERVAL - 1);
    maestroExecutionDao.updateTask(task);
    actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("6292ff0c02a892ed0990e872c210ce7d", actual.getWorkerId());
    assertEquals(2, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());

    // should update if the update time is larger than max update interval
    task.setPollCount(10);
    task.setUpdateTime(MAX_UPDATE_INTERVAL + 1);
    maestroExecutionDao.updateTask(task);
    actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("6292ff0c02a892ed0990e872c210ce7d", actual.getWorkerId());
    assertEquals(11, actual.getPollCount());
    assertEquals(MAX_UPDATE_INTERVAL + 1, actual.getUpdateTime());
  }

  @Test
  public void testUpdateTaskMissingChecksum() {
    executionDao.updateTask(task);
    Task actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertNull(actual.getWorkerId());
    assertEquals(0, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());

    // should update DB if there is no checksum
    maestroExecutionDao.updateTask(task);
    actual = maestroExecutionDao.getTask(TEST_TASK_ID);
    assertEquals("df52193d782fd8e02292a75504ce6eb7", actual.getWorkerId());
    assertEquals(1, actual.getPollCount());
    assertEquals(0, actual.getUpdateTime());
  }

  @Test
  public void testTaskCompressionOnUpdate() throws JsonProcessingException {
    task.setInputData(
        Collections.singletonMap(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    maestroExecutionDao.updateTask(task);

    String taskPayload = getTaskPayload();
    JsonNode root = mapper.readTree(taskPayload);
    Assert.assertEquals(
        "{\"compressed\":\"H4sIAAAAAAAAAKtWSiQWKFkRrVapFgC8tuw9WAAAAA==\",\"compressor\":\"gzip\"}",
        root.get("inputData").toString());

    Task dbTask = maestroExecutionDao.getTask(task.getTaskId());
    Assert.assertEquals(task.getInputData(), dbTask.getInputData());
  }

  private String getTaskPayload() {
    return maestroExecutionDao.withRetryableQuery(
        "SELECT payload FROM task WHERE task_id = ?",
        statement -> statement.setString(1, task.getTaskId()),
        result -> {
          if (result.next()) {
            return result.getString(PAYLOAD_COLUMN);
          }
          return null;
        });
  }
}
