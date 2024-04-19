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

import static org.junit.Assert.*;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CockroachDBRateLimitingDAOTest extends CockroachDBBaseTest {
  private RateLimitingDAO dao;
  private ExecutionDAO executionDAO;

  @Before
  public void setUp() {
    dao = new CockroachDBRateLimitingDAO(dataSource, objectMapper, config);
    executionDAO =
        new CockroachDBExecutionDAO(
            dataSource,
            new CockroachDBIndexDAO(dataSource, objectMapper, config),
            objectMapper,
            config);
    executionDAO.createWorkflow(createTestWorkflow(TEST_WORKFLOW_ID));
  }

  @After
  public void tearDown() {
    executionDAO.removeWorkflow(TEST_WORKFLOW_ID);
  }

  @Test
  public void testExceedsRateLimitWhenNoRateLimitSet() {
    TaskDef taskDef = createTaskDef(0, 0);
    Task task = createRunningTestTask(TEST_TASK_ID_1);
    assertFalse(dao.exceedsRateLimitPerFrequency(task, taskDef));
  }

  @Test
  public void testExceedsRateLimitWithinLimit() {
    TaskDef taskDef = createTaskDef(10, 0);
    Task task = createRunningTestTask(TEST_TASK_ID_1);
    executionDAO.updateTask(task);
    task = createRunningTestTask(TEST_TASK_ID_2);
    executionDAO.updateTask(task);
    assertFalse(dao.exceedsRateLimitPerFrequency(task, taskDef));
  }

  @Test
  public void testExceedsRateLimitOutOfLimit() {
    TaskDef taskDef = createTaskDef(1, 0);
    Task task = createRunningTestTask(TEST_TASK_ID_1);
    executionDAO.updateTask(task);
    assertFalse(dao.exceedsRateLimitPerFrequency(task, taskDef));
    task = createRunningTestTask(TEST_TASK_ID_2);
    executionDAO.updateTask(task);
    assertTrue(dao.exceedsRateLimitPerFrequency(task, taskDef));
  }
}
