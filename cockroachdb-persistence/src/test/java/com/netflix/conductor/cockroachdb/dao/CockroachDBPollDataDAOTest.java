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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.dao.PollDataDAO;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class CockroachDBPollDataDAOTest extends CockroachDBBaseTest {

  private PollDataDAO dao;

  @Before
  public void setUp() {
    dao = new CockroachDBPollDataDAO(dataSource, objectMapper, config);
  }

  protected PollDataDAO getPollDataDAO() {
    return dao;
  }

  @Test
  public void testPollData() {
    getPollDataDAO().updateLastPollData("taskDef", null, "workerId1");
    PollData pollData = getPollDataDAO().getPollData("taskDef", null);
    assertNotNull(pollData);
    assertTrue(pollData.getLastPollTime() > 0);
    assertEquals(pollData.getQueueName(), "taskDef");
    assertNull(pollData.getDomain());
    assertEquals(pollData.getWorkerId(), "workerId1");

    getPollDataDAO().updateLastPollData("taskDef", "domain1", "workerId1");
    pollData = getPollDataDAO().getPollData("taskDef", "domain1");
    assertNotNull(pollData);
    assertTrue(pollData.getLastPollTime() > 0);
    assertEquals(pollData.getQueueName(), "taskDef");
    assertEquals(pollData.getDomain(), "domain1");
    assertEquals(pollData.getWorkerId(), "workerId1");

    List<PollData> pData = getPollDataDAO().getPollData("taskDef");
    assertEquals(pData.size(), 2);

    pollData = getPollDataDAO().getPollData("taskDef", "domain2");
    assertNull(pollData);
  }
}
