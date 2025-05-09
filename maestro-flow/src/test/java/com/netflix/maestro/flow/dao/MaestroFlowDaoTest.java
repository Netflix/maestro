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
package com.netflix.maestro.flow.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.MaestroDatabaseHelper;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.FlowBaseTest;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.metrics.MaestroMetrics;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class MaestroFlowDaoTest extends FlowBaseTest {
  private static MaestroMetrics METRICS;

  private final DatabaseConfiguration config = MaestroDatabaseHelper.getConfig();
  private final DataSource dataSource = MaestroDatabaseHelper.getDataSource();

  @BeforeClass
  public static void init() {
    METRICS = Mockito.mock(MaestroMetrics.class);
  }

  private MaestroFlowDao dao;
  private FlowGroup group;

  @Before
  public void setUp() {
    dao = new MaestroFlowDao(dataSource, MAPPER, config, METRICS);
    group = new FlowGroup(10, 1, "testAddress", 12345);
    assertNotNull(dao.insertGroup(10, "testAddress"));
  }

  @After
  public void tearDown() {
    dao.deleteGroup(10);
  }

  @Test
  public void testInsertFlow() {
    Flow flow = createFlow();
    dao.insertFlow(flow);
    var flows = dao.getFlows(new FlowGroup(10, 2, "testAddress", 12345), 10, "test");
    assertEquals(1, flows.size());
    assertEquals(10, flows.getFirst().getGroupId());
    assertEquals("test-flow-id", flows.getFirst().getFlowId());
    assertEquals(2, flows.getFirst().getGeneration());
    assertEquals(flow.getStartTime(), flows.getFirst().getStartTime());
    assertEquals("test-flow-ref", flows.getFirst().getReference());
    dao.deleteFlow(flow);
  }

  @Test
  public void testInsertFlowRetry() {
    Flow flow = createFlow();
    dao.insertFlow(flow);
    AssertHelper.assertThrows(
        "should throw and retry",
        MaestroRetryableError.class,
        "insertFlow for flow [test-flow-ref] is failed (res=[0])",
        () -> dao.insertFlow(flow));
    dao.deleteFlow(flow);
  }

  @Test
  public void testDeleteFlow() {
    Flow flow = createFlow();
    dao.insertFlow(flow);
    dao.deleteFlow(flow);
    var flows = dao.getFlows(new FlowGroup(10, 2, "testAddress", 12345), 10, "test");
    assertTrue(flows.isEmpty());
  }

  @Test
  public void testGetFlows() {
    Flow flow = createFlow();
    dao.insertFlow(flow);
    var flows = dao.getFlows(new FlowGroup(10, 1, "testAddress", 12345), 10, "test");
    assertTrue(flows.isEmpty());
    flows = dao.getFlows(new FlowGroup(9, 2, "testAddress", 12345), 10, "test");
    assertTrue(flows.isEmpty());
    flows = dao.getFlows(new FlowGroup(9, 2, "testAddress", 12345), 10, "z");
    assertTrue(flows.isEmpty());
    dao.deleteFlow(flow);
  }

  @Test
  public void testExistFlowWithSameKeys() {
    Flow flow = createFlow();
    dao.insertFlow(flow);
    assertTrue(dao.existFlowWithSameKeys(10, "test-flow-id"));
    assertFalse(dao.existFlowWithSameKeys(2, "test-flow-id"));
    assertFalse(dao.existFlowWithSameKeys(10, "test-flow-id2"));
    dao.deleteFlow(flow);
  }

  @Test
  public void testHeartbeatGroup() {
    assertNotNull(dao.heartbeatGroup(group));
    assertNull(dao.heartbeatGroup(new FlowGroup(10, 2, "testAddress", 12345)));
  }

  @Test
  public void testReleaseGroup() {
    dao.releaseGroup(group);
    FlowGroup actual = dao.getGroup(group.groupId());
    assertEquals(0, actual.heartbeatTs());
    dao.deleteGroup(group.groupId());
    dao.releaseGroup(group);
    assertNull(dao.getGroup(group.groupId()));
  }

  @Test
  public void testClaimExpiredGroup() {
    assertNull(dao.claimExpiredGroup("address2", 100000));
    FlowGroup claimed = dao.claimExpiredGroup("address2", -100000);
    assertEquals(10, claimed.groupId());
    assertEquals(2, claimed.generation());
    assertEquals("address2", claimed.address());
    assertTrue(claimed.heartbeatTs() > 12345);
  }

  @Test
  public void testInsertGroup() {
    AssertHelper.assertThrows(
        "should throw and retry",
        MaestroRetryableError.class,
        "insertGroup for group-[10] has a conflict and please retry",
        () -> dao.insertGroup(10, "testAddress"));
    FlowGroup claimed = dao.claimExpiredGroup("address2", -100000);
    assertEquals(10, claimed.groupId());
    assertEquals(2, claimed.generation());
    assertEquals("address2", claimed.address());
    assertTrue(claimed.heartbeatTs() > 12345);
  }

  @Test
  public void testGetGroup() {
    FlowGroup actual = dao.getGroup(group.groupId());
    assertEquals(10, actual.groupId());
    assertEquals(1, actual.generation());
    assertEquals("testAddress", actual.address());
    assertTrue(actual.heartbeatTs() > 0);
    assertNull(dao.getGroup(2));
  }
}
