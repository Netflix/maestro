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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.stepruntime.PausedStepAttempt;
import com.netflix.maestro.models.stepruntime.StepBreakpoint;
import com.netflix.maestro.utils.IdHelper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroStepBreakpointDaoTest extends MaestroDaoBaseTest {
  private MaestroStepBreakpointDao maestroStepBreakpointDao;
  @Mock private MaestroWorkflowDao workflowDao;
  private Connection conn;
  private static final String TEST_WORKFLOW_ID1 = "wf1";
  private static final String TEST_WORKFLOW_ID2 = "wf2";
  private static final String TEST_WORKFLOW_ID3 = "wf2";
  private static final long TEST_WORKFLOW_INSTANCE1 = 1L;
  private static final long TEST_WORKFLOW_INSTANCE2 = 2L;
  private static final long TEST_WORKFLOW_INSTANCE3 = 3L;
  private static final long TEST_WORKFLOW_RUN1 = 1L;
  private static final long TEST_WORKFLOW_RUN2 = 2L;
  private static final long TEST_WORKFLOW_VERSION1 = 1L;
  private static final long TEST_WORKFLOW_VERSION2 = 2L;
  private static final String TEST_STEP_ID1 = "job.1";
  private static final String TEST_STEP_ID2 = "job.2";
  private static final String TEST_STEP_ID3 = "job.3";
  private static final long INTERNAL_ID = 54321;
  private static final String TEST_FOREACH_STEP_ID4 = "job.4";
  private static final String TEST_FOREACH_STEP_ID5 = "job.5";
  private static final String TEST_NON_EXIST_STEP_ID = "non-exist";
  private static final long TEST_STEP_ATTEMPT1 = 1L;
  private static final long TEST_STEP_ATTEMPT2 = 2L;
  private static final int DELETION_BATCH_LIMIT = 3;
  private static final User TEST_USER = User.create("test_user");
  private static final String SAMPLE_WORKFLOW_ID = "sample-active-wf-with-props";
  private WorkflowDefinition wfd;

  private final Supplier<Integer> batchDeletionLimitSupplier = () -> DELETION_BATCH_LIMIT;

  @Before
  public void setUp() throws Exception {
    maestroStepBreakpointDao =
        new MaestroStepBreakpointDao(
            dataSource, MAPPER, config, workflowDao, batchDeletionLimitSupplier);
    conn = dataSource.getConnection();
    Connection connection = dataSource.getConnection();
    wfd = loadWorkflow(SAMPLE_WORKFLOW_ID);
    try {
      @Cleanup
      PreparedStatement stmt = connection.prepareStatement("TRUNCATE maestro_step_breakpoint");
      stmt.execute();
      connection.commit();
      connection.close();
    } catch (Exception e) {
      // swallow an init truncate failures
    }
  }

  @After
  public void after() throws Exception {
    conn.close();
  }

  @Test
  public void testAddBreakpoint() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    StepBreakpoint bp =
        maestroStepBreakpointDao.addStepBreakpoint(
            TEST_WORKFLOW_ID2,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            TEST_USER);
    assertEquals(TEST_WORKFLOW_ID2, bp.getWorkflowId());
    assertEquals(TEST_STEP_ID1, bp.getStepId());
    assertEquals(TEST_WORKFLOW_INSTANCE1, bp.getWorkflowInstanceId().longValue());
    assertEquals(TEST_WORKFLOW_VERSION1, bp.getWorkflowVersionId().longValue());
    assertNull(bp.getWorkflowRunId());
    assertNull(bp.getStepAttemptId());
    bp =
        maestroStepBreakpointDao.addStepBreakpoint(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            TEST_USER);
    assertEquals(TEST_WORKFLOW_ID1, bp.getWorkflowId());
    assertEquals(TEST_STEP_ID1, bp.getStepId());
    assertNull(bp.getWorkflowVersionId());
    assertNull(bp.getWorkflowInstanceId());
    assertNull(bp.getWorkflowRunId());
    assertNull(bp.getStepAttemptId());
  }

  @Test
  public void testAddForEachBreakpoint() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String hashedInternalId = IdHelper.hashKey(INTERNAL_ID);
    StepBreakpoint bp =
        maestroStepBreakpointDao.addStepBreakpoint(
            TEST_WORKFLOW_ID2,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID4,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            TEST_USER);
    assertEquals(
        String.format("%s_%s_", Constants.FOREACH_INLINE_WORKFLOW_PREFIX, hashedInternalId),
        bp.getWorkflowId());
    assertEquals(TEST_FOREACH_STEP_ID4, bp.getStepId());
    assertEquals(TEST_WORKFLOW_INSTANCE1, bp.getWorkflowInstanceId().longValue());
    assertEquals(TEST_WORKFLOW_VERSION1, bp.getWorkflowVersionId().longValue());
    assertNull(bp.getWorkflowRunId());
    assertNull(bp.getStepAttemptId());
  }

  @Test
  public void testInsertUpsertBreakpointCycle() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID2,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID2,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID2,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    List<StepBreakpoint> bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID2,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, bp.size());
    assertEquals(TEST_WORKFLOW_ID2, bp.get(0).getWorkflowId());
    assertEquals(TEST_STEP_ID1, bp.get(0).getStepId());
    assertEquals(TEST_WORKFLOW_INSTANCE1, bp.get(0).getWorkflowInstanceId().longValue());
    assertEquals(TEST_WORKFLOW_VERSION1, bp.get(0).getWorkflowVersionId().longValue());
    assertNull(bp.get(0).getWorkflowRunId());
    assertNull(bp.get(0).getStepAttemptId());
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, bp.size());
    assertEquals(TEST_WORKFLOW_ID1, bp.get(0).getWorkflowId());
    assertEquals(TEST_STEP_ID1, bp.get(0).getStepId());
    assertNull(bp.get(0).getWorkflowVersionId());
    assertNull(bp.get(0).getWorkflowInstanceId());
    assertNull(bp.get(0).getWorkflowRunId());
    assertNull(bp.get(0).getStepAttemptId());
  }

  @Test
  public void testUpsertStepBreakPoint() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    List<StepBreakpoint> bps =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(3, bps.size());
  }

  @Test
  public void testRemoveBreakpoint() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID3,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1,
        TEST_USER);
    List<StepBreakpoint> bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID3,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, bp.size());
    maestroStepBreakpointDao.removeStepBreakpoint(
        TEST_WORKFLOW_ID3,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1,
        true);
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID3,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(0, bp.size());
    AssertHelper.assertThrows(
        "removing not existing breakpoint",
        MaestroNotFoundException.class,
        "No breakpoint is deleted because breakpoint with identifier [wf1][1][1][1][job.2][1] is not"
            + " been created yet or has already been deleted",
        () ->
            maestroStepBreakpointDao.removeStepBreakpoint(
                TEST_WORKFLOW_ID1,
                TEST_WORKFLOW_VERSION1,
                TEST_WORKFLOW_INSTANCE1,
                TEST_WORKFLOW_RUN1,
                TEST_STEP_ID2,
                TEST_STEP_ATTEMPT1,
                true));
  }

  @Test
  public void testNonExistStepBreakpoints() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID2,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);

    List<StepBreakpoint> bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, bp.size());

    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_NON_EXIST_STEP_ID,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(0, bp.size());

    AssertHelper.assertThrows(
        "removing not existing breakpoint",
        MaestroNotFoundException.class,
        "No breakpoint is deleted because breakpoint with identifier [wf1][0][0][0][non-exist][0] is not"
            + " been created yet or has already been deleted",
        () ->
            maestroStepBreakpointDao.removeStepBreakpoint(
                TEST_WORKFLOW_ID1,
                Constants.MATCH_ALL_WORKFLOW_VERSIONS,
                Constants.MATCH_ALL_WORKFLOW_INSTANCES,
                Constants.MATCH_ALL_RUNS,
                TEST_NON_EXIST_STEP_ID,
                Constants.MATCH_ALL_STEP_ATTEMPTS,
                true));

    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));

    assertEquals(
        1,
        maestroStepBreakpointDao
            .getPausedStepAttempts(
                TEST_WORKFLOW_ID1,
                TEST_WORKFLOW_VERSION2,
                TEST_WORKFLOW_INSTANCE1,
                TEST_WORKFLOW_RUN1,
                TEST_STEP_ID1,
                TEST_STEP_ATTEMPT1)
            .size());

    assertEquals(
        0,
        maestroStepBreakpointDao
            .getPausedStepAttempts(
                TEST_WORKFLOW_ID1,
                TEST_WORKFLOW_VERSION1,
                TEST_WORKFLOW_INSTANCE1,
                TEST_WORKFLOW_RUN1,
                TEST_NON_EXIST_STEP_ID,
                TEST_STEP_ATTEMPT1)
            .size());

    assertEquals(
        1,
        maestroStepBreakpointDao.removeStepBreakpoint(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            true));
  }

  @Test
  public void testGetBreakpoints() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION2,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE2,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION2,
        TEST_WORKFLOW_INSTANCE1,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION2,
        TEST_WORKFLOW_INSTANCE2,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN2,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN2,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT2,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN2,
        TEST_STEP_ID2,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT2,
        TEST_USER);
    List<StepBreakpoint> bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(13, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(10, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(7, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(6, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertEquals(5, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE2,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID2,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID2,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(3, bp.size());
    bp =
        maestroStepBreakpointDao.getStepBreakPoints(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID2,
            TEST_STEP_ATTEMPT1);
    assertEquals(1, bp.size());
  }

  @Test
  public void testAddStepBreakpointForAllWorkflowVersionsInvalid() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    AssertHelper.assertThrows(
        "invalid step",
        MaestroBadRequestException.class,
        "Breakpoint can't be set as stepId [non-exist] is not present for the workflowId"
            + " [sample-active-wf-with-props]",
        () ->
            maestroStepBreakpointDao.addStepBreakpoint(
                TEST_WORKFLOW_ID1,
                Constants.MATCH_ALL_WORKFLOW_VERSIONS,
                Constants.MATCH_ALL_WORKFLOW_INSTANCES,
                Constants.MATCH_ALL_RUNS,
                TEST_NON_EXIST_STEP_ID,
                Constants.MATCH_ALL_RUNS,
                TEST_USER));
  }

  @Test
  public void testInsertPausedStepInstanceAttempt() throws SQLException {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String workflowId = wfd.getPropertiesSnapshot().getWorkflowId();
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    conn.commit();
    conn.close();
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertEquals(1, pausedStepAttempts.size());
    // Also test remove cycle
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        workflowId,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    int cnt =
        maestroStepBreakpointDao.removeStepBreakpoint(
            workflowId,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            false);
    assertEquals(0, cnt);
    maestroStepBreakpointDao.addStepBreakpoint(
        workflowId,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    cnt =
        maestroStepBreakpointDao.removeStepBreakpoint(
            workflowId,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            true);
    assertEquals(1, cnt);
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertEquals(0, pausedStepAttempts.size());
  }

  @Test
  public void testResumePausedStepAttempt() throws SQLException {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String workflowId = wfd.getPropertiesSnapshot().getWorkflowId();
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1);
    conn.commit();
    conn.close();
    maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1);
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID2,
            TEST_STEP_ATTEMPT1);
    assertEquals(0, pausedStepAttempts.size());
  }

  @Test
  public void testResumePausedStepAttemptInvalid() throws SQLException {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String workflowId = wfd.getPropertiesSnapshot().getWorkflowId();
    AssertHelper.assertThrows(
        "resuming already resumed step",
        MaestroNotFoundException.class,
        "No paused attempt is resumed because step attempt with identifier [sample-active-wf-with-props][1][1][1][job.2][1]"
            + " is not been created yet or already resumed",
        () ->
            maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
                workflowId,
                TEST_WORKFLOW_VERSION1,
                TEST_WORKFLOW_INSTANCE1,
                TEST_WORKFLOW_RUN1,
                TEST_STEP_ID2,
                TEST_STEP_ATTEMPT1));
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1);
    conn.commit();
    conn.close();
    maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1);
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID2,
            TEST_STEP_ATTEMPT1);
    assertEquals(0, pausedStepAttempts.size());
    AssertHelper.assertThrows(
        "invalid resume call",
        MaestroNotFoundException.class,
        "No paused attempt is resumed because step attempt with identifier [sample-active-wf-with-props][1][1][1][job.2][1]"
            + " is not been created yet or already resumed",
        () ->
            maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
                workflowId,
                TEST_WORKFLOW_VERSION1,
                TEST_WORKFLOW_INSTANCE1,
                TEST_WORKFLOW_RUN1,
                TEST_STEP_ID2,
                TEST_STEP_ATTEMPT1));
  }

  @Test
  public void testRemovePausedStepAttemptsWithConn() throws SQLException {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String workflowId = wfd.getPropertiesSnapshot().getWorkflowId();
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    conn.commit();
    conn.close();
    maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertEquals(0, pausedStepAttempts.size());
  }

  @Test
  public void testGetPausedStepAttemptsByStepIdentifier() throws SQLException {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String workflowId = wfd.getPropertiesSnapshot().getWorkflowId();
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT2);
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN2,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION2,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION2,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1);
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        workflowId,
        TEST_WORKFLOW_VERSION2,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID2,
        TEST_STEP_ATTEMPT1);
    conn.commit();
    conn.close();
    // Get all for <wfId, stepId> group
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(5, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(4, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(3, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(2, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertEquals(1, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID2,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(2, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID2,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(0, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION2,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID2,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(2, pausedStepAttempts.size());
    pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            workflowId,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID2,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, pausedStepAttempts.size());
    boolean resume =
        maestroStepBreakpointDao.shouldStepResume(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertFalse(resume);
    resume =
        maestroStepBreakpointDao.shouldStepResume(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT2);
    assertFalse(resume);
    resume =
        maestroStepBreakpointDao.shouldStepResume(
            workflowId,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN2,
            TEST_STEP_ID2,
            TEST_STEP_ATTEMPT2);
    assertTrue(resume);
  }

  @Test
  public void testShouldStepPause() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));
    boolean resume =
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertFalse(resume);
    maestroStepBreakpointDao.removeStepBreakpoint(
        TEST_WORKFLOW_ID1,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        true);
    assertFalse(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_STEP_ID1,
            Constants.MATCH_ALL_STEP_ATTEMPTS));
    resume =
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertTrue(resume);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));
    resume =
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertFalse(resume);
    maestroStepBreakpointDao.removeStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        true);
    assertFalse(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));

    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION2,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_STEP_ID1,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));
    assertFalse(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));
    resume =
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertFalse(resume);
    resume =
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1);
    assertTrue(resume);
  }

  @Test
  public void testShouldStepPauseForeach() {
    String hashedInternalId = IdHelper.hashKey(INTERNAL_ID);
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String test_foreach_wf_id =
        Constants.FOREACH_INLINE_WORKFLOW_PREFIX
            + "_"
            + hashedInternalId
            + "_"
            + TEST_WORKFLOW_INSTANCE3
            + "_"
            + TEST_STEP_ID3;
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID4,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    boolean resume1 =
        maestroStepBreakpointDao.shouldStepResume(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1);
    assertFalse(resume1);
    boolean resume2 =
        maestroStepBreakpointDao.shouldStepResume(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1);
    assertFalse(resume2);
  }

  @Test
  public void testShouldStepPauseForeachRemove() {
    String hashedInternalId = IdHelper.hashKey(INTERNAL_ID);
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String test_foreach_wf_id =
        Constants.FOREACH_INLINE_WORKFLOW_PREFIX
            + "_"
            + hashedInternalId
            + "_"
            + TEST_WORKFLOW_INSTANCE3
            + "_"
            + TEST_STEP_ID3;
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID4,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    int resumedSteps =
        maestroStepBreakpointDao.removeStepBreakpoint(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID4,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            true);
    assertEquals(2, resumedSteps);
    boolean resume1 =
        maestroStepBreakpointDao.shouldStepResume(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1);
    assertTrue(resume1);
    boolean resume2 =
        maestroStepBreakpointDao.shouldStepResume(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1);
    assertTrue(resume2);
  }

  @Test
  public void testShouldStepPauseForeachGet() {
    String hashedInternalId = IdHelper.hashKey(INTERNAL_ID);
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String test_foreach_wf_id =
        Constants.FOREACH_INLINE_WORKFLOW_PREFIX
            + "_"
            + hashedInternalId
            + "_"
            + TEST_WORKFLOW_INSTANCE3
            + "_"
            + TEST_STEP_ID3;
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID4,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE3,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            TEST_STEP_ATTEMPT1));
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID4,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(3, pausedStepAttempts.size());
    pausedStepAttempts.forEach(
        pausedStepAttempt -> assertEquals(test_foreach_wf_id, pausedStepAttempt.getWorkflowId()));
  }

  @Test
  public void testGetInlineWorkflowStepBreakPoints() {
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID4,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID5,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN2,
        TEST_FOREACH_STEP_ID4,
        TEST_STEP_ATTEMPT1,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        TEST_WORKFLOW_VERSION1,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID5,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN1,
        TEST_FOREACH_STEP_ID5,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    List<StepBreakpoint> stepBreakpoints =
        maestroStepBreakpointDao.getStepBreakPoints(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID5,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    List<StepBreakpoint> stepBreakpoints1 =
        maestroStepBreakpointDao.getStepBreakPoints(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID4,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(3, stepBreakpoints.size());
    assertEquals(1, stepBreakpoints1.size());
    stepBreakpoints.forEach(
        stepBreakpoint ->
            assertEquals(
                wfd.getPropertiesSnapshot().getWorkflowId(), stepBreakpoint.getWorkflowId()));
  }

  @Test
  public void testResumeInlineWorkflowStepInstanceAttempt() {
    String hashedInternalId = IdHelper.hashKey(INTERNAL_ID);
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String test_foreach_wf_id =
        Constants.FOREACH_INLINE_WORKFLOW_PREFIX
            + "_"
            + hashedInternalId
            + "_"
            + TEST_WORKFLOW_INSTANCE3
            + "_"
            + TEST_STEP_ID3;
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID5,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        TEST_WORKFLOW_VERSION1,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID5,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID5,
            TEST_STEP_ATTEMPT1));
    assertTrue(
        maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID5,
            TEST_STEP_ATTEMPT1));
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID5,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(2, pausedStepAttempts.size());

    maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
        test_foreach_wf_id,
        TEST_WORKFLOW_VERSION2,
        TEST_WORKFLOW_INSTANCE2,
        TEST_WORKFLOW_RUN1,
        TEST_FOREACH_STEP_ID5,
        TEST_STEP_ATTEMPT1);
    List<PausedStepAttempt> pausedStepAttemptsAfter =
        maestroStepBreakpointDao.getPausedStepAttempts(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID5,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(1, pausedStepAttemptsAfter.size());
    assertTrue(
        maestroStepBreakpointDao.shouldStepResume(
            test_foreach_wf_id,
            TEST_WORKFLOW_VERSION2,
            TEST_WORKFLOW_INSTANCE2,
            TEST_WORKFLOW_RUN1,
            TEST_FOREACH_STEP_ID5,
            TEST_STEP_ATTEMPT1));
  }

  @Test
  public void testShouldStepResume() throws SQLException {
    maestroStepBreakpointDao.insertPausedStepInstanceAttempt(
        conn,
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    conn.commit();
    conn.close();
    assertFalse(
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));
    maestroStepBreakpointDao.resumePausedStepInstanceAttempt(
        TEST_WORKFLOW_ID1,
        TEST_WORKFLOW_VERSION1,
        TEST_WORKFLOW_INSTANCE1,
        TEST_WORKFLOW_RUN1,
        TEST_STEP_ID1,
        TEST_STEP_ATTEMPT1);
    assertTrue(
        maestroStepBreakpointDao.shouldStepResume(
            TEST_WORKFLOW_ID1,
            TEST_WORKFLOW_VERSION1,
            TEST_WORKFLOW_INSTANCE1,
            TEST_WORKFLOW_RUN1,
            TEST_STEP_ID1,
            TEST_STEP_ATTEMPT1));
  }

  @Test
  public void testRemoveForeachStepBreakpointMultiBatchDeletion() {
    String hashedInternalId = IdHelper.hashKey(INTERNAL_ID);
    when(workflowDao.getWorkflowDefinition(anyString(), anyString())).thenReturn(wfd);
    String test_foreach_wf_id =
        Constants.FOREACH_INLINE_WORKFLOW_PREFIX
            + "_"
            + hashedInternalId
            + "_"
            + TEST_WORKFLOW_INSTANCE3
            + "_"
            + TEST_STEP_ID3;
    maestroStepBreakpointDao.addStepBreakpoint(
        wfd.getPropertiesSnapshot().getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        TEST_FOREACH_STEP_ID5,
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        TEST_USER);
    for (int i = 1; i <= DELETION_BATCH_LIMIT + 1; i++) {
      assertTrue(
          maestroStepBreakpointDao.createPausedStepAttemptIfNeeded(
              test_foreach_wf_id,
              TEST_WORKFLOW_VERSION1,
              TEST_WORKFLOW_INSTANCE1,
              i,
              TEST_FOREACH_STEP_ID5,
              TEST_STEP_ATTEMPT1));
    }
    List<PausedStepAttempt> pausedStepAttempts =
        maestroStepBreakpointDao.getPausedStepAttempts(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID5,
            Constants.MATCH_ALL_STEP_ATTEMPTS);
    assertEquals(DELETION_BATCH_LIMIT + 1, pausedStepAttempts.size());
    assertEquals(
        DELETION_BATCH_LIMIT + 1,
        maestroStepBreakpointDao.removeStepBreakpoint(
            wfd.getPropertiesSnapshot().getWorkflowId(),
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            TEST_FOREACH_STEP_ID5,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            true));
  }
}
