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
package com.netflix.maestro.extensions.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.maestro.extensions.ExtensionsDaoBaseTest;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedInstance;
import com.netflix.maestro.extensions.dao.models.ForeachFlattenedModel;
import com.netflix.maestro.extensions.models.StepIteration;
import com.netflix.maestro.extensions.models.StepIterationsSummary;
import com.netflix.maestro.extensions.utils.StepInstanceStatusEncoder;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepInstance.Status;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaestroForeachFlattenedDaoTest extends ExtensionsDaoBaseTest {
  private static final String WORKFLOW_ID = "workflow-id";
  private static final String STEP_ID = "step-id";
  private static final String ITERATION_RANK = "14-13-11-12";

  private MaestroForeachFlattenedDao foreachFlattenedDao;
  private static final TypeReference<List<Map<String, Object>>> REF = new TypeReference<>() {};

  @Before
  public void setUp() throws Exception {
    foreachFlattenedDao = new MaestroForeachFlattenedDao(dataSource, MAPPER, config, metrics);
    Connection conn = dataSource.getConnection();
    @Cleanup
    PreparedStatement stmt = conn.prepareStatement("TRUNCATE maestro_step_foreach_flattened");
    stmt.execute();
    List<Map<String, Object>> records = loadObject("flattening/flattening_records.json", REF);

    String insertQuery =
        "INSERT INTO maestro_step_foreach_flattened(workflow_id, workflow_instance_id, workflow_run_id, step_id, iteration_rank, initial_step_created_ms, instance, run_id_validity_end, step_attempt_seq, loop_parameters, step_status, step_status_encoded, step_status_priority, step_runtime_state, is_inserted_in_new_run) VALUES (?,?,?,?,?,?,?::jsonb,?,?,?::jsonb,?,?,?,?::jsonb,?)";

    @Cleanup PreparedStatement pstmt = conn.prepareStatement(insertQuery);
    for (Map<String, Object> record : records) {
      int idx = 0;
      Map<String, Object> instance =
          MAPPER.readValue(MAPPER.writeValueAsString(record.get("instance")), Map.class);
      Map<String, Object> stepRuntimeState =
          MAPPER.readValue(MAPPER.writeValueAsString(record.get("step_runtime_state")), Map.class);
      pstmt.setString(++idx, (String) instance.get("workflow_id"));
      pstmt.setLong(++idx, Long.parseLong(instance.get("workflow_instance_id").toString()));
      pstmt.setLong(++idx, Long.parseLong(instance.get("workflow_run_id").toString()));
      pstmt.setString(++idx, (String) instance.get("step_id"));
      pstmt.setString(++idx, (String) instance.get("iteration_rank"));
      pstmt.setLong(++idx, Long.parseLong(instance.get("initial_step_created_ms").toString()));
      pstmt.setString(++idx, MAPPER.writeValueAsString(record.get("instance")));
      pstmt.setLong(++idx, (Long) record.get("run_id_validity_end"));
      pstmt.setString(++idx, (String) record.get("step_attempt_seq"));
      pstmt.setString(++idx, MAPPER.writeValueAsString(record.get("loop_parameters")));
      pstmt.setString(++idx, (String) stepRuntimeState.get("status"));
      pstmt.setLong(++idx, (Long) record.get("step_status_encoded"));
      pstmt.setLong(++idx, (Long) record.get("step_status_priority"));
      pstmt.setString(++idx, MAPPER.writeValueAsString(record.get("step_runtime_state")));
      pstmt.setBoolean(++idx, (Boolean) record.get("is_inserted_in_new_run"));
      pstmt.addBatch();
    }
    pstmt.executeBatch();
    conn.commit();
    conn.close();
  }

  @Test
  public void testInsertFreshStepAttemptInNewRootRun() {
    String stepAttemptSeq = "14-11-15-11-13-11-11-11-12-13";
    long workflowRunId = 1;
    ForeachFlattenedModel foreachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(foreachFlattenedModel);
  }

  @Test
  public void testInsertFreshStepAttemptInExistingRootRun() {
    String stepAttemptSeq = "14-11-15-11-13-11-11-11-12-13";
    long workflowRunId = 1;
    ForeachFlattenedModel foreachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(foreachFlattenedModel);

    String newStepAttemptSeq = "14-11-15-11-13-11-12-11-13-11";
    ForeachFlattenedModel newForeachFlattenedModel =
        getForeachFlattenedModel(newStepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(newForeachFlattenedModel);
  }

  @Test
  public void testInsertOutOfOrderInsertOldStepAttemptInExistingRootRun() {
    String newStepAttemptSeq = "14-11-15-11-13-11-12-11-13-11";
    long workflowRunId = 1;
    ForeachFlattenedModel newForeachFlattenedModel =
        getForeachFlattenedModel(newStepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(newForeachFlattenedModel);

    // Insert older step attempt sequence after newer one
    String oldStepAttemptSeq = "14-11-15-11-13-11-11-11-12-13";
    ForeachFlattenedModel oldForeachFlattenedModel =
        getForeachFlattenedModel(oldStepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(oldForeachFlattenedModel);

    List<StepIteration> stepIterations =
        foreachFlattenedDao.scanStepIterations(
            newForeachFlattenedModel.getInstance().getWorkflowId(),
            newForeachFlattenedModel.getInstance().getWorkflowInstanceId(),
            newForeachFlattenedModel.getInstance().getWorkflowRunId(),
            newForeachFlattenedModel.getInstance().getStepId(),
            null,
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(1, stepIterations.size());
    // Asserting the newer step attempt sequence is the one that is always persisted
    Assert.assertEquals(newStepAttemptSeq, stepIterations.get(0).getStepAttemptSeq());
  }

  @Test
  public void testInsertStepAttemptWithNewStatusChangeInExistingRootRun() {
    String stepAttemptSeq = "14-11-15-11-13-11-11-11-12-13";
    long workflowRunId = 1;
    ForeachFlattenedModel foreachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(foreachFlattenedModel);

    ForeachFlattenedModel runningForeachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.RUNNING, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(runningForeachFlattenedModel);

    ForeachFlattenedModel failedForeachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.PLATFORM_FAILED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(failedForeachFlattenedModel);
  }

  @Test
  public void testInsertStepAttemptWithOutOfOrderStatusChangesInExistingRootRun() {
    String stepAttemptSeq = "14-11-15-11-13-11-11-11-12-13";
    long workflowRunId = 1;
    ForeachFlattenedModel foreachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.CREATED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(foreachFlattenedModel);

    ForeachFlattenedModel failedForeachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.PLATFORM_FAILED, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(failedForeachFlattenedModel);

    // This one is out of order, RUNNING cannot come after PLATFORM_FAILED event
    ForeachFlattenedModel runningForeachFlattenedModel =
        getForeachFlattenedModel(stepAttemptSeq, Status.RUNNING, workflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(runningForeachFlattenedModel);

    List<StepIteration> stepIterations =
        foreachFlattenedDao.scanStepIterations(
            failedForeachFlattenedModel.getInstance().getWorkflowId(),
            failedForeachFlattenedModel.getInstance().getWorkflowInstanceId(),
            failedForeachFlattenedModel.getInstance().getWorkflowRunId(),
            failedForeachFlattenedModel.getInstance().getStepId(),
            null,
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(1, stepIterations.size());
    // Assert that we should not insert RUNNING state after PLATFORM_FAILED
    Assert.assertEquals(
        Status.PLATFORM_FAILED, stepIterations.get(0).getStepRuntimeState().getStatus());
  }

  @Test
  public void testInsertOutOfOrderInsertStepAttemptAcrossDifferentRootRuns() {
    String firstStepAttemptSeq = "15-11-15-11-13-11-12-11-13-11";
    long firstWorkflowRunId = 5;
    ForeachFlattenedModel firstForeachFlattenedModel =
        getForeachFlattenedModel(firstStepAttemptSeq, Status.CREATED, firstWorkflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(firstForeachFlattenedModel);

    String thirdStepAttemptSeq = "11-11-15-11-13-11-11-11-12-13";
    long thirdWorkflowRunId = 1;
    ForeachFlattenedModel thirdForeachFlattenedModel =
        getForeachFlattenedModel(thirdStepAttemptSeq, Status.CREATED, thirdWorkflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(thirdForeachFlattenedModel);

    String secondStepAttemptSeq = "13-11-15-11-13-11-11-11-12-13";
    long secondWorkflowRunId = 3;
    ForeachFlattenedModel secondForeachFlattenedModel =
        getForeachFlattenedModel(secondStepAttemptSeq, Status.CREATED, secondWorkflowRunId);
    foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(secondForeachFlattenedModel);

    List<StepIteration> stepIterationsRun5 =
        foreachFlattenedDao.scanStepIterations(
            WORKFLOW_ID,
            1,
            firstWorkflowRunId,
            STEP_ID,
            null,
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(1, stepIterationsRun5.size());
    StepIteration stepIterationRun5 = stepIterationsRun5.get(0);
    Assert.assertEquals(firstStepAttemptSeq, stepIterationRun5.getStepAttemptSeq());
    Assert.assertEquals(firstWorkflowRunId, stepIterationRun5.getWorkflowRunId());

    List<StepIteration> stepIterationsRun3 =
        foreachFlattenedDao.scanStepIterations(
            WORKFLOW_ID,
            1,
            secondWorkflowRunId,
            STEP_ID,
            null,
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(1, stepIterationsRun3.size());
    StepIteration stepIterationRun3 = stepIterationsRun3.get(0);
    Assert.assertEquals(secondStepAttemptSeq, stepIterationRun3.getStepAttemptSeq());
    Assert.assertEquals(secondWorkflowRunId, stepIterationRun3.getWorkflowRunId());

    List<StepIteration> stepIterationsRun1 =
        foreachFlattenedDao.scanStepIterations(
            WORKFLOW_ID,
            1,
            thirdWorkflowRunId,
            STEP_ID,
            null,
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(1, stepIterationsRun1.size());
    StepIteration stepIterationRun1 = stepIterationsRun1.get(0);
    Assert.assertEquals(thirdStepAttemptSeq, stepIterationRun1.getStepAttemptSeq());
    Assert.assertEquals(thirdWorkflowRunId, stepIterationRun1.getWorkflowRunId());
  }

  @Test
  public void testScanStepIterationsSimple() {
    // start of scan
    List<StepIteration> stepIterations0 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations0.size());

    // ask for next 10 items
    List<StepIteration> stepIterations1 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            stepIterations0.get(9).getIterationRank(),
            10,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations1.size());

    // ask for previous 10
    List<StepIteration> stepIterations2 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            stepIterations1.get(0).getIterationRank(),
            10,
            false,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations2.size());

    // stepIterations0 and stepIterations2 should be equal
    Assert.assertEquals(stepIterations0, stepIterations2);

    // check total number of records, should be 100
    List<StepIteration> stepIterations4 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            200,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(100, stepIterations4.size());

    // corner cases towards the end
    List<StepIteration> stepIterations5 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            "225-11", // 4th last element
            20,
            true,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(3, stepIterations5.size());

    List<StepIteration> stepIterations6 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            "225-11", // 4th last element
            10,
            false,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations6.size());
    Assert.assertEquals("222-13", stepIterations6.get(0).getIterationRank());
    Assert.assertEquals("224-14", stepIterations6.get(9).getIterationRank());

    // test last only

    List<StepIteration> stepIterations7 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            3,
            false,
            Collections.emptyMap(),
            Collections.EMPTY_LIST);
    Assert.assertEquals(3, stepIterations7.size());
    Assert.assertEquals("225-12", stepIterations7.get(0).getIterationRank());
    Assert.assertEquals("225-14", stepIterations7.get(2).getIterationRank());
  }

  @Test
  public void testScanStepIterationsWithLoopParameterAndStatusFilters() {
    // start of scan
    List<StepIteration> stepIterations0 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            10,
            true,
            Collections.singletonMap("i_range", "1"),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations0.size());
    Assert.assertEquals("1", stepIterations0.get(0).getLoopParams().get("i_range"));
    Assert.assertEquals("1", stepIterations0.get(9).getLoopParams().get("i_range"));

    // ask for next 10 items
    List<StepIteration> stepIterations1 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            stepIterations0.get(9).getIterationRank(),
            10,
            true,
            Collections.singletonMap("i_range", "1"),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations1.size());

    // ask for previous 10
    List<StepIteration> stepIterations2 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            stepIterations1.get(0).getIterationRank(),
            10,
            false,
            Collections.singletonMap("i_range", "1"),
            Collections.EMPTY_LIST);
    Assert.assertEquals(10, stepIterations2.size());

    // stepIterations0 and stepIterations2 should be equal
    Assert.assertEquals(stepIterations0, stepIterations2);

    // check total number of records, should be 100
    List<StepIteration> stepIterations4 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            200,
            true,
            Collections.singletonMap("i_range", "1"),
            Collections.EMPTY_LIST);
    Assert.assertEquals(25, stepIterations4.size());

    // apply the statues filter
    List<StepIteration> stepIterations5 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            200,
            true,
            Collections.singletonMap("i_range", "1"),
            Arrays.asList("SUCCEEDED"));
    Assert.assertEquals(25, stepIterations5.size());

    // apply the unknown statues filter
    List<StepIteration> stepIterations6 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            200,
            true,
            Collections.singletonMap("i_range", "1"),
            Arrays.asList("FAILED"));
    Assert.assertEquals(0, stepIterations6.size());

    // apply multiple statues filter
    List<StepIteration> stepIterations7 =
        foreachFlattenedDao.scanStepIterations(
            "foreach.flattening.test.4",
            1,
            1,
            "i_nested-step-2",
            null,
            200,
            true,
            Collections.singletonMap("i_range", "1"),
            Arrays.asList("FAILED", "SUCCEEDED"));
    Assert.assertEquals(25, stepIterations7.size());
  }

  @Test
  public void testSummary() {
    StepIterationsSummary stepIterationsSummary =
        foreachFlattenedDao.getStepIterationsSummary(
            "foreach.flattening.test.4", 1, 1, "i_nested-step-2", true, true, 4);

    Assert.assertNotNull(stepIterationsSummary);
    Assert.assertEquals(
        99,
        stepIterationsSummary.getCountByStatus().get(StepInstance.Status.SUCCEEDED).longValue());

    Assert.assertEquals(
        1, stepIterationsSummary.getCountByStatus().get(Status.FATALLY_FAILED).longValue());

    Assert.assertEquals(
        "11-12", stepIterationsSummary.getRepresentativeIteration().getIterationRank());
    Assert.assertEquals(
        Status.FATALLY_FAILED,
        stepIterationsSummary.getRepresentativeIteration().getStepRuntimeState().getStatus());

    Assert.assertEquals(2, stepIterationsSummary.getLoopParamValues().size());
  }

  @Test
  public void testSampleValueOrderByIterationRank() {
    String[] loopParamValues =
        new String[] {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
    long workflowInstanceId = 1;
    long workflowRunId = 1;
    long createTime = System.currentTimeMillis();
    String stepId = "sample-value-step";
    String attemptSeq = "11-11";
    String loopParamName = "days";
    StepInstance.Status status = Status.SUCCEEDED;
    for (int i = 0; i < loopParamValues.length; ++i) {
      Map<String, Object> loopParams = new HashMap<>();
      loopParams.put(loopParamName, loopParamValues[i]);
      StepRuntimeState stepRuntimeState = new StepRuntimeState();
      stepRuntimeState.setStatus(status);
      int index = i + 1;
      ForeachFlattenedInstance instance =
          new ForeachFlattenedInstance(
              WORKFLOW_ID, workflowInstanceId, workflowRunId, stepId, "1" + index, createTime);
      ForeachFlattenedModel model =
          new ForeachFlattenedModel(
              instance,
              Long.MAX_VALUE,
              StepInstanceStatusEncoder.encode(status),
              StepInstanceStatusEncoder.getPriority(status),
              stepRuntimeState,
              attemptSeq,
              loopParams);
      foreachFlattenedDao.insertOrUpdateForeachFlattenedModel(model);
    }
    StepIterationsSummary summary =
        foreachFlattenedDao.getStepIterationsSummary(WORKFLOW_ID, 1, 1, stepId, false, false, 5);
    List<String> sampledValues = summary.getLoopParamValues().get(loopParamName);
    Assert.assertEquals(loopParamValues.length, sampledValues.size());
    for (int i = 0; i < sampledValues.size(); ++i) {
      Assert.assertEquals(loopParamValues[i], sampledValues.get(i));
    }
  }

  @Test
  public void testGetStepIteration() {
    StepIteration iteration =
        foreachFlattenedDao.getStepIteration(
            "foreach.flattening.test.4", 1, 1, "i_nested-step-2", "225-14");
    Assert.assertEquals("11-11-11-11-11-11", iteration.getStepAttemptSeq());
    Assert.assertEquals("225-14", iteration.getIterationRank());
    Assert.assertEquals("i_nested-step-2", iteration.getStepId());
    Map<String, String> params = iteration.getLoopParams();
    Assert.assertEquals(2, params.size());
    Assert.assertEquals("4", params.get("i_range"));
    Assert.assertEquals("25", params.get("num"));
    Assert.assertEquals(Status.SUCCEEDED, iteration.getStepRuntimeState().getStatus());
  }

  private static ForeachFlattenedModel getForeachFlattenedModel(
      String stepAttemptSeq, Status stepStatus, long workflowRunId) {
    long encodedStatus = StepInstanceStatusEncoder.encode(stepStatus);
    long statusPriority = StepInstanceStatusEncoder.getPriority(stepStatus);
    StepRuntimeState stepRuntimeState = new StepRuntimeState();
    stepRuntimeState.setStatus(stepStatus);
    long createTime = System.currentTimeMillis();
    stepRuntimeState.setCreateTime(createTime);
    long runIdValidityEnd = Long.MAX_VALUE;
    long workflowInstanceId = 1;

    ForeachFlattenedInstance foreachFlattenedInstance =
        new ForeachFlattenedInstance(
            WORKFLOW_ID, workflowInstanceId, workflowRunId, STEP_ID, ITERATION_RANK, createTime);

    return new ForeachFlattenedModel(
        foreachFlattenedInstance,
        runIdValidityEnd,
        encodedStatus,
        statusPriority,
        stepRuntimeState,
        stepAttemptSeq,
        getLoopParams());
  }

  private static Map<String, Object> getLoopParams() {
    Map<String, Object> loopParams = new HashMap<>();
    loopParams.put("param1", "value1");
    loopParams.put("param2", 2.23);
    loopParams.put("param3", false);
    loopParams.put("param4", 345);
    return loopParams;
  }
}
