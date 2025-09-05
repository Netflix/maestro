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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.utils.TimeUtils;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class OutputDataDaoTest extends MaestroDaoBaseTest {
  public static final String WORKFLOW_ID = "wid";
  private static final StepType JOB_TYPE = StepType.TITUS;
  private static final String EXT_JOB_ID = "JOB_123";
  private OutputDataDao dao;
  private Map<String, Parameter> params;
  private Map<String, Artifact> artifacts;

  @Before
  public void setUp() throws IOException {
    dao = new OutputDataDao(DATA_SOURCE, MAPPER, CONFIG, metricRepo);
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-notebook.json", WorkflowDefinition.class);
    params = toParameters(definition.getWorkflow().getParams());
    artifacts =
        Map.of(
            Artifact.Type.DYNAMIC_OUTPUT.key(),
            loadObject("fixtures/artifact/sample-dynamic-output-artifact.json", Artifact.class));
  }

  @Test
  public void testParamsSizeOverLimit() throws Exception {
    ObjectMapper mockMapper = mock(ObjectMapper.class);
    OutputDataDao testDao = new OutputDataDao(DATA_SOURCE, mockMapper, CONFIG, metricRepo);
    when(mockMapper.writeValueAsString(any()))
        .thenReturn(new String(new char[Constants.JSONIFIED_PARAMS_STRING_SIZE_LIMIT + 1]));
    AssertHelper.assertThrows(
        "Output data size is over limit",
        IllegalArgumentException.class,
        "Output data's total size [750001] is larger than system param size limit [750000]",
        () ->
            testDao.insertOrUpdateOutputData(
                new OutputData(
                    JOB_TYPE,
                    EXT_JOB_ID,
                    WORKFLOW_ID,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    params,
                    new HashMap<>())));
  }

  @Test(expected = NullPointerException.class)
  public void testValidateParamEmpty() {
    OutputData outputData = new OutputData(null, null);
    dao.insertOrUpdateOutputData(outputData);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateParamsAndArtifactsEmpty() {
    OutputData data =
        new OutputData(
            JOB_TYPE,
            EXT_JOB_ID,
            WORKFLOW_ID,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            new HashMap<>(),
            new HashMap<>());
    dao.insertOrUpdateOutputData(data);
  }

  @Test
  public void testCreateAndUpsertParams() {
    addOutputData(params, null);

    Optional<OutputData> outputDataOpt = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE);
    assertTrue(outputDataOpt.isPresent());
    OutputData paramResult = outputDataOpt.get();

    verifyExpectedDTOs(paramResult);

    assertFalse(dao.getOutputDataForExternalJob("invalid", JOB_TYPE).isPresent());

    // Check Upsert
    dao.insertOrUpdateOutputData(paramResult);

    OutputData paramResult2 = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE).get();
    assertEquals(paramResult.getCreateTime(), paramResult2.getCreateTime());
    assertNotEquals(paramResult.getModifyTime(), paramResult2.getModifyTime());
    assertEquals(paramResult.getParams(), paramResult2.getParams());
    assertNull(paramResult2.getArtifacts());
  }

  @Test
  public void testCreateAndUpsertArtifacts() {
    addOutputData(null, artifacts);

    Optional<OutputData> outputDataOpt = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE);
    assertTrue(outputDataOpt.isPresent());
    OutputData created = outputDataOpt.get();
    assertEquals(EXT_JOB_ID, created.getExternalJobId());
    assertEquals(JOB_TYPE, created.getExternalJobType());
    assertEquals(WORKFLOW_ID, created.getWorkflowId());
    assertEquals(1, created.getArtifacts().size());
    assertNotNull(created.getCreateTime());
    assertNotNull(created.getModifyTime());

    // Check Upsert
    dao.insertOrUpdateOutputData(created);

    OutputData updated = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE).get();
    assertEquals(created.getCreateTime(), updated.getCreateTime());
    assertNotEquals(created.getModifyTime(), updated.getModifyTime());
    assertEquals(created.getArtifacts(), updated.getArtifacts());
    assertNull(updated.getParams());
  }

  @Test
  public void testCreateAndUpsertBoth() {
    addOutputData(params, artifacts);

    Optional<OutputData> outputDataOpt = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE);
    assertTrue(outputDataOpt.isPresent());
    OutputData created = outputDataOpt.get();
    verifyExpectedDTOs(created);
    assertEquals(1, created.getArtifacts().size());
    TimeUtils.sleep(3); // Ensure modify time is different

    // Check Upsert
    dao.insertOrUpdateOutputData(created);

    OutputData updated = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE).get();
    assertEquals(created.getCreateTime(), updated.getCreateTime());
    assertNotEquals(created.getModifyTime(), updated.getModifyTime());
    assertEquals(created.getParams(), updated.getParams());
    assertEquals(created.getArtifacts(), updated.getArtifacts());
  }

  private void addOutputData(Map<String, Parameter> params, Map<String, Artifact> artifacts) {
    OutputData data =
        new OutputData(
            JOB_TYPE,
            OutputDataDaoTest.EXT_JOB_ID,
            WORKFLOW_ID,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            params,
            artifacts);
    dao.insertOrUpdateOutputData(data);
  }

  private void verifyExpectedDTOs(OutputData paramResult) {
    assertEquals(OutputDataDaoTest.EXT_JOB_ID, paramResult.getExternalJobId());
    assertEquals(JOB_TYPE, paramResult.getExternalJobType());
    assertEquals(WORKFLOW_ID, paramResult.getWorkflowId());
    assertEquals(3, paramResult.getParams().size());
    assertNotNull(paramResult.getCreateTime());
    assertNotNull(paramResult.getModifyTime());
  }
}
