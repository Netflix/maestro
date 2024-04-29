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
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.dto.ExternalJobType;
import com.netflix.maestro.engine.dto.OutputData;
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
  private static final ExternalJobType JOB_TYPE = ExternalJobType.TITUS;
  private static final String EXT_JOB_ID = "JOB_123";
  private OutputDataDao dao;
  private Map<String, Parameter> params;

  @Before
  public void setUp() throws IOException {
    dao = new OutputDataDao(dataSource, MAPPER, config);
    WorkflowDefinition definition =
        loadObject("fixtures/parameters/sample-wf-notebook.json", WorkflowDefinition.class);
    params = toParameters(definition.getWorkflow().getParams());
  }

  @Test(expected = NullPointerException.class)
  public void testValidateParamEmpty() {
    OutputData outputData = new OutputData(null, null);
    dao.insertOrUpdateOutputData(outputData);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateParamListEmpty() {
    OutputData param =
        new OutputData(
            JOB_TYPE,
            EXT_JOB_ID,
            WORKFLOW_ID,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            new HashMap<>(),
            new HashMap<>());
    dao.insertOrUpdateOutputData(param);
  }

  @Test
  public void testCreateAndUpsert() {
    addOutputData(EXT_JOB_ID);

    Optional<OutputData> outputDataOpt = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE);
    assertTrue(outputDataOpt.isPresent());
    OutputData paramResult = outputDataOpt.get();

    verifyExpectedDTOs(paramResult, EXT_JOB_ID);

    assertFalse(dao.getOutputDataForExternalJob("invalid", JOB_TYPE).isPresent());

    // Check Upsert
    dao.insertOrUpdateOutputData(paramResult);

    OutputData paramResult2 = dao.getOutputDataForExternalJob(EXT_JOB_ID, JOB_TYPE).get();
    assertEquals(paramResult.getCreateTime(), paramResult2.getCreateTime());
    assertNotEquals(paramResult.getModifyTime(), paramResult2.getModifyTime());
    assertEquals(paramResult.getParams(), paramResult2.getParams());
  }

  private void addOutputData(String externalJobId) {
    OutputData param =
        new OutputData(
            JOB_TYPE,
            externalJobId,
            WORKFLOW_ID,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            params,
            new HashMap<>());
    dao.insertOrUpdateOutputData(param);
  }

  private void verifyExpectedDTOs(OutputData paramResult, String externalJobId) {
    assertEquals(externalJobId, paramResult.getExternalJobId());
    assertEquals(JOB_TYPE, paramResult.getExternalJobType());
    assertEquals(WORKFLOW_ID, paramResult.getWorkflowId());
    assertEquals(3, paramResult.getParams().size());
    assertNotNull(paramResult.getCreateTime());
    assertNotNull(paramResult.getModifyTime());
  }
}
