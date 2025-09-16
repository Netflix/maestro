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
package com.netflix.maestro.server.controllers;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroOutputDataDao;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.models.api.StepOutputDataRequest;
import com.netflix.maestro.models.parameter.StringArrayParameter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class OutputDataControllerTest extends MaestroBaseTest {
  @Mock private MaestroOutputDataDao mockOutputDataDao;

  private OutputDataController outputDataController;

  @Before
  public void before() {
    this.outputDataController = new OutputDataController(mockOutputDataDao);
  }

  @Test
  public void testUpsertOutputDataByRemovingValue() throws Exception {
    StepOutputDataRequest outputData =
        loadObject("fixtures/api/sample-output-data-request.json", StepOutputDataRequest.class);
    outputDataController.upsertOutputData(outputData);
    ArgumentCaptor<OutputData> captor = ArgumentCaptor.forClass(OutputData.class);
    verify(mockOutputDataDao, times(1)).insertOrUpdateOutputData(captor.capture());

    OutputData actual = captor.getValue();
    StepOutputDataRequest expected =
        loadObject("fixtures/api/sample-output-data-request.json", StepOutputDataRequest.class);
    Assert.assertNotEquals(expected.getParams(), actual.getParams());
    StringArrayParameter expectedParam =
        expected.getParams().remove("stringarray").asStringArrayParam();
    StringArrayParameter actualParam =
        actual.getParams().remove("stringarray").asStringArrayParam();
    Assert.assertEquals(expected.getParams(), actual.getParams());
    Assert.assertArrayEquals(expectedParam.getEvaluatedResult(), actualParam.getEvaluatedResult());
    Assert.assertArrayEquals(expectedParam.getValue(), actualParam.getEvaluatedResult());
    Assert.assertNotNull(actualParam.getValue());
    Assert.assertEquals(1, actualParam.getValue().length);
    Assert.assertNull(actualParam.getExpression());
  }
}
