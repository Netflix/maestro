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
package com.netflix.maestro.engine.params;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.OutputDataDao;
import com.netflix.maestro.engine.dto.ExternalJobType;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.TitusArtifact;
import com.netflix.maestro.models.parameter.InternalParamMode;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParameter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class OutputDataManagerTest extends MaestroEngineBaseTest {
  private @Mock OutputDataDao outputDataDao;
  private OutputDataManager outputDataManager;
  private StepRuntimeSummary runtimeSummary;
  private Map<String, Artifact> artifacts;
  private OutputData outputData;
  private final String titusTaskId = "t1234";
  private final TypeReference<Map<String, Parameter>> paramMap =
      new TypeReference<Map<String, Parameter>>() {};

  @Before
  public void before() throws JsonProcessingException {
    outputDataDao = Mockito.mock(OutputDataDao.class);
    outputDataManager = new OutputDataManager(outputDataDao);

    runtimeSummary = runtimeSummaryBuilder().build();

    TitusArtifact artifact = new TitusArtifact();
    artifact.setTitusTaskId(titusTaskId);
    artifacts = Collections.singletonMap(Artifact.Type.TITUS.key(), artifact);

    Map<String, Parameter> params =
        loadParams(
            "{\"str_param\": {\"type\": \"STRING\",\"value\": \"hello\", \"evaluated_result\": \"hello\", \"evaluated_time\": 1625176565957}}");

    outputData =
        new OutputData(
            ExternalJobType.TITUS,
            titusTaskId,
            "wfid",
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            params,
            new HashMap<>());
    outputData.setExternalJobId(titusTaskId);
    outputData.setExternalJobType(ExternalJobType.TITUS);
  }

  private Map<String, Parameter> loadParams(String param) throws JsonProcessingException {
    return MAPPER.readValue(param, paramMap);
  }

  private void setupOutputDataDao() {
    when(outputDataDao.getOutputDataForExternalJob(titusTaskId, ExternalJobType.TITUS))
        .thenReturn(Optional.of(outputData));
  }

  private StepRuntimeSummary.StepRuntimeSummaryBuilder runtimeSummaryBuilder() {
    return StepRuntimeSummary.builder()
        .stepId("step1")
        .stepInstanceUuid("uuid123")
        .params(new HashMap<>());
  }

  @Test
  public void testMissingJobIdArtifact() {
    outputDataManager.validateAndMergeOutputParams(runtimeSummary);
    assertTrue(runtimeSummary.getParams().isEmpty());
  }

  @Test
  public void testMissingOutputParams() {
    runtimeSummary = runtimeSummaryBuilder().artifacts(artifacts).build();
    outputDataManager.validateAndMergeOutputParams(runtimeSummary);
    assertTrue(runtimeSummary.getParams().isEmpty());
  }

  @Test
  public void testUndefinedOutputParameter() {
    setupOutputDataDao();
    runtimeSummary = runtimeSummaryBuilder().artifacts(artifacts).build();
    AssertHelper.assertThrows(
        "throws validation error if output param not defined",
        MaestroValidationException.class,
        "Invalid output parameter [str_param], not defined in params",
        () -> outputDataManager.validateAndMergeOutputParams(runtimeSummary));
  }

  @Test
  public void testMismatchedOutputParameterType() {
    setupOutputDataDao();
    runtimeSummary =
        runtimeSummaryBuilder()
            .artifacts(artifacts)
            .params(
                Collections.singletonMap(
                    "str_param",
                    LongParameter.builder()
                        .name("str_param")
                        .value(1L)
                        .evaluatedResult(1L)
                        .evaluatedTime(System.currentTimeMillis())
                        .build()))
            .build();
    AssertHelper.assertThrows(
        "throws validation error if mismatched types",
        MaestroValidationException.class,
        "ParameterDefinition type mismatch name [str_param] from [STRING] != to [LONG]",
        () -> outputDataManager.validateAndMergeOutputParams(runtimeSummary));
  }

  @Test
  public void testMutableOnStartOutputParameter() {
    setupOutputDataDao();
    runtimeSummary =
        runtimeSummaryBuilder()
            .artifacts(artifacts)
            .params(
                Collections.singletonMap(
                    "str_param",
                    StringParameter.builder()
                        .name("p1")
                        .value("v1")
                        .evaluatedResult("v1")
                        .mode(ParamMode.MUTABLE_ON_START)
                        .evaluatedTime(System.currentTimeMillis())
                        .build()))
            .build();

    AssertHelper.assertThrows(
        "throws validation error if MUTABLE_ON_START mode",
        MaestroValidationException.class,
        "Cannot modify param with mode [MUTABLE_ON_START] for parameter [str_param]",
        () -> outputDataManager.validateAndMergeOutputParams(runtimeSummary));
  }

  @Test
  public void testReservedOutputParameter() {
    setupOutputDataDao();
    runtimeSummary =
        runtimeSummaryBuilder()
            .artifacts(artifacts)
            .params(
                Collections.singletonMap(
                    "str_param",
                    StringParameter.builder()
                        .name("str_param")
                        .value("v1")
                        .evaluatedResult("v1")
                        .addMetaField(
                            Constants.METADATA_INTERNAL_PARAM_MODE,
                            InternalParamMode.RESERVED.name())
                        .evaluatedTime(System.currentTimeMillis())
                        .build()))
            .build();
    AssertHelper.assertThrows(
        "throws validation error if RESERVED mode",
        MaestroValidationException.class,
        "Cannot modify param with mode [CONSTANT] for parameter [str_param]",
        () -> outputDataManager.validateAndMergeOutputParams(runtimeSummary));
  }

  @Test
  public void testValidOutputParameters() {
    Map<String, Parameter> runtimeParams = new HashMap<>();
    runtimeParams.put(
        "str_param",
        StringParameter.builder()
            .name("str_param")
            .value("v1")
            .evaluatedResult("v1")
            .mode(ParamMode.MUTABLE)
            .evaluatedTime(System.currentTimeMillis())
            .build());
    setupOutputDataDao();
    runtimeSummary = runtimeSummaryBuilder().artifacts(artifacts).params(runtimeParams).build();
    outputDataManager.validateAndMergeOutputParams(runtimeSummary);
    assertEquals("hello", runtimeSummary.getParams().get("str_param").asString());
  }

  @Test
  public void testValidOutputParamTypes() throws IOException {
    Map<String, Parameter> runtimeParams =
        loadObject("fixtures/outputdata/sample-output-data-params-defaults.json", paramMap);
    Map<String, Parameter> outputParams =
        loadObject("fixtures/outputdata/sample-output-data-params-updated.json", paramMap);
    outputData =
        new OutputData(
            ExternalJobType.TITUS,
            titusTaskId,
            "wfid",
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            outputParams,
            new HashMap<>());
    setupOutputDataDao();
    runtimeSummary = runtimeSummaryBuilder().artifacts(artifacts).params(runtimeParams).build();
    outputDataManager.validateAndMergeOutputParams(runtimeSummary);
    long[] expectedLongArray = {4L, 5L, 6L};
    assertEquals("goodbye", runtimeSummary.getParams().get("str_param").asString());
    assertArrayEquals(
        expectedLongArray, runtimeSummary.getParams().get("long_array_param").asLongArray());
    assertEquals(51L, runtimeSummary.getParams().get("long_param").asLong().longValue());
    assertEquals("e", runtimeSummary.getParams().get("string_map_param").asStringMap().get("a"));
    assertEquals("f", runtimeSummary.getParams().get("string_map_param").asStringMap().get("b"));
    assertArrayEquals(
        new String[] {"p1", "p2", "p3"},
        (String[]) runtimeSummary.getParams().get("map_param").asMap().get("table_name"));
  }
}
