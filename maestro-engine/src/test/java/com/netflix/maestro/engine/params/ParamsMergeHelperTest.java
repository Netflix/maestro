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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.InternalParamMode;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringMapParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.utils.JsonHelper;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ParamsMergeHelperTest extends MaestroEngineBaseTest {

  private ParamsMergeHelper.MergeContext restartContext;
  private ParamsMergeHelper.MergeContext systemMergeContext;
  private ParamsMergeHelper.MergeContext definitionContext;
  private ParamsMergeHelper.MergeContext upstreamMergeContext;
  // This context will be used by subworkflow and foreach step.
  private ParamsMergeHelper.MergeContext upstreamDefinitionMergeContext;
  private ParamsMergeHelper.MergeContext upstreamRestartMergeContext;
  private ParamsMergeHelper.MergeContext foreachRestartMergeContext;

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Before
  public void setUp() {
    this.restartContext =
        new ParamsMergeHelper.MergeContext(ParamSource.RESTART, false, false, true);
    this.definitionContext =
        new ParamsMergeHelper.MergeContext(ParamSource.DEFINITION, false, false, false);
    this.systemMergeContext =
        new ParamsMergeHelper.MergeContext(ParamSource.TEMPLATE_SCHEMA, true, false, false);
    this.upstreamMergeContext =
        new ParamsMergeHelper.MergeContext(ParamSource.LAUNCH, false, true, false);
    this.upstreamDefinitionMergeContext =
        new ParamsMergeHelper.MergeContext(ParamSource.SUBWORKFLOW, false, true, false);
    this.upstreamRestartMergeContext =
        new ParamsMergeHelper.MergeContext(ParamSource.SUBWORKFLOW, false, true, true);
    this.foreachRestartMergeContext =
        new ParamsMergeHelper.MergeContext(ParamSource.FOREACH, false, true, true);
  }

  private Map<String, ParamDefinition> parseParamDefMap(String json)
      throws JsonProcessingException {
    TypeReference<Map<String, ParamDefinition>> paramDefMap = new TypeReference<>() {};
    return MAPPER.readValue(json.replaceAll("'", "\""), paramDefMap);
  }

  private Map<String, Parameter> parseParamMap(String json) throws JsonProcessingException {
    TypeReference<Map<String, Parameter>> paramMap = new TypeReference<>() {};
    return MAPPER.readValue(json.replaceAll("'", "\""), paramMap);
  }

  @Test
  public void testMergeEmpty() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams = parseParamDefMap("{}");
    Map<String, ParamDefinition> paramsToMerge = parseParamDefMap("{}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, restartContext);
    assertEquals(0, allParams.size());
  }

  @Test
  public void testMergeBothOverwrite() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'tomerge': {'type': 'STRING','value': 'hello', 'mode': 'MUTABLE'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye', 'mode': 'MUTABLE'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    StringParamDefinition tomerge = allParams.get("tomerge").asStringParamDef();
    assertEquals("goodbye", tomerge.getValue());
    assertEquals(ParamMode.MUTABLE, tomerge.getMode());
    assertEquals(ParamSource.DEFINITION, tomerge.getSource());
  }

  @Test
  public void testMergeOverwriteModes() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomerge': {'type': 'STRING','value': 'hello', 'internal_mode': 'OPTIONAL'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye', 'mode': 'MUTABLE'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, restartContext);
    assertEquals(1, allParams.size());
    assertEquals("goodbye", allParams.get("tomerge").asStringParamDef().getValue());
    assertEquals(ParamMode.MUTABLE, allParams.get("tomerge").asStringParamDef().getMode());
    // Should keep internal mode until cleanup
    assertEquals(
        InternalParamMode.OPTIONAL, allParams.get("tomerge").asStringParamDef().getInternalMode());
  }

  @Test
  public void testMergeMapOverwrite() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomergemap': {'type': 'MAP', 'value': {'tomerge': {'type': 'STRING','value': 'hello', 'validator': '@notEmpty'}}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomergemap': {'type': 'MAP', 'value': {'tomerge':{'type': 'STRING', 'value': 'goodbye', 'validator': 'param != null'}}}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    MapParamDefinition tomergemap = allParams.get("tomergemap").asMapParamDef();
    assertEquals("goodbye", tomergemap.getValue().get("tomerge").getValue());
    assertEquals(
        "param != null",
        tomergemap.getValue().get("tomerge").asStringParamDef().getValidator().getRule());
    assertEquals(
        ParamSource.DEFINITION,
        tomergemap.getValue().get("tomerge").asStringParamDef().getSource());
    assertEquals(ParamSource.DEFINITION, tomergemap.getSource());
  }

  @Test
  public void testMergeStringMapOverwrite() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomergemap': {'type': 'STRING_MAP', 'value': {'tomerge': 'hello', 'all': 'allval'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomergemap': {'type': 'STRING_MAP', 'value': {'tomerge': 'goodbye', 'new': 'newval'}}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    StringMapParamDefinition tomergemap = allParams.get("tomergemap").asStringMapParamDef();
    assertEquals("goodbye", tomergemap.getValue().get("tomerge"));
    assertEquals("allval", tomergemap.getValue().get("all"));
    assertEquals("newval", tomergemap.getValue().get("new"));
    assertEquals(ParamSource.DEFINITION, tomergemap.getSource());
  }

  @Test
  public void testMergeBothOverwriteSEL() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'tomerge': {'type': 'LONG','expression': '5 * 10'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'LONG', 'expression': '20 * 10'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals("20 * 10", allParams.get("tomerge").asLongParamDef().getExpression());
  }

  @Test
  public void testMergeMapDefinedBySEL() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomerge': {'type': 'MAP','expression': 'data = new HashMap(); data.put(\\'foo\\', 123); return data;'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomerge': {'type': 'MAP','expression': 'data = new HashMap(); data.put(\\'foo\\', 1.23); return data;'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(
        "data = new HashMap(); data.put(\"foo\", 1.23); return data;",
        allParams.get("tomerge").asMapParamDef().getExpression());

    // tomerge param in allParams is a SEL but is a literal in valParamsToMerge. Then mergeParams
    // should throw an error
    Map<String, ParamDefinition> valParamsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'MAP','value': {}}}");
    AssertHelper.assertThrows(
        "Should not allow merging literal map param to a SEL defined param",
        IllegalArgumentException.class,
        "MAP param [tomerge] definition exp=[data = new HashMap(); data.put(\"foo\", 1.23); return data;] is not a literal",
        () -> ParamsMergeHelper.mergeParams(allParams, valParamsToMerge, definitionContext));
  }

  @Test
  public void testMergeStringMapDefinedBySEL() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomerge': {'type': 'STRING_MAP','expression': 'data = new HashMap(); data.put(\\'foo\\', \\'bar\\'); return data;'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomerge': {'type': 'STRING_MAP','expression': 'data = new HashMap(); data.put(\\'foo\\', \\'bat\\'); return data;'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(
        "data = new HashMap(); data.put(\"foo\", \"bat\"); return data;",
        allParams.get("tomerge").asStringMapParamDef().getExpression());

    // tomerge param in allParams is a SEL but is a literal in valParamsToMerge. Then mergeParams
    // should throw an error
    Map<String, ParamDefinition> valParamsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'STRING_MAP','value': {}}}");
    AssertHelper.assertThrows(
        "Should not allow merging string literal string map param to a SEL defined param",
        IllegalArgumentException.class,
        "param [tomerge] definition exp=[data = new HashMap(); data.put(\"foo\", \"bat\"); return data;] is not a literal",
        () -> ParamsMergeHelper.mergeParams(allParams, valParamsToMerge, definitionContext));
  }

  @Test
  public void testMergeNoOverwrite() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomerge1': {'type': 'STRING','value': 'hello', 'source': 'SYSTEM', 'validator': '@notEmpty'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomerge2': {'type': 'STRING', 'value': 'goodbye', 'validator': 'param != null'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(2, allParams.size());
    assertEquals("hello", allParams.get("tomerge1").asStringParamDef().getValue());
    assertEquals("goodbye", allParams.get("tomerge2").asStringParamDef().getValue());
    assertEquals(ParamSource.SYSTEM, allParams.get("tomerge1").asStringParamDef().getSource());
    assertEquals(ParamSource.DEFINITION, allParams.get("tomerge2").asStringParamDef().getSource());
    assertEquals(
        "@notEmpty", allParams.get("tomerge1").asStringParamDef().getValidator().getRule());
    assertEquals(
        "param != null", allParams.get("tomerge2").asStringParamDef().getValidator().getRule());
  }

  @Test
  public void testMergeNestedMapNoOverwrite() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomergemap': {'type': 'MAP', 'source': 'SYSTEM', 'value': {'tomerge1': {'type': 'STRING','value': 'hello', 'meta': {'source': 'DEFINITION'}}}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomergemap': {'type': 'MAP', 'value': {'tomerge2':{'type': 'STRING', 'value': 'goodbye'}}}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    MapParamDefinition tomergemap = allParams.get("tomergemap").asMapParamDef();
    assertEquals("hello", tomergemap.getValue().get("tomerge1").getValue());
    assertEquals(
        ParamSource.DEFINITION,
        tomergemap.getValue().get("tomerge1").asStringParamDef().getSource());
    assertEquals("goodbye", tomergemap.getValue().get("tomerge2").getValue());
    assertEquals(
        ParamSource.DEFINITION,
        tomergemap.getValue().get("tomerge2").asStringParamDef().getSource());

    // update to definition since map was merged
    assertEquals(ParamSource.DEFINITION, tomergemap.getSource());
  }

  @Test
  public void testMergeMapSourceNoOverlap() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomergemap1': {'type': 'MAP', 'source': 'SYSTEM', 'value': {'tomerge1': {'source':'SYSTEM', 'type': 'STRING','value': 'hello'}}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomergemap2': {'type': 'MAP', 'value': {'tomerge2':{'type': 'STRING', 'value': 'goodbye'}}}}");

    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(2, allParams.size());
    MapParamDefinition tomergemap1 = allParams.get("tomergemap1").asMapParamDef();
    assertEquals("hello", tomergemap1.getValue().get("tomerge1").getValue());
    // did not touch, can remain system
    assertEquals(
        ParamSource.SYSTEM, tomergemap1.getValue().get("tomerge1").asStringParamDef().getSource());
    assertEquals(ParamSource.SYSTEM, tomergemap1.getSource());

    MapParamDefinition tomergemap2 = allParams.get("tomergemap2").asMapParamDef();
    assertEquals("goodbye", tomergemap2.getValue().get("tomerge2").getValue());
    assertEquals(
        ParamSource.DEFINITION,
        tomergemap2.getValue().get("tomerge2").asStringParamDef().getSource());
    // got it from definition stage
    assertEquals(ParamSource.DEFINITION, tomergemap2.getSource());
  }

  @Test
  public void testKeepMergeKeepOrder() {
    Map<String, ParamDefinition> allParams = new LinkedHashMap<>();
    Map<String, ParamDefinition> paramsToMerge = new LinkedHashMap<>();

    String[] keyOrder = new String[40];
    // add params with some order, and few overlapping
    for (int i = 0; i < 20; i++) {
      String key = "prev_param_" + i;
      allParams.put(key, buildParam(key, key).toDefinition());
      keyOrder[i] = key;

      if (i <= 5) {
        paramsToMerge.put(key, buildParam(key, key + "_updated").toDefinition());
      }

      String newKey = "new_param_" + i;
      keyOrder[20 + i] = newKey;
      paramsToMerge.put(newKey, buildParam(newKey, newKey).toDefinition());
    }
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertArrayEquals(keyOrder, allParams.keySet().toArray());
  }

  @Test
  public void testMergeDisallowInvalidStartChanged() {
    for (ParamMode mode : Arrays.asList(ParamMode.CONSTANT, ParamMode.IMMUTABLE)) {
      AssertHelper.assertThrows(
          String.format("Should not allow modifying reserved modes, mode [%s]", mode),
          MaestroValidationException.class,
          String.format("Cannot modify param with mode [%s] for parameter [tomerge]", mode),
          new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
              Map<String, ParamDefinition> allParams =
                  parseParamDefMap(
                      String.format(
                          "{'tomerge': {'type': 'STRING','value': 'hello', 'mode': '%s'}}", mode));
              Map<String, ParamDefinition> paramsToMerge =
                  parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
              ParamsMergeHelper.mergeParams(
                  allParams,
                  paramsToMerge,
                  new ParamsMergeHelper.MergeContext(ParamSource.LAUNCH, false, false, false));
            }
          });
    }
  }

  @Test
  public void testMergeDisallowInvalidRestartChanged() {
    for (ParamMode mode :
        Arrays.asList(ParamMode.MUTABLE_ON_START, ParamMode.CONSTANT, ParamMode.IMMUTABLE)) {
      AssertHelper.assertThrows(
          String.format("Should not allow modifying reserved modes, mode [%s]", mode),
          MaestroValidationException.class,
          String.format("Cannot modify param with mode [%s] for parameter [tomerge]", mode),
          new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
              Map<String, ParamDefinition> allParams =
                  parseParamDefMap(
                      String.format(
                          "{'tomerge': {'type': 'STRING','value': 'hello', 'mode': '%s'}}", mode));
              Map<String, ParamDefinition> paramsToMerge =
                  parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
              ParamsMergeHelper.mergeParams(allParams, paramsToMerge, restartContext);
            }
          });
    }
  }

  @Test
  public void testMergeDisallowInvalidInternalMode() {
    InternalParamMode mode = InternalParamMode.RESERVED;
    for (ParamsMergeHelper.MergeContext context :
        Arrays.asList(definitionContext, upstreamMergeContext)) {
      AssertHelper.assertThrows(
          String.format("Should not allow modifying reserved modes, mode [%s]", mode),
          MaestroValidationException.class,
          "Cannot modify param with mode [CONSTANT] for parameter [tomerge]",
          new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
              Map<String, ParamDefinition> allParams =
                  parseParamDefMap(
                      String.format(
                          "{'tomerge': {'type': 'STRING','value': 'hello', 'internal_mode': '%s'}}",
                          mode));
              Map<String, ParamDefinition> paramsToMerge =
                  parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
              ParamsMergeHelper.mergeParams(allParams, paramsToMerge, context);
            }
          });
    }
  }

  @Test
  public void testMergeAllowSystemChanges() throws JsonProcessingException {
    for (ParamMode mode :
        Arrays.asList(ParamMode.MUTABLE_ON_START, ParamMode.CONSTANT, ParamMode.IMMUTABLE)) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'tomerge': {'type': 'STRING','value': 'hello', 'mode': '%s'}}", mode));
      Map<String, ParamDefinition> paramsToMerge =
          parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
      ParamsMergeHelper.mergeParams(allParams, paramsToMerge, systemMergeContext);
    }
  }

  @Test
  public void testMergeAllowUpstreamChanges() throws JsonProcessingException {
    for (ParamMode mode :
        Arrays.asList(ParamMode.MUTABLE_ON_START, ParamMode.CONSTANT, ParamMode.IMMUTABLE)) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'tomerge': {'type': 'STRING','value': 'hello', 'mode': '%s'}}", mode));
      Map<String, ParamDefinition> paramsToMerge =
          parseParamDefMap(
              "{'tomerge': {'type': 'STRING', 'value': 'goodbye', 'source': 'SYSTEM_INJECTED'}}");
      ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamMergeContext);
    }
  }

  @Test
  public void testMergeDisallowUpstreamChangesNoSource() throws JsonProcessingException {
    for (ParamMode mode : Arrays.asList(ParamMode.CONSTANT, ParamMode.IMMUTABLE)) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'tomerge': {'type': 'STRING','value': 'hello', 'mode': '%s'}}", mode));
      Map<String, ParamDefinition> paramsToMergeNoSource =
          parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
      AssertHelper.assertThrows(
          String.format("Should not allow modifying reserved modes, mode [%s]", mode),
          MaestroValidationException.class,
          String.format("Cannot modify param with mode [%s] for parameter [tomerge]", mode),
          () ->
              ParamsMergeHelper.mergeParams(
                  allParams, paramsToMergeNoSource, upstreamMergeContext));
    }
  }

  @Test
  public void testMergeAllowSystemChangesInternalMode() throws JsonProcessingException {
    for (InternalParamMode mode : Collections.singletonList(InternalParamMode.RESERVED)) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'tomerge': {'type': 'STRING','value': 'hello', 'internal_mode': '%s'}}",
                  mode.toString()));
      Map<String, ParamDefinition> paramsToMerge =
          parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
      ParamsMergeHelper.mergeParams(allParams, paramsToMerge, systemMergeContext);
    }
  }

  // only allow set internal mode for system tasks
  @Test
  public void testMergeAllowSystemUpdateInternalMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'tomerge': {'type': 'LONG','value': 2}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'LONG', 'value': 3, 'internal_mode': 'OPTIONAL'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, systemMergeContext);
    assertEquals(
        InternalParamMode.OPTIONAL, allParams.get("tomerge").asLongParamDef().getInternalMode());
  }

  @Test
  public void testMergeDisallowUpstreamUpdatesInternalModeNoSource() {
    AssertHelper.assertThrows(
        "Should not allow modifying internal mode",
        MaestroValidationException.class,
        "Cannot modify system mode for parameter [tomerge]",
        new Runnable() {
          @SneakyThrows
          @Override
          public void run() {
            Map<String, ParamDefinition> allParams =
                parseParamDefMap("{'tomerge': {'type': 'LONG','value': 2}}");
            Map<String, ParamDefinition> paramsToMerge =
                parseParamDefMap(
                    "{'tomerge': {'type': 'LONG', 'value': 3, 'internal_mode': 'OPTIONAL'}}");
            ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamMergeContext);
          }
        });
  }

  @Test
  public void testMergeNotAllowUpdateInternalMode() {
    // Don't allow to modify the source
    AssertHelper.assertThrows(
        "Should not allow modifying internal mode",
        MaestroValidationException.class,
        "Cannot modify system mode for parameter [tomerge]",
        new Runnable() {
          @SneakyThrows
          @Override
          public void run() {
            Map<String, ParamDefinition> allParams =
                parseParamDefMap("{'tomerge': {'type': 'LONG','value': 2}}");
            Map<String, ParamDefinition> paramsToMerge =
                parseParamDefMap(
                    "{'tomerge': {'type': 'LONG', 'value': 3, 'internal_mode': 'OPTIONAL'}}");
            ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
          }
        });
  }

  @Test
  public void testMergeCannotModifySource() {
    // Don't allow to modify the source
    AssertHelper.assertThrows(
        "Should not allow updating source",
        MaestroValidationException.class,
        "Cannot modify source for parameter [tomerge]",
        new Runnable() {
          @SneakyThrows
          @Override
          public void run() {
            Map<String, ParamDefinition> allParams =
                parseParamDefMap("{'tomerge': {'type': 'LONG','value': 2}}");
            Map<String, ParamDefinition> paramsToMerge =
                parseParamDefMap("{'tomerge': {'type': 'LONG', 'value': 3, 'source': 'SYSTEM'}}");
            ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
          }
        });
  }

  @Test
  public void testDisallowMergeModifyInternalModeNonSystem() {
    // Don't allow for non system context
    AssertHelper.assertThrows(
        "Should not allow updating internal mode",
        MaestroValidationException.class,
        "Cannot modify system mode for parameter [tomerge]",
        new Runnable() {
          @SneakyThrows
          @Override
          public void run() {
            Map<String, ParamDefinition> allParams =
                parseParamDefMap(
                    "{'tomerge': {'type': 'LONG','value': 2, 'internal_mode': 'PROVIDED'}}");
            Map<String, ParamDefinition> paramsToMerge =
                parseParamDefMap(
                    "{'tomerge': {'type': 'LONG', 'value': 3, 'internal_mode': 'OPTIONAL'}}");
            ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
          }
        });
  }

  @Test
  public void testMergeDisallowLessRestrictiveMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomerge': {'type': 'STRING','value': 'hello', 'mode': 'MUTABLE_ON_START'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye', 'mode': 'MUTABLE'}}");

    AssertHelper.assertThrows(
        "Should not allow setting to less strict mode",
        MaestroValidationException.class,
        "Cannot modify param mode to be less strict for parameter [tomerge] from [MUTABLE_ON_START] to [MUTABLE]",
        () -> ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext));
  }

  @Test
  public void testMergeAllowMoreRestrictiveMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'tomerge': {'type': 'STRING','value': 'hello', 'mode': 'MUTABLE'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomerge': {'type': 'STRING', 'value': 'goodbye', 'mode': 'MUTABLE_ON_START'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals("goodbye", allParams.get("tomerge").asStringParamDef().getValue());
    assertEquals(ParamMode.MUTABLE_ON_START, allParams.get("tomerge").asStringParamDef().getMode());
  }

  @Test
  public void testMergeInternalModeToMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'param1': {'type': 'LONG','value': 2, 'internal_mode': 'RESERVED'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'param1': {'type': 'LONG', 'value': 3}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, systemMergeContext);
    assertEquals(ParamMode.CONSTANT, allParams.get("param1").asLongParamDef().getMode());

    for (String mode : Arrays.asList("OPTIONAL", "PROVIDED", "REQUIRED")) {
      allParams =
          parseParamDefMap(
              String.format(
                  "{'param1': {'type': 'LONG','value': 2, 'internal_mode': '%s'}}", mode));
      paramsToMerge = parseParamDefMap("{'param1': {'type': 'LONG', 'value': 3}}");
      ParamsMergeHelper.mergeParams(allParams, paramsToMerge, systemMergeContext);
      assertEquals(ParamMode.MUTABLE, allParams.get("param1").asLongParamDef().getMode());
    }
  }

  @Test
  public void testMergeAllowImmutableNewRun() throws JsonProcessingException {
    ParamMode mode = ParamMode.MUTABLE_ON_START;
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            String.format("{'tomerge': {'type': 'STRING','value': 'hello', 'mode': '%s'}}", mode));
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap("{'tomerge': {'type': 'STRING', 'value': 'goodbye'}}");
    ParamsMergeHelper.mergeParams(
        allParams,
        paramsToMerge,
        new ParamsMergeHelper.MergeContext(ParamSource.LAUNCH, false, false, false));
  }

  @Test
  public void testMergeTypeConflict() {
    AssertHelper.assertThrows(
        "Should not allow mismatched types",
        MaestroValidationException.class,
        "ParameterDefinition type mismatch name [tomerge] from [STRING] != to [LONG]",
        new Runnable() {
          @SneakyThrows
          @Override
          public void run() {
            Map<String, ParamDefinition> allParams =
                ParamsMergeHelperTest.this.parseParamDefMap(
                    "{'tomerge': {'type': 'LONG','value': 123, 'name': 'tomerge'}}");
            Map<String, ParamDefinition> paramsToMerge =
                ParamsMergeHelperTest.this.parseParamDefMap(
                    "{'tomerge': {'type': 'STRING', 'value': 'goodbye', 'name': 'tomerge'}}");
            ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
          }
        });
  }

  @Test
  public void testAllowedTypeCastingIntoString() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING','value': '', 'name': 'tomerge'}}");
    Map<String, ParamDefinition> paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'LONG', 'value': '234', 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals("234", allParams.get("tomerge").asStringParamDef().getValue());

    allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING_ARRAY', 'value' : [], 'name': 'tomerge'}}");
    paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'LONG_ARRAY', 'value': [1, 2, 3], 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(3, allParams.get("tomerge").asStringArrayParamDef().getValue().length);

    allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING', 'value' : false, 'name': 'tomerge'}}");
    paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'BOOLEAN', 'value': true, 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals("true", allParams.get("tomerge").asStringParamDef().getValue());

    allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING', 'value' : '', 'name': 'tomerge'}}");
    paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'DOUBLE', 'value': 122.12, 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals("122.12", allParams.get("tomerge").asStringParamDef().getValue());
  }

  @Test
  public void testAllowedTypeCastingIntoDouble() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'DOUBLE', 'value' : 1234, 'name': 'tomerge'}}");
    Map<String, ParamDefinition> paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'LONG', 'value': 122, 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(BigDecimal.valueOf(122.0), allParams.get("tomerge").asDoubleParamDef().getValue());

    allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'DOUBLE', 'value' : 123.00, 'name': 'tomerge'}}");
    paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING', 'value': '122', 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(BigDecimal.valueOf(122.0), allParams.get("tomerge").asDoubleParamDef().getValue());
  }

  @Test
  public void testAllowedTypeCastingIntoLong() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'LONG','value': 123, 'name': 'tomerge'}}");
    Map<String, ParamDefinition> paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING', 'value': '234', 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(Long.valueOf(234), allParams.get("tomerge").asLongParamDef().getValue());
  }

  @Test
  public void testAllowedTypeCastingIntoBoolean() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'BOOLEAN','value': false, 'name': 'tomerge'}}");
    Map<String, ParamDefinition> paramsToMerge =
        ParamsMergeHelperTest.this.parseParamDefMap(
            "{'tomerge': {'type': 'STRING', 'value': 'trUe', 'name': 'tomerge'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(Boolean.TRUE, allParams.get("tomerge").asBooleanParamDef().getValue());
  }

  @Test
  public void testMergeWithParamMismatchDefinedBySEL() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'tomerge': {'type': 'STRING','expression': 'stringValue'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tomerge': {'type': 'LONG','expression': 'Long data = 123; return data;'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(
        "Long data = 123; return data;",
        allParams.get("tomerge").asStringParamDef().getExpression());
  }

  @Test
  public void testMergeTags() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'withtags': {'type': 'STRING','tags': ['tag1', 'tag3'], 'value': 'hello'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'withtags': {'type': 'STRING','tags': ['tag2', 'tag3'], 'value': 'goodbye'}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(3, allParams.get("withtags").asStringParamDef().getTags().getTags().size());
  }

  @Test
  public void testMergePartialTags() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'basetag': {'type': 'STRING','tags': ['tag1'], 'value': 'hello'}, 'mergetag': {'type': 'STRING', 'value': 'hello'}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'basetag': {'type': 'STRING', 'value': 'goodbye'}, 'mergetag': {'type': 'STRING','value': 'goodbye', 'tags': ['tag1']} }");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(2, allParams.size());
    assertEquals(1, allParams.get("basetag").asStringParamDef().getTags().getTags().size());
    assertEquals(1, allParams.get("mergetag").asStringParamDef().getTags().getTags().size());
  }

  @Test
  public void testMergeNestedTags() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tagmap': {'type': 'MAP','value': {'withtag': {'type': 'STRING','tags': ['tag1']}}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'tagmap': {'type': 'MAP','value': {'withtag': {'type': 'STRING','tags': ['tag2']}}}}");
    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext);
    assertEquals(1, allParams.size());
    assertEquals(
        2,
        allParams
            .get("tagmap")
            .asMapParamDef()
            .getValue()
            .get("withtag")
            .getTags()
            .getTags()
            .size());
  }

  @Test
  public void testParameterConversionRemoveInternalMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'optional': {'type': 'STRING', 'value': 'hello', 'internal_mode': 'OPTIONAL'}}");
    Map<String, ParamDefinition> convertedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertNull(convertedParams.get("optional").asStringParamDef().getInternalMode());
  }

  @Test
  public void testParameterConversionRemoveInternalModeNestedMap() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'tomergemap1': {'type': 'MAP', 'internal_mode': 'RESERVED', 'value': {'tomerge1': {'type': 'STRING','value': 'hello', 'internal_mode': 'RESERVED'}}}}");
    Map<String, ParamDefinition> convertedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertNull(convertedParams.get("tomergemap1").asMapParamDef().getInternalMode());
    assertNull(
        convertedParams
            .get("tomergemap1")
            .asMapParamDef()
            .getValue()
            .get("tomerge1")
            .asStringParamDef()
            .getInternalMode());
  }

  @Test
  public void testCleanupNoParams() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams = parseParamDefMap("{}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertEquals(0, cleanedParams.size());
  }

  @Test
  public void testCleanupWithMapInsideMap() throws JsonProcessingException {
    for (ParamMode mode : ParamMode.values()) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'map': {'type': 'MAP','value': {'present': {'type': 'STRING', 'mode': '%s', 'value': 'hello'},"
                      + " 'm':{'type':'MAP','mode': '%s', 'expression':'job_param'}}}}",
                  mode, mode));
      Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
      assertEquals(2, cleanedParams.get("map").asMapParamDef().getValue().size());
    }
  }

  @Test
  public void testCleanupAllPresentParams() throws JsonProcessingException {
    for (ParamMode mode : ParamMode.values()) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'optional': {'type': 'STRING', 'mode': '%s', 'value': 'hello'}}",
                  mode.toString()));
      Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
      assertEquals(1, cleanedParams.size());
    }
  }

  @Test
  public void testCleanupAllNestedPresentParams() throws JsonProcessingException {
    for (ParamMode mode : ParamMode.values()) {
      Map<String, ParamDefinition> allParams =
          parseParamDefMap(
              String.format(
                  "{'map': {'type': 'MAP','value': {'present': {'type': 'STRING', 'mode': '%s', 'value': 'hello'}}}}",
                  mode.toString()));
      Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
      assertEquals(1, cleanedParams.get("map").asMapParamDef().getValue().size());
    }
  }

  @Test
  public void testCleanupOptionalEmptyParams() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'optional': {'type': 'STRING', 'internal_mode': 'OPTIONAL'}}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertEquals(0, cleanedParams.size());
  }

  @Test
  public void testCleanupOptionalNestedEmptyParams() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'map': {'type': 'MAP','value': {'optional': {'type': 'STRING', 'internal_mode': 'OPTIONAL'}}}}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertEquals(0, cleanedParams.get("map").asMapParamDef().getValue().size());
  }

  @Test
  public void testCleanupOptionalEmptyMap() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap("{'map': {'type': 'MAP','value': {}, 'internal_mode': 'OPTIONAL'}}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertEquals(0, cleanedParams.size());
  }

  @Test
  public void testCleanupOptionalEmptyNestedMap() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'map': {'type': 'MAP','value': {'nested': {'type': 'MAP','value': {}, 'internal_mode': 'OPTIONAL'}}, 'internal_mode': 'OPTIONAL'}}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertEquals(0, cleanedParams.size());
  }

  @Test
  public void testCleanupOptionalEmptyNestedMapEmptyElement() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'map': {'type': 'MAP','value': {'nested': {'type': 'MAP','value': {'str': {'type': 'STRING', 'internal_mode': 'OPTIONAL'}}, 'internal_mode': 'OPTIONAL'}}, 'internal_mode': 'OPTIONAL'}}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertEquals(0, cleanedParams.size());
  }

  @Test
  public void testCleanupOptionalEmptyNestedMapMultipleElements() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'data_auditor': {'type': 'MAP','value': {'winston': {'type': 'MAP','value': {}, 'internal_mode': 'OPTIONAL'}, 'audits': {'type': 'STRING_ARRAY','value': ['a','b'], 'internal_mode': 'REQUIRED'}}, 'internal_mode': 'REQUIRED'}}");
    Map<String, ParamDefinition> cleanedParams = ParamsMergeHelper.cleanupParams(allParams);
    assertFalse(
        cleanedParams.get("data_auditor").asMapParamDef().getValue().containsKey("winston"));
  }

  @Test
  public void testCleanupNonOptionalNestedEmptyParamsThrowException()
      throws JsonProcessingException {
    for (InternalParamMode mode : InternalParamMode.values()) {
      if (!mode.equals(InternalParamMode.OPTIONAL)) {
        Map<String, ParamDefinition> allParams =
            parseParamDefMap(
                String.format(
                    "{'map': {'type': 'MAP','value': {'empty': {'type': 'STRING', 'internal_mode': '%s'}}}}",
                    mode));
        AssertHelper.assertThrows(
            String.format("Should not allow empty for internal_mode, mode [%s]", mode.name()),
            IllegalArgumentException.class,
            "[empty] is a required parameter (type=[STRING])",
            () -> ParamsMergeHelper.cleanupParams(allParams));
      }
    }
  }

  @Test
  public void testStepCreateWithSource() {
    ParamsMergeHelper.MergeContext mergeContext =
        ParamsMergeHelper.MergeContext.stepCreate(ParamSource.DEFINITION);
    assertEquals(ParamSource.DEFINITION, mergeContext.getMergeSource());
    assertFalse(mergeContext.isUpstreamMerge());
    assertFalse(mergeContext.isSystem());

    mergeContext = ParamsMergeHelper.MergeContext.stepCreate(ParamSource.SYSTEM);
    assertEquals(ParamSource.SYSTEM, mergeContext.getMergeSource());
    assertFalse(mergeContext.isUpstreamMerge());
    assertTrue(mergeContext.isSystem());
  }

  @Test
  public void testStepWorkflowCreate() {
    ParamsMergeHelper.MergeContext mergeContext =
        ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.DEFINITION, true);
    assertEquals(ParamSource.DEFINITION, mergeContext.getMergeSource());
    assertTrue(mergeContext.isUpstreamMerge());
    assertFalse(mergeContext.isSystem());

    mergeContext = ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.SYSTEM, false);
    assertEquals(ParamSource.SYSTEM, mergeContext.getMergeSource());
    assertFalse(mergeContext.isUpstreamMerge());
    assertTrue(mergeContext.isSystem());
  }

  @Test
  public void testStepWorkflowCreateFromRequest() {
    RunRequest mockRequest = Mockito.mock(RunRequest.class);

    when(mockRequest.isSystemInitiatedRun()).thenReturn(false);
    ParamsMergeHelper.MergeContext mergeContext =
        ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.DEFINITION, mockRequest);
    assertEquals(ParamSource.DEFINITION, mergeContext.getMergeSource());
    assertFalse(mergeContext.isUpstreamMerge());
    assertFalse(mergeContext.isSystem());

    when(mockRequest.isSystemInitiatedRun()).thenReturn(false);
    mergeContext = ParamsMergeHelper.MergeContext.workflowCreate(ParamSource.SYSTEM, false);
    assertEquals(ParamSource.SYSTEM, mergeContext.getMergeSource());
    assertFalse(mergeContext.isUpstreamMerge());
    assertTrue(mergeContext.isSystem());
  }

  @Test
  public void testMergeUpstreamMergeWithLessStrictMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'default_value','mode': 'MUTABLE_ON_START', 'meta': {'source': 'SYSTEM_DEFAULT'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'parent_wf_defined_value','mode': 'MUTABLE', 'meta': {'source': 'DEFINITION'}}}");

    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamDefinitionMergeContext);
    assertEquals(1, allParams.size());
    assertEquals("parent_wf_defined_value", allParams.get("workflow_default_param").getValue());
    assertEquals(ParamMode.MUTABLE_ON_START, allParams.get("workflow_default_param").getMode());
    assertEquals(ParamSource.SUBWORKFLOW, allParams.get("workflow_default_param").getSource());
  }

  @Test
  public void testMergeNoUpstreamMergeWithLessStrictMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'default_value','mode': 'MUTABLE_ON_START', 'meta': {'source': 'SYSTEM_DEFAULT'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'parent_wf_defined_value','mode': 'MUTABLE', 'meta': {'source': 'DEFINITION'}}}");

    AssertHelper.assertThrows(
        "throws exception when a non-upstream source tries to merge param with less strict mode",
        MaestroValidationException.class,
        "Cannot modify param mode to be less strict for parameter [workflow_default_param] from [MUTABLE_ON_START] to [MUTABLE]",
        () -> ParamsMergeHelper.mergeParams(allParams, paramsToMerge, definitionContext));
  }

  @Test
  public void testMergeUpstreamMergeWithImmutableMode() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'default_value','mode': 'IMMUTABLE', 'meta': {'source': 'SYSTEM_DEFAULT'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'parent_wf_defined_value', 'mode': 'MUTABLE', 'meta': {'source': 'DEFINITION'}}}");

    AssertHelper.assertThrows(
        "throws exception when a non-upstream source tries to merge param with less strict mode",
        MaestroValidationException.class,
        "Cannot modify param with mode [IMMUTABLE] for parameter [workflow_default_param]",
        () ->
            ParamsMergeHelper.mergeParams(
                allParams, paramsToMerge, upstreamDefinitionMergeContext));
  }

  @Test
  public void testMergeUpstreamRestartWithMutableOnStart() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'default_value','mode': 'MUTABLE_ON_START', 'meta': {'source': 'SYSTEM_DEFAULT'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'parent_wf_defined_value', 'mode': 'MUTABLE', 'meta': {'source': 'DEFINITION'}}}");

    AssertHelper.assertThrows(
        "throws exception when a non-upstream source tries to merge param with less strict mode",
        MaestroValidationException.class,
        "Cannot modify param with mode [MUTABLE_ON_START] for parameter [workflow_default_param]",
        () -> ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamRestartMergeContext));
  }

  @Test
  public void testMergeUpstreamRestartWithMutableOnRestart() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'default_value','mode': 'MUTABLE_ON_START_RESTART', 'meta': {'source': 'SYSTEM_DEFAULT'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'workflow_default_param': {'type': 'STRING','value': 'parent_wf_defined_value', 'mode': 'MUTABLE', 'meta': {'source': 'DEFINITION'}}}");

    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamRestartMergeContext);
    assertEquals(1, allParams.size());
    assertEquals("parent_wf_defined_value", allParams.get("workflow_default_param").getValue());
    assertEquals(
        ParamMode.MUTABLE_ON_START_RESTART, allParams.get("workflow_default_param").getMode());
    assertEquals(ParamSource.SUBWORKFLOW, allParams.get("workflow_default_param").getSource());
  }

  @Test
  public void testMergeDiffTypeSameValueRestartWithMutableStart() throws JsonProcessingException {
    Map<String, ParamDefinition> allParams =
        parseParamDefMap(
            "{'TARGET_RUN_HOUR': {'name': 'TARGET_RUN_HOUR','type': 'STRING','value': '0', 'mode': 'MUTABLE_ON_START', 'tags': [], 'meta': {'source': 'SYSTEM_DEFAULT'}}}");
    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'TARGET_RUN_HOUR': {'name': 'TARGET_RUN_HOUR','type': 'LONG','value': 0}}");

    ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamRestartMergeContext);
    assertEquals(1, allParams.size());
    assertEquals("0", allParams.get("TARGET_RUN_HOUR").getValue());
    assertEquals(ParamMode.MUTABLE_ON_START, allParams.get("TARGET_RUN_HOUR").getMode());
    assertEquals(ParamSource.SUBWORKFLOW, allParams.get("TARGET_RUN_HOUR").getSource());
  }

  @Test
  public void testMergeForeachRestartWithMutableOnStart() throws IOException {
    DefaultParamManager defaultParamManager =
        new DefaultParamManager(JsonHelper.objectMapperWithYaml());
    defaultParamManager.init();
    Map<String, ParamDefinition> allParams =
        defaultParamManager.getDefaultParamsForType(StepType.FOREACH).get();

    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'loop_params': {'value': {'i' : { 'value': [1, 2, 3], 'type': 'long_array', "
                + "'validator': 'param!=null && param.size() > 2', 'mode': 'mutable'}, "
                + "'j' : {'expression': 'param1', 'type': 'STRING_ARRAY', "
                + "'validator': 'param!=null && param.size() > 2'}}, 'type': 'MAP'}}");

    AssertHelper.assertThrows(
        "throws exception when a foreach source restarts and tries to mutate params with MUTABLE_ON_START mode",
        MaestroValidationException.class,
        "Cannot modify param with mode [MUTABLE_ON_START] for parameter [loop_params]",
        () -> ParamsMergeHelper.mergeParams(allParams, paramsToMerge, foreachRestartMergeContext));
  }

  @Test
  public void testMergeSubworkflowRestartWithMutableOnStart() throws IOException {
    DefaultParamManager defaultParamManager =
        new DefaultParamManager(JsonHelper.objectMapperWithYaml());
    defaultParamManager.init();
    Map<String, ParamDefinition> allParams =
        defaultParamManager.getDefaultParamsForType(StepType.SUBWORKFLOW).get();

    Map<String, ParamDefinition> paramsToMerge =
        parseParamDefMap(
            "{'subworkflow_version': {'value': 'active', 'type': 'STRING', 'mode': 'MUTABLE'}}");

    AssertHelper.assertThrows(
        "throws exception when a subworkflow source restarts and tries to mutate params with MUTABLE_ON_START mode",
        MaestroValidationException.class,
        "Cannot modify param with mode [MUTABLE_ON_START] for parameter [subworkflow_version]",
        () -> ParamsMergeHelper.mergeParams(allParams, paramsToMerge, upstreamRestartMergeContext));
  }

  @Test
  public void testMergeOutputDataParamsInvalidException() {
    Map<String, Parameter> allParams = new LinkedHashMap<>();
    Map<String, Parameter> paramsToMerge = new LinkedHashMap<>();
    paramsToMerge.put("key", StringParameter.builder().value("test").build());
    AssertHelper.assertThrows(
        "throws exception when output params to merge are not present in all params",
        MaestroValidationException.class,
        "Invalid output parameter [key], not defined in params",
        () -> ParamsMergeHelper.mergeOutputDataParams(allParams, paramsToMerge));
  }

  @Test
  public void testMergeOutputDataParamsMapParam() throws JsonProcessingException {
    Map<String, Parameter> allParams =
        parseParamMap(
            "{\"map_param\":{\"value\":{\"string_array_param\":{\"value\":[\"p1\",\"p2\"],\"type\":\"STRING_ARRAY\"},\"long_param\":{\"value\":123,\"type\":\"LONG\"},\"nested_map_param\":{\"value\":{\"nested_1\":{\"value\":\"val1\",\"type\":\"STRING\"}},\"type\":\"MAP\"}},\"type\":\"MAP\",\"evaluated_result\":{\"string_array_param\":[\"p1\",\"p2\"],\"long_param\":123,\"nested_map_param\":{\"nested_1\":\"val1\"}},\"evaluated_time\":1626893775979}}");
    Map<String, Parameter> paramsToMerge =
        parseParamMap(
            "{\"map_param\":{\"value\":{\"string_array_param\":{\"value\":[\"p3\",\"p4\"],\"type\":\"STRING_ARRAY\"},\"nested_map_param\":{\"value\":{\"nested_2\":{\"value\":\"val2\",\"type\":\"STRING\"}},\"type\":\"MAP\"}},\"type\":\"MAP\",\"evaluated_result\":{\"string_array_param\":[\"p3\",\"p4\"],\"nested_map_param\":{\"nested_2\":\"val2\"}},\"evaluated_time\":1626893775979}}");
    ParamsMergeHelper.mergeOutputDataParams(allParams, paramsToMerge);

    Map<String, ParamDefinition> mergedMapParamDef = allParams.get("map_param").getValue();
    Map<String, Object> mergedMapEvaluated = allParams.get("map_param").getEvaluatedResult();
    // verify string array is replaced in both def and evaluated result.
    assertArrayEquals(
        new String[] {"p3", "p4"},
        mergedMapParamDef.get("string_array_param").asStringArrayParamDef().getValue());
    assertArrayEquals(
        new String[] {"p3", "p4"}, (String[]) mergedMapEvaluated.get("string_array_param"));
    // verify the long param is retained in both def and evaluated result
    assertEquals(123, (long) mergedMapParamDef.get("long_param").asLongParamDef().getValue());
    assertEquals(123, (long) mergedMapEvaluated.get("long_param"));
    // verify the nested map param is merged in both def and evaluated result
    Map<String, ParamDefinition> nestedMap =
        mergedMapParamDef.get("nested_map_param").asMapParamDef().getValue();
    assertEquals(2, nestedMap.size());
    assertTrue(nestedMap.containsKey("nested_1"));
    assertTrue(nestedMap.containsKey("nested_2"));
    Map<String, Object> nestedEvaluated =
        (Map<String, Object>) mergedMapEvaluated.get("nested_map_param");
    assertEquals("val1", nestedEvaluated.get("nested_1"));
    assertEquals("val2", nestedEvaluated.get("nested_2"));
  }

  @Test
  public void testMergeOutputDataParamsStringMapParam() throws JsonProcessingException {
    Map<String, Parameter> allParams =
        parseParamMap(
            "{\"string_map_param\":{\"evaluated_result\":{\"a\":\"b\",\"b\":\"d\"},"
                + "\"evaluated_time\":1626893775979,\"type\":\"STRING_MAP\",\"value\":{\"a\":\"b\",\"b\":\"d\"}}}");
    Map<String, Parameter> paramsToMerge =
        parseParamMap(
            "{\"string_map_param\":{\"evaluated_result\":{\"c\":\"e\",\"d\":\"f\"},\"evaluated_time\":1626893775979,\"type\":\"STRING_MAP\",\"value\":{\"c\":\"e\",\"d\":\"f\"}}}");
    ParamsMergeHelper.mergeOutputDataParams(allParams, paramsToMerge);
    Map<String, String> mergedStringMap = allParams.get("string_map_param").getValue();
    Map<String, String> mergedEvaluated = allParams.get("string_map_param").getEvaluatedResult();
    assertEquals(4, mergedStringMap.size());
    for (String key : new String[] {"a", "b", "c", "d"}) {
      assertTrue(mergedStringMap.containsKey(key));
      assertTrue(mergedEvaluated.containsKey(key));
    }
  }

  @Test
  public void testMergeOutputDataParamsOtherParams() throws JsonProcessingException {
    Map<String, Parameter> allParams =
        parseParamMap(
            "{\"long_array_param\":{\"evaluated_result\":[1,2,3],\"evaluated_time\":1626893775979,\"type\":\"LONG_ARRAY\",\"value\":[1,2,3]},\"long_param\":{\"evaluated_result\":21,\"evaluated_time\":1626893775979,\"type\":\"LONG\",\"value\":21},\"str_param\":{\"evaluated_result\":\"hello\",\"evaluated_time\":1626893775979,\"type\":\"STRING\",\"value\":\"hello\"},\"double_param\":{\"evaluated_result\":3.14,\"evaluated_time\":1626893775979,\"type\":\"DOUBLE\",\"value\":3.14},\"double_array_param\":{\"evaluated_result\":[1.1,-0.5,3.2],\"evaluated_time\":1626893775979,\"type\":\"DOUBLE_ARRAY\",\"value\":[1.1,-0.5,3.2]},\"boolean_param\":{\"evaluated_result\":false,\"evaluated_time\":1626893775979,\"type\":\"BOOLEAN\",\"value\":false},\"boolean_array_param\":{\"evaluated_result\":[true,false,true],\"evaluated_time\":1626893775979,\"type\":\"BOOLEAN_ARRAY\",\"value\":[true,false,true]}}");
    Map<String, Parameter> paramsToMerge =
        parseParamMap(
            "{\"long_array_param\":{\"evaluated_result\":[4,5],\"evaluated_time\":1626893775979,\"type\":\"LONG_ARRAY\",\"value\":[4,5]},\"long_param\":{\"evaluated_result\":21,\"evaluated_time\":1626893775979,\"type\":\"LONG\",\"value\":21},\"double_param\":{\"evaluated_result\":3.14,\"evaluated_time\":1626893775979,\"type\":\"DOUBLE\",\"value\":3.14},\"double_array_param\":{\"evaluated_result\":[1.1],\"evaluated_time\":1626893775979,\"type\":\"DOUBLE_ARRAY\",\"value\":[1.1]},\"boolean_param\":{\"evaluated_result\":false,\"evaluated_time\":1626893775979,\"type\":\"BOOLEAN\",\"value\":false},\"boolean_array_param\":{\"evaluated_result\":[true,true],\"evaluated_time\":1626893775979,\"type\":\"BOOLEAN_ARRAY\",\"value\":[true,true]}}");
    ParamsMergeHelper.mergeOutputDataParams(allParams, paramsToMerge);
    assertArrayEquals(
        new long[] {4, 5}, allParams.get("long_array_param").asLongArrayParam().getValue());
    assertEquals(21, (long) allParams.get("long_param").asLongParam().getValue());
    assertEquals("hello", allParams.get("str_param").asStringParam().getValue());
    assertEquals(
        3.14, allParams.get("double_param").asDoubleParam().getValue().doubleValue(), 0.01);
    assertArrayEquals(
        new double[] {1.1},
        allParams.get("double_array_param").asDoubleArrayParam().getEvaluatedResult(),
        0.01);
    assertEquals(false, allParams.get("boolean_param").asBooleanParam().getValue());
    assertArrayEquals(
        new boolean[] {true, true},
        allParams.get("boolean_array_param").asBooleanArrayParam().getValue());
  }
}
