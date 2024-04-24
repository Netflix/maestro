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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.utils.JsonHelper;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import lombok.Data;
import org.junit.Test;

public class RunStrategyTest extends MaestroBaseTest {
  private final List<String> validCases =
      Arrays.asList(
          "{\"run_strategy\": \"sequential\"}",
          "{\"run_strategy\": \"STRICT_SEQUENTIAL\"}",
          "{\"run_strategy\": \"FIRST_ONLY\"}",
          "{\"run_strategy\": \"LAST_ONLY\"}",
          "{\"run_strategy\": \"parallel\"}",
          "{\"run_strategy\": 0}",
          "{\"run_strategy\": 1}",
          "{\"run_strategy\": 2}",
          "{\"run_strategy\": {\"rule\": \"sequential\", \"workflow_concurrency\": 1}}",
          "{\"run_strategy\": {\"rule\": \"parallel\", \"workflow_concurrency\": 2}}",
          "{\"run_strategy\": {\"rule\": \"parallel\", \"workflow_concurrency\": 1}}",
          "{\"run_strategy\": {\"rule\": \"parallel\", \"workflow_concurrency\": 0}}",
          "{\"run_strategy\": {\"rule\": \"sequential\"}}");

  private final List<Long> validCaseConcurrency =
      Arrays.asList(1L, 1L, 1L, 1L, Defaults.DEFAULT_PARALLELISM, 0L, 1L, 2L, 1L, 2L, 1L, 0L, 1L);

  private final List<String[]> invalidCases =
      Arrays.asList(
          new String[] {
            "{\"run_strategy\": {\"workflow_concurrency\": 2}}",
            "rule in run strategy cannot be null"
          },
          new String[] {
            "{\"run_strategy\": {\"rule\": \"sequential\", \"workflow_concurrency\": 2}}",
            "Only PARALLEL run strategy allows a workflow_concurrency greater than 1"
          },
          new String[] {
            "{\"run_strategy\": {\"rule\": \"STRICT_SEQUENTIAL\", \"workflow_concurrency\": 2}}",
            "Only PARALLEL run strategy allows a workflow_concurrency greater than 1 "
          },
          new String[] {
            "{\"run_strategy\": {\"rule\": \"FIRST_ONLY\", \"workflow_concurrency\": 2}}",
            "Only PARALLEL run strategy allows a workflow_concurrency greater than 1 "
          },
          new String[] {
            "{\"run_strategy\": {\"rule\": \"LAST_ONLY\", \"workflow_concurrency\": 2}}",
            "Only PARALLEL run strategy allows a workflow_concurrency greater than 1 "
          });

  @Data
  private static class TestClass {
    @JsonProperty("run_strategy")
    private RunStrategy runStrategy;
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    Iterator<Long> iter = validCaseConcurrency.iterator();
    for (String s : validCases) {
      TestClass rs = MAPPER.readValue(s, TestClass.class);
      assertEquals(iter.next().longValue(), rs.getRunStrategy().getWorkflowConcurrency());
      assertEquals(rs, MAPPER.readValue(MAPPER.writeValueAsString(rs), TestClass.class));
    }
  }

  @Test
  public void testInvalidCases() {
    for (String[] s : invalidCases) {
      AssertHelper.assertThrows(
          "those are invalid cases",
          MaestroInternalError.class,
          s[1],
          () -> JsonHelper.fromJson(MAPPER, s[0], TestClass.class));
    }
  }
}
