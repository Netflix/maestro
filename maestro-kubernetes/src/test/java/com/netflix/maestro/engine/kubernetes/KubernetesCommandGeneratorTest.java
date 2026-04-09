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
package com.netflix.maestro.engine.kubernetes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link KubernetesCommandGenerator}. */
public class KubernetesCommandGeneratorTest extends MaestroBaseTest {
  private KubernetesCommandGenerator generator;
  private StepRuntimeSummary runtimeSummary;

  @Before
  public void setup() throws Exception {
    generator = new KubernetesCommandGenerator(MAPPER);
    runtimeSummary =
        loadObject("fixtures/execution/sample-step-runtime-summary.json", StepRuntimeSummary.class);
    runtimeSummary
        .getParams()
        .put(
            "kubernetes",
            MapParameter.builder()
                .evaluatedResult(
                    Map.of(
                        "app_name", "test-app",
                        "cpu", "0.5",
                        "disk", "1G",
                        "gpu", "0",
                        "memory", "1G",
                        "image", "test-image",
                        "entrypoint", "test-entrypoint",
                        "env",
                            Map.of(
                                "key1", "value1",
                                "key2", "value2"),
                        "job_deduplication_key", "job_deduplication_key",
                        "owner_email", "owner_email"))
                .evaluatedTime(12345L)
                .build());
  }

  @Test
  public void testGenerateMigratesDeprecatedEntrypoint() {
    // Old workflow definitions with entrypoint get migrated to args automatically
    KubernetesStepContext context =
        new KubernetesStepContext(new WorkflowSummary(), runtimeSummary, new TypedStep());
    KubernetesCommand command = generator.generate(context);
    assertEquals("test-app", command.getAppName());
    assertEquals("0.5", command.getCpu());
    assertEquals("1G", command.getDisk());
    assertEquals("0", command.getGpu());
    assertEquals("1G", command.getMemory());
    assertEquals("test-image", command.getImage());
    assertArrayEquals(new String[] {"/bin/sh", "-c"}, command.getCommand());
    assertArrayEquals(new String[] {"test-entrypoint"}, command.getArgs());
    assertEquals(Map.of("key1", "value1", "key2", "value2"), command.getEnv());
    assertEquals("job_deduplication_key", command.getJobDeduplicationKey());
    assertEquals("owner_email", command.getOwnerEmail());
  }

  @Test
  public void testGenerateWithCommandAndArgs() {
    runtimeSummary
        .getParams()
        .put(
            "kubernetes",
            MapParameter.builder()
                .evaluatedResult(
                    Map.of(
                        "image", "test-image",
                        "command", new String[] {"echo"},
                        "args", new String[] {"hello", "world"}))
                .evaluatedTime(12345L)
                .build());
    KubernetesStepContext context =
        new KubernetesStepContext(new WorkflowSummary(), runtimeSummary, new TypedStep());
    KubernetesCommand command = generator.generate(context);
    assertEquals("test-image", command.getImage());
    assertArrayEquals(new String[] {"echo"}, command.getCommand());
    assertArrayEquals(new String[] {"hello", "world"}, command.getArgs());
    assertNull(command.getEntrypoint());
  }

  @Test
  public void testGenerateWithNoCommandOrArgs() {
    // Neither command nor args set — image defaults apply
    runtimeSummary
        .getParams()
        .put(
            "kubernetes",
            MapParameter.builder()
                .evaluatedResult(Map.of("image", "test-image"))
                .evaluatedTime(12345L)
                .build());
    KubernetesStepContext context =
        new KubernetesStepContext(new WorkflowSummary(), runtimeSummary, new TypedStep());
    KubernetesCommand command = generator.generate(context);
    assertEquals("test-image", command.getImage());
    assertNull(command.getCommand());
    assertNull(command.getArgs());
    assertNull(command.getEntrypoint());
  }

  @Test
  public void testGenerateFailedValidation() {
    runtimeSummary.getParams().remove("kubernetes");
    KubernetesStepContext context =
        new KubernetesStepContext(new WorkflowSummary(), runtimeSummary, new TypedStep());

    AssertHelper.assertThrows(
        "invalid case that kubernetes params is missing",
        NullPointerException.class,
        "kubernetes params must be present",
        () -> generator.generate(context));
  }
}
