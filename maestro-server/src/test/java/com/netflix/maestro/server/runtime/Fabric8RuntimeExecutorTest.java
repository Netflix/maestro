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
package com.netflix.maestro.server.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.kubernetes.KubernetesJobResult;
import com.netflix.maestro.engine.kubernetes.KubernetesStepContext;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class Fabric8RuntimeExecutorTest extends MaestroBaseTest {
  @Mock private KubernetesClient client;
  @Mock private MixedOperation podOp;
  @Mock private MixedOperation nsOp;
  @Mock private PodResource podResource;

  private Fabric8RuntimeExecutor executor;

  @Before
  public void setUp() {
    executor = new Fabric8RuntimeExecutor(client);
    when(client.pods()).thenReturn(podOp);
    when(podOp.inNamespace("default")).thenReturn(nsOp);
    when(nsOp.resource(any(Pod.class))).thenReturn(podResource);
    Pod pod = new Pod();
    PodStatus status = new PodStatus();
    status.setPhase("Running");
    pod.setStatus(status);
    when(podResource.create()).thenReturn(pod);
  }

  @Test
  public void testLaunchJobWithCommandAndArgs() {
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .command(new String[] {"echo"})
            .args(new String[] {"hello", "world"})
            .env(Collections.emptyMap())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    KubernetesJobResult result = executor.launchJob(context);
    assertEquals(StepRuntime.State.CONTINUE, result.jobStatus());

    Pod pod = podCaptor.getValue();
    var container = pod.getSpec().getContainers().getFirst();
    assertEquals(List.of("echo"), container.getCommand());
    assertEquals(List.of("hello", "world"), container.getArgs());
  }

  @Test
  public void testLaunchJobWithArgsOnly() {
    // Default command ["/bin/sh", "-c"] from YAML is resolved by the time it reaches here
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .command(new String[] {"/bin/sh", "-c"})
            .args(new String[] {"echo hello"})
            .env(Collections.emptyMap())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    executor.launchJob(context);

    Pod pod = podCaptor.getValue();
    var container = pod.getSpec().getContainers().getFirst();
    assertEquals(List.of("/bin/sh", "-c"), container.getCommand());
    assertEquals(List.of("echo hello"), container.getArgs());
  }

  @Test
  public void testLaunchJobWithNeitherCommandNorArgs() {
    // Neither set — image defaults apply
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .env(Collections.emptyMap())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    executor.launchJob(context);

    Pod pod = podCaptor.getValue();
    var container = pod.getSpec().getContainers().getFirst();
    assertTrue(container.getCommand() == null || container.getCommand().isEmpty());
    assertTrue(container.getArgs() == null || container.getArgs().isEmpty());
  }

  @Test
  public void testLaunchJobWithoutPreStop() {
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .command(new String[] {"echo"})
            .args(new String[] {"hello"})
            .env(Collections.emptyMap())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    executor.launchJob(context);

    Pod pod = podCaptor.getValue();
    var container = pod.getSpec().getContainers().getFirst();
    assertNull(container.getLifecycle());
  }

  @Test
  public void testLaunchJobWithPreStopExec() {
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .command(new String[] {"echo"})
            .args(new String[] {"hello"})
            .env(Collections.emptyMap())
            .preStop(
                KubernetesCommand.PreStop.builder()
                    .exec(
                        KubernetesCommand.PreStop.Exec.builder()
                            .command(new String[] {"/bin/sh", "-c", "cleanup.sh --flag"})
                            .build())
                    .build())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    executor.launchJob(context);

    Pod pod = podCaptor.getValue();
    var container = pod.getSpec().getContainers().getFirst();
    assertEquals(
        List.of("/bin/sh", "-c", "cleanup.sh --flag"),
        container.getLifecycle().getPreStop().getExec().getCommand());
  }

  @Test
  public void testLaunchJobWithResourceLimitsAndRequests() {
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .cpu("2")
            .cpuRequest("1")
            .memory("4G")
            .memoryRequest("2G")
            .disk("10G")
            .diskRequest("5G")
            .gpu("1")
            .env(Collections.emptyMap())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    executor.launchJob(context);

    var resources = podCaptor.getValue().getSpec().getContainers().getFirst().getResources();
    assertEquals(new Quantity("2"), resources.getLimits().get("cpu"));
    assertEquals(new Quantity("4G"), resources.getLimits().get("memory"));
    assertEquals(new Quantity("10G"), resources.getLimits().get("ephemeral-storage"));
    assertEquals(new Quantity("1"), resources.getLimits().get("nvidia.com/gpu"));
    assertEquals(new Quantity("1"), resources.getRequests().get("cpu"));
    assertEquals(new Quantity("2G"), resources.getRequests().get("memory"));
    assertEquals(new Quantity("5G"), resources.getRequests().get("ephemeral-storage"));
    assertNull(resources.getRequests().get("nvidia.com/gpu"));
  }

  @Test
  public void testLaunchJobWithResourceLimitsOnly() {
    KubernetesCommand command =
        KubernetesCommand.builder()
            .jobDeduplicationKey("test-job")
            .image("test-image")
            .cpu("2")
            .memory("4G")
            .disk("10G")
            .env(Collections.emptyMap())
            .build();
    KubernetesStepContext context = buildContext(command);

    ArgumentCaptor<Pod> podCaptor = ArgumentCaptor.forClass(Pod.class);
    when(nsOp.resource(podCaptor.capture())).thenReturn(podResource);

    executor.launchJob(context);

    var resources = podCaptor.getValue().getSpec().getContainers().getFirst().getResources();
    assertEquals(new Quantity("2"), resources.getLimits().get("cpu"));
    assertEquals(new Quantity("4G"), resources.getLimits().get("memory"));
    assertEquals(new Quantity("10G"), resources.getLimits().get("ephemeral-storage"));
    assertTrue(resources.getRequests() == null || resources.getRequests().isEmpty());
  }

  private KubernetesStepContext buildContext(KubernetesCommand command) {
    KubernetesStepContext context =
        new KubernetesStepContext(
            new WorkflowSummary(), StepRuntimeSummary.builder().build(), null);
    context.setCommand(command);
    return context;
  }
}
