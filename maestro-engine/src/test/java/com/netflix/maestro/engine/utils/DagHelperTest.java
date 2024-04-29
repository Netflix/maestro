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
package com.netflix.maestro.engine.utils;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DagHelperTest extends MaestroEngineBaseTest {
  private Map<String, StepTransition> runtimeDag1;
  private Map<String, StepTransition> runtimeDag2;

  @Before
  public void setup() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-dag-test-1-wf.json", WorkflowCreateRequest.class);
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRuntimeWorkflow(request.getWorkflow());
    runtimeDag1 = new DagTranslator(MAPPER).translate(instance);

    request =
        loadObject(
            "fixtures/workflows/request/sample-dag-test-2-wf.json", WorkflowCreateRequest.class);
    instance = new WorkflowInstance();
    instance.setRuntimeWorkflow(request.getWorkflow());
    runtimeDag2 = new DagTranslator(MAPPER).translate(instance);
  }

  @Test
  public void testComplete() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_5", Boolean.TRUE);
    idStatusMap.put("job_2", Boolean.TRUE);
    idStatusMap.put("job_3", Boolean.TRUE);
    idStatusMap.put("job_4", Boolean.TRUE);
    idStatusMap.put("job_1", Boolean.TRUE);
    Assert.assertTrue(DagHelper.isDone(runtimeDag2, idStatusMap, null));
  }

  @Test
  public void testIsDone() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-dag-test-3-wf.json", WorkflowCreateRequest.class);
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRuntimeWorkflow(request.getWorkflow());
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_1", Boolean.FALSE);
    Assert.assertTrue(
        DagHelper.isDone(new DagTranslator(MAPPER).translate(instance), idStatusMap, null));
  }

  @Test
  public void testDoneWithFailedStep() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_5", Boolean.TRUE);
    idStatusMap.put("job_2", Boolean.TRUE);
    idStatusMap.put("job_3", Boolean.FALSE);
    Assert.assertTrue(DagHelper.isDone(runtimeDag2, idStatusMap, null));
  }

  @Test
  public void testIncomplete() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_5", Boolean.TRUE);
    idStatusMap.put("job_2", Boolean.TRUE);
    idStatusMap.put("job_3", Boolean.TRUE);
    Assert.assertFalse(DagHelper.isDone(runtimeDag2, idStatusMap, null));
  }

  @Test
  public void testRestartDone() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_9", Boolean.TRUE);
    idStatusMap.put("job_8", Boolean.TRUE);
    Assert.assertTrue(
        DagHelper.isDone(
            runtimeDag1,
            idStatusMap,
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_9").build()));
  }

  @Test
  public void testRestartDoneWithFailedStep() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_3", Boolean.FALSE);
    idStatusMap.put("job_9", Boolean.TRUE);
    idStatusMap.put("job_8", Boolean.TRUE);
    Assert.assertTrue(
        DagHelper.isDone(
            runtimeDag1,
            idStatusMap,
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_9").build()));
  }

  @Test
  public void testRestartIncomplete() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_3", Boolean.FALSE);
    idStatusMap.put("job_9", Boolean.TRUE);
    Assert.assertFalse(
        DagHelper.isDone(
            runtimeDag1,
            idStatusMap,
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_3").build()));
  }

  @Test
  public void testRestartPathIncomplete() {
    Map<String, Boolean> idStatusMap = new LinkedHashMap<>();
    idStatusMap.put("job_9", Boolean.TRUE);
    idStatusMap.put("job_8", Boolean.TRUE);
    Assert.assertFalse(
        DagHelper.isDone(
            runtimeDag1,
            idStatusMap,
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_3").build()));
  }

  @Test
  public void testComputeStepIdsInRuntimeDagForStart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRuntimeDag(runtimeDag2);
    Set<String> actual = DagHelper.computeStepIdsInRuntimeDag(instance, Collections.emptySet());
    Assert.assertEquals("[job_1, job_3, job_2, job_5, job_4]", actual.toString());
  }

  @Test
  public void testComputeStepIdsInRuntimeDagForInstanceRestart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRunConfig(new RunConfig());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_BEGINNING);
    instance.setRuntimeDag(runtimeDag2);
    Set<String> actual =
        DagHelper.computeStepIdsInRuntimeDag(instance, Collections.singleton("job_2"));
    Assert.assertEquals("[job_1, job_3, job_2, job_5, job_4]", actual.toString());
  }

  @Test
  public void testComputeStepIdsInRuntimeDagForStepRestart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRunConfig(new RunConfig());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_9").build());
    instance.setRuntimeDag(runtimeDag1);
    Set<String> actual =
        DagHelper.computeStepIdsInRuntimeDag(instance, Collections.singleton("job_9"));
    Assert.assertEquals("[job_9, job_8]", actual.toString());
  }

  @Test
  public void testComputeStepIdsInRuntimeDagForStepRestartAnotherPath() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRunConfig(new RunConfig());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_9").build());
    instance.setRuntimeDag(runtimeDag1);
    Set<String> actual =
        DagHelper.computeStepIdsInRuntimeDag(instance, Collections.singleton("job_3"));
    Assert.assertEquals(
        "[job_1, job_3, job_11, job_2, job_10, job_5, job_4, job_7, job_6, job_9, job_8]",
        actual.toString());
  }

  @Test
  public void testInvalidComputeStepIdsInRuntimeDag() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setRunConfig(new RunConfig());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_6").build());
    instance.setRuntimeDag(runtimeDag1);
    AssertHelper.assertThrows(
        "Invalid DAG and restart config ",
        IllegalArgumentException.class,
        "Invalid state: stepId [job_6] should be one of start nodes in the DAG",
        () -> DagHelper.computeStepIdsInRuntimeDag(instance, Collections.singleton("job_3")));

    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_3").build());
    AssertHelper.assertThrows(
        "Invalid status for steps",
        IllegalArgumentException.class,
        "Invalid state: stepId [job_6] should not have any status",
        () -> DagHelper.computeStepIdsInRuntimeDag(instance, Collections.singleton("job_6")));
  }

  @Test
  public void testGetNotCreatedRootNodesInRestartRuntimeDag() {
    RestartConfig config =
        RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_3").build();
    Set<String> actual = DagHelper.getNotCreatedRootNodesInRestartRuntimeDag(runtimeDag1, config);
    Assert.assertEquals(Collections.singleton("job_9"), actual);
  }

  @Test
  public void testGetNotCreatedRootNodesInRestartRuntimeDagInvalid() {
    RestartConfig config =
        RestartConfig.builder().addRestartNode("sample-dag-test-1", 1, "job_6").build();
    AssertHelper.assertThrows(
        "Invalid status for steps",
        IllegalArgumentException.class,
        "stepId [job_6] should be one of root nodes in the DAG",
        () -> DagHelper.getNotCreatedRootNodesInRestartRuntimeDag(runtimeDag1, config));
  }
}
