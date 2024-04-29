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
package com.netflix.maestro.engine.transformation;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DagTranslatorTest extends MaestroBaseTest {
  private DagTranslator translator;
  private WorkflowInstance instance;

  @Before
  public void before() throws Exception {
    translator = new DagTranslator(MAPPER);

    Map<String, StepAggregatedView> stepMap = new HashMap<>();
    stepMap.put("job1", StepAggregatedView.builder().status(StepInstance.Status.SUCCEEDED).build());
    stepMap.put(
        "job3", StepAggregatedView.builder().status(StepInstance.Status.FATALLY_FAILED).build());
    WorkflowInstanceAggregatedInfo aggregatedInfo = new WorkflowInstanceAggregatedInfo();
    aggregatedInfo.setStepAggregatedViews(stepMap);

    instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-failed.json", WorkflowInstance.class);
    instance.setAggregatedInfo(aggregatedInfo);
    instance.setRunConfig(new RunConfig());
  }

  @Test
  public void testTranslateIncludingAllSteps() {
    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("job1", "job.2", "job3", "job4")), dag.keySet());
  }

  @Test
  public void testTranslateForRestartFromBeginning() {
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_BEGINNING);

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("job1", "job.2", "job3", "job4")), dag.keySet());
  }

  @Test
  public void testTranslateForRestartFromIncomplete() {
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(new HashSet<>(Arrays.asList("job.2", "job3", "job4")), dag.keySet());
    StepTransition jobTransition = new StepTransition();
    jobTransition.setPredecessors(Collections.singletonList("job3"));
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job.2"));

    jobTransition.setPredecessors(Collections.emptyList());
    jobTransition.setSuccessors(new HashMap<>());
    jobTransition.getSuccessors().put("job.2", "true");
    jobTransition.getSuccessors().put("job4", "true");
    Assert.assertEquals(jobTransition, dag.get("job3"));

    jobTransition.setPredecessors(Arrays.asList("job3", "job.2"));
    jobTransition.setSuccessors(Collections.emptyMap());
    Assert.assertEquals(jobTransition, dag.get("job4"));
  }

  @Test
  public void testTranslateForRestartFromIncompleteWithNotCreatedSteps() {
    instance
        .getAggregatedInfo()
        .getStepAggregatedViews()
        .put("job3", StepAggregatedView.builder().status(StepInstance.Status.NOT_CREATED).build());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(new HashSet<>(Arrays.asList("job.2", "job3", "job4")), dag.keySet());
    StepTransition jobTransition = new StepTransition();
    jobTransition.setPredecessors(Collections.singletonList("job3"));
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job.2"));

    jobTransition.setPredecessors(Collections.emptyList());
    jobTransition.setSuccessors(new HashMap<>());
    jobTransition.getSuccessors().put("job.2", "true");
    jobTransition.getSuccessors().put("job4", "true");
    Assert.assertEquals(jobTransition, dag.get("job3"));

    jobTransition.setPredecessors(Arrays.asList("job3", "job.2"));
    jobTransition.setSuccessors(Collections.emptyMap());
    Assert.assertEquals(jobTransition, dag.get("job4"));
  }

  @Test
  public void testTranslateForRestartFromSpecific() {
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder()
                .addRestartNode("sample-dag-test-3", 1, "job3")
                .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                .build());

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(new HashSet<>(Arrays.asList("job.2", "job3", "job4")), dag.keySet());
    StepTransition jobTransition = new StepTransition();
    jobTransition.setPredecessors(Collections.singletonList("job3"));
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job.2"));

    jobTransition.setPredecessors(Collections.emptyList());
    jobTransition.setSuccessors(new HashMap<>());
    jobTransition.getSuccessors().put("job.2", "true");
    jobTransition.getSuccessors().put("job4", "true");
    Assert.assertEquals(jobTransition, dag.get("job3"));

    jobTransition.setPredecessors(Arrays.asList("job3", "job.2"));
    jobTransition.setSuccessors(Collections.emptyMap());
    Assert.assertEquals(jobTransition, dag.get("job4"));
  }

  @Test
  public void testTranslateForRestartCustomizedRun() {
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_CUSTOMIZED_RUN);
    instance.getRunConfig().setStartStepIds(Collections.singletonList("job3"));

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(new HashSet<>(Arrays.asList("job.2", "job3", "job4")), dag.keySet());
    StepTransition jobTransition = new StepTransition();
    jobTransition.setPredecessors(Collections.singletonList("job3"));
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job.2"));

    jobTransition.setPredecessors(Collections.emptyList());
    jobTransition.setSuccessors(new HashMap<>());
    jobTransition.getSuccessors().put("job.2", "true");
    jobTransition.getSuccessors().put("job4", "true");
    Assert.assertEquals(jobTransition, dag.get("job3"));

    jobTransition.setPredecessors(Arrays.asList("job3", "job.2"));
    jobTransition.setSuccessors(Collections.emptyMap());
    Assert.assertEquals(jobTransition, dag.get("job4"));
  }

  @Test
  public void testTranslateForRestartFromSpecificWithTwoBranches() {
    instance.getRuntimeWorkflow().getSteps().get(2).getTransition().getSuccessors().remove("job.2");
    instance
        .getAggregatedInfo()
        .getStepAggregatedViews()
        .put("job.2", StepAggregatedView.builder().status(StepInstance.Status.STOPPED).build());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder()
                .addRestartNode("sample-dag-test-3", 1, "job3")
                .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                .build());

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(new HashSet<>(Arrays.asList("job.2", "job3", "job4")), dag.keySet());
    StepTransition jobTransition = new StepTransition();
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job.2"));

    jobTransition.setPredecessors(Collections.emptyList());
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job3"));

    jobTransition.setPredecessors(Arrays.asList("job3", "job.2"));
    jobTransition.setSuccessors(Collections.emptyMap());
    Assert.assertEquals(jobTransition, dag.get("job4"));
  }

  @Test
  public void testTranslateForRestartFromSpecificWithCompleteBranch() {
    instance.getRuntimeWorkflow().getSteps().get(2).getTransition().getSuccessors().remove("job.2");
    instance
        .getAggregatedInfo()
        .getStepAggregatedViews()
        .put("job.2", StepAggregatedView.builder().status(StepInstance.Status.STOPPED).build());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder()
                .addRestartNode("sample-dag-test-3", 1, "job1")
                .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                .build());

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("job1", "job.2", "job3", "job4")), dag.keySet());
  }

  @Test
  public void testTranslateForRestartFromSpecificWithNotCreatedSteps() {
    instance
        .getAggregatedInfo()
        .getStepAggregatedViews()
        .put("job3", StepAggregatedView.builder().status(StepInstance.Status.NOT_CREATED).build());
    instance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_SPECIFIC);
    instance
        .getRunConfig()
        .setRestartConfig(
            RestartConfig.builder()
                .addRestartNode("sample-dag-test-3", 1, "job3")
                .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                .build());

    Map<String, StepTransition> dag = translator.translate(instance);
    Assert.assertEquals(new HashSet<>(Arrays.asList("job.2", "job3", "job4")), dag.keySet());
    StepTransition jobTransition = new StepTransition();
    jobTransition.setPredecessors(Collections.singletonList("job3"));
    jobTransition.setSuccessors(Collections.singletonMap("job4", "true"));
    Assert.assertEquals(jobTransition, dag.get("job.2"));

    jobTransition.setPredecessors(Collections.emptyList());
    jobTransition.setSuccessors(new HashMap<>());
    jobTransition.getSuccessors().put("job.2", "true");
    jobTransition.getSuccessors().put("job4", "true");
    Assert.assertEquals(jobTransition, dag.get("job3"));

    jobTransition.setPredecessors(Arrays.asList("job3", "job.2"));
    jobTransition.setSuccessors(Collections.emptyMap());
    Assert.assertEquals(jobTransition, dag.get("job4"));
  }
}
