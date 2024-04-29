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

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class WorkflowGraphTest extends MaestroBaseTest {

  private static class TestTranslator implements Translator<Step, String> {
    @Override
    public String translate(Step definition) {
      return definition.getId();
    }
  }

  private static class IdentityTranslator implements Translator<Step, Step> {
    @Override
    public Step translate(Step definition) {
      return definition;
    }
  }

  @Test
  public void testComputePaths() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(), WorkflowGraph.computeDag(request.getWorkflow(), null, null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(3, paths.size());
    Assert.assertEquals(
        Arrays.asList("job.1", "job.2", "job.3", "#job.6", "job.6", "job.7"), paths.get(0));
    Assert.assertEquals(Collections.singletonList("job.4"), paths.get(1));
    Assert.assertEquals(Arrays.asList("#job.5", "job.5"), paths.get(2));
  }

  @Test
  public void testComputeDAGPath() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-dag-test-1-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(), WorkflowGraph.computeDag(request.getWorkflow(), null, null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(7, paths.size());
    Assert.assertEquals(Arrays.asList("job_3", "job_2"), paths.get(0));
    Assert.assertEquals(Arrays.asList("job_9", "#job_6", "job_6", "job_1", "job_10"), paths.get(1));
    Assert.assertEquals(Arrays.asList("#job_5", "job_5"), paths.get(2));
    Assert.assertEquals(Arrays.asList("#job_7", "job_7"), paths.get(3));
    Assert.assertEquals(Arrays.asList("#job_8", "job_8"), paths.get(4));
    Assert.assertEquals(Arrays.asList("#job_11", "job_11"), paths.get(5));
    Assert.assertEquals(Arrays.asList("#job_4", "job_4"), paths.get(6));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase1() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.1"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(2, paths.size());
    Assert.assertEquals(
        Arrays.asList("job.1", "job.2", "job.3", "#job.6", "job.6", "job.7"), paths.get(0));
    Assert.assertEquals(Arrays.asList("#job.5", "job.5"), paths.get(1));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase2() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.2"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(2, paths.size());
    Assert.assertEquals(Arrays.asList("job.2", "job.3", "#job.6", "job.6", "job.7"), paths.get(0));
    Assert.assertEquals(Arrays.asList("#job.5", "job.5"), paths.get(1));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase3() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.3"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(Arrays.asList("job.3", "job.6", "job.7"), paths.get(0));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase4() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.4"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(Collections.singletonList("job.4"), paths.get(0));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase5() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.5"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(Arrays.asList("job.5", "job.6", "job.7"), paths.get(0));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase6() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.6"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(Arrays.asList("job.6", "job.7"), paths.get(0));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase7() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Collections.singletonList("job.7"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(Collections.singletonList("job.7"), paths.get(0));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase8() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(request.getWorkflow(), Arrays.asList("job.3", "job.2"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(2, paths.size());
    Assert.assertEquals(Arrays.asList("job.2", "job.3", "#job.6", "job.6", "job.7"), paths.get(0));
    Assert.assertEquals(Arrays.asList("#job.5", "job.5"), paths.get(1));
  }

  @Test
  public void testComputePathsWithStartStepIdsCase9() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(request.getWorkflow(), Arrays.asList("job.5", "job.3"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(2, paths.size());
    Assert.assertEquals(Arrays.asList("job.3", "#job.6", "job.6", "job.7"), paths.get(0));
    Assert.assertEquals(Collections.singletonList("job.5"), paths.get(1));
  }

  @Test
  public void testComputePathsWithThreeStartStepIds() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(), Arrays.asList("job.1", "job.4", "job.7"), null));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(3, paths.size());
    Assert.assertEquals(
        Arrays.asList("job.1", "job.2", "job.3", "#job.6", "job.6", "job.7"), paths.get(0));
    Assert.assertEquals(Collections.singletonList("job.4"), paths.get(1));
    Assert.assertEquals(Arrays.asList("#job.5", "job.5"), paths.get(2));
  }

  @Test
  public void testComputePathsWithStartStepIdsAndEndStepIds() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(),
                Arrays.asList("job.5", "job.3"),
                Collections.singletonList("job.6")));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(2, paths.size());
    Assert.assertEquals(Arrays.asList("job.3", "#job.6", "job.6"), paths.get(0));
    Assert.assertEquals(Collections.singletonList("job.5"), paths.get(1));
  }

  @Test
  public void testComputePathsWithSingleStep() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(),
            WorkflowGraph.computeDag(
                request.getWorkflow(),
                Collections.singletonList("job.6"),
                Collections.singletonList("job.6")));
    List<List<String>> paths = graph.computePaths(new TestTranslator());
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(Collections.singletonList("job.6"), paths.get(0));
  }

  @Test
  public void testComputeDag() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    Map<String, StepTransition> dag =
        WorkflowGraph.computeDag(
            request.getWorkflow(),
            Collections.singletonList("job.6"),
            Collections.singletonList("job.6"));
    Assert.assertEquals(1, dag.size());
    Assert.assertEquals(Collections.singletonMap("job.6", new StepTransition()), dag);
  }

  @Test
  public void testComputePathsWithStepParamOverrides() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    ((AbstractStep) request.getWorkflow().getSteps().get(0))
        .setParams(singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "foo")));
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(), WorkflowGraph.computeDag(request.getWorkflow(), null, null));
    List<List<Step>> paths = graph.computePaths(new IdentityTranslator());
    Assert.assertEquals(3, paths.size());
    Assert.assertEquals("job.1", paths.get(0).get(0).getId());
    Assert.assertEquals(
        Collections.singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "foo")),
        paths.get(0).get(0).getParams());
  }

  @Test
  public void testComputePathsWithStepParamAdditions() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-conditional-wf.json", WorkflowCreateRequest.class);
    ((AbstractStep) request.getWorkflow().getSteps().get(0))
        .setParams(singletonMap("foo", ParamDefinition.buildParamDefinition("foo", "foo")));
    WorkflowGraph graph =
        WorkflowGraph.build(
            request.getWorkflow(), WorkflowGraph.computeDag(request.getWorkflow(), null, null));
    List<List<Step>> paths = graph.computePaths(new IdentityTranslator());
    Assert.assertEquals(3, paths.size());
    Assert.assertEquals("job.1", paths.get(0).get(0).getId());
    Assert.assertEquals(1, paths.get(0).get(0).getParams().size());
    Assert.assertEquals(
        ParamDefinition.buildParamDefinition("foo", "foo"),
        paths.get(0).get(0).getParams().get("foo"));
    Assert.assertNull(paths.get(0).get(0).getParams().get("bar"));
  }

  @Test
  public void testComputeDAGPathWithCycle() throws Exception {
    WorkflowCreateRequest request =
        loadObject(
            "fixtures/workflows/request/sample-dag-with-cycle-wf.json",
            WorkflowCreateRequest.class);

    AssertHelper.assertThrows(
        "Invalid workflow definition [sample-dag-test-1-wf], where DAG contains cycle",
        IllegalArgumentException.class,
        "Invalid workflow definition [sample-dag-test-1-wf], where DAG contains cycle",
        () -> WorkflowGraph.computeDag(request.getWorkflow(), null, null));
  }
}
