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
package com.netflix.maestro.engine.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroJobTemplateDao;
import com.netflix.maestro.engine.properties.JobTemplateCacheProperties;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class JobTemplateManagerTest extends MaestroBaseTest {
  @Mock private MaestroJobTemplateDao jobTemplateDao;
  private JobTemplateManager jobTemplateManager;
  private JobTemplate jobTemplate;
  private TypedStep step;

  @Before
  public void setUp() throws IOException {
    JobTemplateCacheProperties cacheProperties = new JobTemplateCacheProperties();
    cacheProperties.setCacheTtl(60000); // 60 seconds
    jobTemplateManager = new JobTemplateManager(jobTemplateDao, cacheProperties);
    jobTemplate = loadObject("fixtures/stepruntime/job_template.json", JobTemplate.class);
    step = new TypedStep();
    step.setId("test-step");
    step.setType(StepType.NOTEBOOK);
    step.setSubType("shell");
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithNoTemplate() {
    when(jobTemplateDao.getJobTemplate(anyString(), anyString())).thenReturn(null);
    Map<String, ParamDefinition> params = jobTemplateManager.loadRuntimeParams(step, "default");

    assertTrue(params.isEmpty());
    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "default");

    var tags = jobTemplateManager.loadTags(step, "default");
    assertTrue(tags.isEmpty());
    verify(jobTemplateDao, times(2)).getJobTemplate("shell", "default");
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithTemplate() {
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);
    Map<String, ParamDefinition> params = jobTemplateManager.loadRuntimeParams(step, "v1");

    assertEquals(3, params.size());
    assertEquals("echo hello world", ((StringParamDefinition) params.get("script")).getValue());

    var tags = jobTemplateManager.loadTags(step, "v1");

    assertEquals(3, tags.size());
    assertEquals(
        List.of("notebook", "shell", "example"),
        tags.stream().map(Tag::getName).collect(Collectors.toList()));
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithStepTypeMismatch() {
    jobTemplate.getDefinition().setStepType(StepType.KUBERNETES);
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    AssertHelper.assertThrows(
        "Step type mismatch should throw exception",
        IllegalArgumentException.class,
        "Job template definition step type [KUBERNETES] does not match the current step type [NOTEBOOK]",
        () -> jobTemplateManager.loadRuntimeParams(step, "v1"));

    AssertHelper.assertThrows(
        "Step type mismatch should throw exception",
        IllegalArgumentException.class,
        "Job template definition step type [KUBERNETES] does not match the current step type [NOTEBOOK]",
        () -> jobTemplateManager.loadTags(step, "v1"));
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithParentInheritance() {
    JobTemplate parentTemplate = new JobTemplate();
    parentTemplate.setDefinition(new JobTemplate.Definition());
    parentTemplate.getDefinition().setStepType(StepType.KUBERNETES);
    parentTemplate.getDefinition().setJobType("parent-job-type");
    parentTemplate
        .getDefinition()
        .setParams(
            Map.of("parent_param", buildParam("parent_param", "parent_value").toDefinition()));
    parentTemplate.getDefinition().setVersion("default");
    parentTemplate.getDefinition().setTags(List.of(Tag.create("foo")));

    jobTemplate.getDefinition().setInheritFrom(Map.of("parent-job-type", "default"));
    when(jobTemplateDao.getJobTemplate("parent-job-type", "default")).thenReturn(parentTemplate);
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    Map<String, ParamDefinition> params = jobTemplateManager.loadRuntimeParams(step, "v1");

    assertEquals(4, params.size());
    assertTrue(params.containsKey("kubernetes"));
    assertTrue(params.containsKey("notebook"));
    assertEquals("parent_value", ((StringParamDefinition) params.get("parent_param")).getValue());
    assertEquals("echo hello world", ((StringParamDefinition) params.get("script")).getValue());

    var tags = jobTemplateManager.loadTags(step, "v1");

    assertEquals(4, tags.size());
    assertEquals(
        List.of("foo", "notebook", "shell", "example"),
        tags.stream().map(Tag::getName).collect(Collectors.toList()));
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithParentOverride() {
    JobTemplate parentTemplate = new JobTemplate();
    parentTemplate.setDefinition(new JobTemplate.Definition());
    parentTemplate.getDefinition().setStepType(StepType.KUBERNETES);
    parentTemplate.getDefinition().setJobType("parent-job-type");
    parentTemplate
        .getDefinition()
        .setParams(Map.of("script", buildParam("script", "parent_value").toDefinition()));
    parentTemplate.getDefinition().setTags(List.of(Tag.create("notebook")));

    jobTemplate.getDefinition().setInheritFrom(Map.of("parent-job-type", "default"));
    when(jobTemplateDao.getJobTemplate("parent-job-type", "default")).thenReturn(parentTemplate);
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    Map<String, ParamDefinition> params = jobTemplateManager.loadRuntimeParams(step, "v1");

    assertEquals(3, params.size());
    assertTrue(params.containsKey("kubernetes"));
    assertTrue(params.containsKey("notebook"));
    assertEquals("echo hello world", ((StringParamDefinition) params.get("script")).getValue());

    var tags = jobTemplateManager.loadTags(step, "v1");
    assertEquals(3, tags.size());
    assertEquals(
        List.of("notebook", "shell", "example"),
        tags.stream().map(Tag::getName).collect(Collectors.toList()));
    assertEquals(Tag.Namespace.JOB_TEMPLATE, tags.iterator().next().getNamespace());
    assertEquals(Map.of("foo", "bar"), tags.iterator().next().getAttributes());
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithCyclicDependency() {
    JobTemplate parentTemplate = new JobTemplate();
    parentTemplate.setDefinition(new JobTemplate.Definition());
    parentTemplate.getDefinition().setStepType(StepType.KUBERNETES);
    parentTemplate.getDefinition().setJobType("parent-job-type");
    parentTemplate.getDefinition().setInheritFrom(Map.of("shell", "v1"));

    jobTemplate.getDefinition().setInheritFrom(Map.of("parent-job-type", "default"));
    when(jobTemplateDao.getJobTemplate("parent-job-type", "default")).thenReturn(parentTemplate);
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    AssertHelper.assertThrows(
        "Cyclic dependency should throw exception",
        IllegalArgumentException.class,
        "Cyclic dependency detected for step [test-step][NOTEBOOK][shell] when inheriting job type [parent-job-type]",
        () -> jobTemplateManager.loadRuntimeParams(step, "v1"));

    AssertHelper.assertThrows(
        "Cyclic dependency should throw exception",
        IllegalArgumentException.class,
        "Cyclic dependency detected for step [test-step][NOTEBOOK][shell] when inheriting job type [parent-job-type]",
        () -> jobTemplateManager.loadTags(step, "v1"));
  }

  @Test
  public void testCaching() {
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    jobTemplateManager.loadRuntimeParams(step, "v1");
    jobTemplateManager.loadRuntimeParams(step, "v1");
    jobTemplateManager.loadRuntimeParams(step, "v1");

    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "v1");
  }

  @Test
  public void testCacheWithDifferentTags() {
    JobTemplate jobTemplateV2 = new JobTemplate();
    jobTemplateV2.setDefinition(new JobTemplate.Definition());
    jobTemplateV2.getDefinition().setStepType(StepType.NOTEBOOK);
    jobTemplateV2.getDefinition().setJobType("shell");
    jobTemplateV2.getDefinition().setVersion("v2");

    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);
    when(jobTemplateDao.getJobTemplate("shell", "v2")).thenReturn(jobTemplateV2);

    jobTemplateManager.loadRuntimeParams(step, "v1");
    jobTemplateManager.loadRuntimeParams(step, "v2");
    jobTemplateManager.loadRuntimeParams(step, "v1");
    jobTemplateManager.loadRuntimeParams(step, "v2");

    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "v1");
    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "v2");
  }

  @Test
  public void testMergeWorkflowParamsIntoSchemaParams() {
    Map<String, ParamDefinition> schemaParams = new LinkedHashMap<>();
    schemaParams.put("cpu", StringParamDefinition.builder().name("cpu").value("1").build());
    schemaParams.put("memory", LongParamDefinition.builder().name("memory").value(1024L).build());

    Map<String, Parameter> workflowParams = new LinkedHashMap<>();
    workflowParams.put("cpu", buildParam("cpu", "2")); // Override cpu
    workflowParams.put("disk", buildParam("disk", "10G")); // New param not in schema

    jobTemplateManager.mergeWorkflowParamsIntoSchemaParams(schemaParams, workflowParams);

    assertEquals("2", ((StringParamDefinition) schemaParams.get("cpu")).getValue());
    assertEquals(1024L, ((LongParamDefinition) schemaParams.get("memory")).getValue().longValue());
    assertEquals(2, schemaParams.size());
  }
}
