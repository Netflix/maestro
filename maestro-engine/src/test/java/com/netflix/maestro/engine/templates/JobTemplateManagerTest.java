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
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.properties.JobTemplateCacheProperties;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.TypedStep;
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
  private WorkflowSummary workflowSummary;
  private TypedStep step;

  @Before
  public void setUp() throws IOException {
    JobTemplateCacheProperties cacheProperties = new JobTemplateCacheProperties();
    cacheProperties.setCacheTtl(60000); // 60 seconds
    jobTemplateManager = new JobTemplateManager(jobTemplateDao, cacheProperties);
    jobTemplate = loadObject("fixtures/stepruntime/job_template.json", JobTemplate.class);
    workflowSummary = new WorkflowSummary();
    workflowSummary.setParams(
        Map.of("job_template_version", buildParam("job_template_version", "v1")));
    step = new TypedStep();
    step.setId("test-step");
    step.setType(StepType.NOTEBOOK);
    step.setSubType("shell");
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithNoTemplate() {
    when(jobTemplateDao.getJobTemplate(anyString(), anyString())).thenReturn(null);
    workflowSummary.setParams(Map.of());
    Map<String, ParamDefinition> params =
        jobTemplateManager.loadRuntimeParams(workflowSummary, step);

    assertTrue(params.isEmpty());
    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "default");

    var tags = jobTemplateManager.loadTags(workflowSummary, step);
    assertTrue(tags.isEmpty());
    verify(jobTemplateDao, times(2)).getJobTemplate("shell", "default");
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithTemplate() {
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);
    Map<String, ParamDefinition> params =
        jobTemplateManager.loadRuntimeParams(workflowSummary, step);

    assertEquals(3, params.size());
    assertEquals("echo hello world", ((StringParamDefinition) params.get("script")).getValue());

    var tags = jobTemplateManager.loadTags(workflowSummary, step);

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
        () -> jobTemplateManager.loadRuntimeParams(workflowSummary, step));

    AssertHelper.assertThrows(
        "Step type mismatch should throw exception",
        IllegalArgumentException.class,
        "Job template definition step type [KUBERNETES] does not match the current step type [NOTEBOOK]",
        () -> jobTemplateManager.loadTags(workflowSummary, step));
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

    Map<String, ParamDefinition> params =
        jobTemplateManager.loadRuntimeParams(workflowSummary, step);

    assertEquals(4, params.size());
    assertTrue(params.containsKey("kubernetes"));
    assertTrue(params.containsKey("notebook"));
    assertEquals("parent_value", ((StringParamDefinition) params.get("parent_param")).getValue());
    assertEquals("echo hello world", ((StringParamDefinition) params.get("script")).getValue());

    var tags = jobTemplateManager.loadTags(workflowSummary, step);

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

    Map<String, ParamDefinition> params =
        jobTemplateManager.loadRuntimeParams(workflowSummary, step);

    assertEquals(3, params.size());
    assertTrue(params.containsKey("kubernetes"));
    assertTrue(params.containsKey("notebook"));
    assertEquals("echo hello world", ((StringParamDefinition) params.get("script")).getValue());

    var tags = jobTemplateManager.loadTags(workflowSummary, step);
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
        () -> jobTemplateManager.loadRuntimeParams(workflowSummary, step));

    AssertHelper.assertThrows(
        "Cyclic dependency should throw exception",
        IllegalArgumentException.class,
        "Cyclic dependency detected for step [test-step][NOTEBOOK][shell] when inheriting job type [parent-job-type]",
        () -> jobTemplateManager.loadTags(workflowSummary, step));
  }

  @Test
  public void testLoadRuntimeParamsAndTagsWithTemplateStepVersion() throws IOException {
    JobTemplate templateJobTemplate =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class);
    TemplateStep templateStep = new TemplateStep();
    templateStep.setId("publish");
    templateStep.setSubType("write_audit_publish");
    templateStep.setSubTypeVersion("v3");
    when(jobTemplateDao.getJobTemplate("write_audit_publish", "v3"))
        .thenReturn(templateJobTemplate);

    Map<String, ParamDefinition> params =
        jobTemplateManager.loadRuntimeParams(workflowSummary, templateStep);

    assertEquals(2, params.size());
    assertTrue(params.containsKey("target_table"));
    assertTrue(params.containsKey("snapshot_id"));
    verify(jobTemplateDao, times(1)).getJobTemplate("write_audit_publish", "v3");
    verify(jobTemplateDao, times(0)).getJobTemplate("write_audit_publish", "v1");
  }

  @Test
  public void testLoadSteps() throws IOException {
    JobTemplate templateJobTemplate =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class);
    TemplateStep templateStep = new TemplateStep();
    templateStep.setId("publish");
    templateStep.setSubType("write_audit_publish");
    when(jobTemplateDao.getJobTemplate("write_audit_publish", "v3"))
        .thenReturn(templateJobTemplate);

    var steps = jobTemplateManager.loadSteps(templateStep, "v3");

    assertEquals(
        List.of("write", "audit", "publish_snapshot"),
        steps.stream().map(Step::getId).collect(Collectors.toList()));
    verify(jobTemplateDao, times(1)).getJobTemplate("write_audit_publish", "v3");
  }

  @Test
  public void testLoadStepsWithMissingTemplate() {
    TemplateStep templateStep = new TemplateStep();
    templateStep.setId("publish");
    templateStep.setSubType("write_audit_publish");
    when(jobTemplateDao.getJobTemplate("write_audit_publish", "v3")).thenReturn(null);

    AssertHelper.assertThrows(
        "Missing job template should throw exception",
        NullPointerException.class,
        "Cannot find the job template [write_audit_publish][v3] for the template step [publish]",
        () -> jobTemplateManager.loadSteps(templateStep, "v3"));
  }

  @Test
  public void testLoadStepsWithStepTypeMismatch() {
    TemplateStep templateStep = new TemplateStep();
    templateStep.setId("publish");
    templateStep.setSubType("shell");
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    AssertHelper.assertThrows(
        "Step type mismatch should throw exception",
        IllegalArgumentException.class,
        "Job template definition step type [NOTEBOOK] does not match the current step type [TEMPLATE]",
        () -> jobTemplateManager.loadSteps(templateStep, "v1"));
  }

  @Test
  public void testLoadStepsWithoutSteps() throws IOException {
    JobTemplate templateJobTemplate =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class);
    templateJobTemplate.getDefinition().setSteps(null);
    TemplateStep templateStep = new TemplateStep();
    templateStep.setId("publish");
    templateStep.setSubType("write_audit_publish");
    when(jobTemplateDao.getJobTemplate("write_audit_publish", "v3"))
        .thenReturn(templateJobTemplate);

    AssertHelper.assertThrows(
        "Job template without steps should throw exception",
        NullPointerException.class,
        "Job template [write_audit_publish][v3] does not define any steps for the template step [publish]",
        () -> jobTemplateManager.loadSteps(templateStep, "v3"));
  }

  @Test
  public void testGetJobTemplateVersion() {
    TemplateStep templateStep = new TemplateStep();
    templateStep.setId("publish");
    templateStep.setSubType("write_audit_publish");
    assertEquals("v1", jobTemplateManager.getJobTemplateVersion(workflowSummary, templateStep));
    templateStep.setSubTypeVersion("v3");
    assertEquals("v3", jobTemplateManager.getJobTemplateVersion(workflowSummary, templateStep));
    workflowSummary.setParams(Map.of());
    templateStep.setSubTypeVersion(null);
    assertEquals(
        "default", jobTemplateManager.getJobTemplateVersion(workflowSummary, templateStep));
  }

  @Test
  public void testCaching() {
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    jobTemplateManager.loadRuntimeParams(workflowSummary, step);
    jobTemplateManager.loadRuntimeParams(workflowSummary, step);
    jobTemplateManager.loadRuntimeParams(workflowSummary, step);

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

    WorkflowSummary workflowSummaryV2 = new WorkflowSummary();
    workflowSummaryV2.setParams(
        Map.of("job_template_version", buildParam("job_template_version", "v2")));

    jobTemplateManager.loadRuntimeParams(workflowSummary, step);
    jobTemplateManager.loadRuntimeParams(workflowSummaryV2, step);
    jobTemplateManager.loadRuntimeParams(workflowSummary, step);
    jobTemplateManager.loadRuntimeParams(workflowSummaryV2, step);

    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "v1");
    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "v2");
  }

  @Test
  public void testMergeWorkflowParamsIntoSchemaParams() {
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(jobTemplate);

    Map<String, Parameter> workflowParams = new LinkedHashMap<>();
    workflowParams.put("job_template_version", buildParam("job_template_version", "v1"));
    workflowParams.put("script", buildParam("script", "overridden script"));
    workflowParams.put("foo", buildParam("foo", "bar"));
    workflowSummary.setParams(workflowParams);

    Map<String, ParamDefinition> params =
        jobTemplateManager.loadRuntimeParams(workflowSummary, step);

    assertEquals(3, params.size());
    assertEquals("overridden script", params.get("script").asStringParamDef().getValue());
    assertTrue(params.containsKey("kubernetes"));
    assertTrue(params.containsKey("notebook"));
  }
}
