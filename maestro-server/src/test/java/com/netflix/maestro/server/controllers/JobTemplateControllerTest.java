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
package com.netflix.maestro.server.controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroJobTemplateDao;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.api.JobTemplateCreateRequest;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class JobTemplateControllerTest extends MaestroBaseTest {
  @Mock private MaestroJobTemplateDao jobTemplateDao;
  @Mock private User.UserBuilder callerBuilder;

  private JobTemplateController jobTemplateController;

  @Before
  public void setUp() {
    when(callerBuilder.build()).thenReturn(User.builder().name("test-caller").build());
    jobTemplateController = new JobTemplateController(jobTemplateDao, callerBuilder);
  }

  @Test
  public void testUpsertJobTemplate() throws IOException {
    JobTemplateCreateRequest request =
        loadObject(
            "fixtures/api/sample-job-template-create-request.json", JobTemplateCreateRequest.class);
    JobTemplate result = jobTemplateController.upsertJobTemplate(request);

    assertNotNull(result);
    assertEquals("shell", result.getDefinition().getJobType());
    assertEquals(StepType.NOTEBOOK, result.getDefinition().getStepType());
    assertEquals("v1", result.getDefinition().getVersion());
    assertEquals("tester", result.getMetadata().getOwner().getName());
    assertEquals("test-caller", result.getMetadata().getVersionAuthor().getName());
    assertEquals("alpha", result.getMetadata().getStatus());
    assertEquals("support-team", result.getMetadata().getSupport());
    assertEquals(1, result.getMetadata().getTestWorkflows().size());
    assertEquals(
        "121EF7B36826EDEDEB662F53A12DB5951E1AE1D7", result.getMetadata().getGitInfo().getSha());
    assertEquals("bar", result.getMetadata().getExtraInfo().get("foo"));

    verify(jobTemplateDao, times(1)).upsertJobTemplate(eq(result));

    JobTemplate result2 = jobTemplateController.upsertJobTemplateYaml(request);
    result.getMetadata().setCreateTime(0);
    result2.getMetadata().setCreateTime(0);
    assertEquals(result, result2);
    verify(jobTemplateDao, times(2)).upsertJobTemplate(Mockito.any(JobTemplate.class));
  }

  @Test
  public void testGetJobTemplate() {
    when(jobTemplateDao.getJobTemplate("shell", "v1")).thenReturn(Mockito.mock(JobTemplate.class));

    jobTemplateController.getJobTemplate("shell", "v1");

    verify(jobTemplateDao, times(1)).getJobTemplate("shell", "v1");
  }

  @Test
  public void testGetJobTemplateNotFound() {
    when(jobTemplateDao.getJobTemplate("nonexisting-job", "v1")).thenReturn(null);

    AssertHelper.assertThrows(
        "Should throw exception when job template not found",
        MaestroNotFoundException.class,
        "No job template found for job_type [nonexisting-job] and version [v1]",
        () -> jobTemplateController.getJobTemplate("nonexisting-job", "v1"));

    verify(jobTemplateDao, times(1)).getJobTemplate("nonexisting-job", "v1");
  }

  @Test
  public void testRemoveJobTemplateByTag() {
    when(jobTemplateDao.removeJobTemplate("test-job", "v1")).thenReturn(1);

    ResponseEntity<?> response = jobTemplateController.removeJobTemplate("test-job", "v1");

    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals(
        "Removed [1] job template versions for job type: [test-job][v1]",
        response.getBody().toString());

    verify(jobTemplateDao, times(1)).removeJobTemplate("test-job", "v1");
  }

  @Test
  public void testRemoveAllJobTemplateVersions() {
    when(jobTemplateDao.removeJobTemplate("test-job", null)).thenReturn(3);

    ResponseEntity<?> response = jobTemplateController.removeJobTemplate("test-job", null);

    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals(
        "Removed [3] job template versions for job type: [test-job][null]",
        response.getBody().toString());

    verify(jobTemplateDao, times(1)).removeJobTemplate("test-job", null);
  }
}
