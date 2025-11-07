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
package com.netflix.maestro.engine.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.exceptions.MaestroDatabaseError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MaestroJobTemplateDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_JOB_TYPE1 = "shell";
  private static final String TEST_JOB_TYPE2 = "python";
  private static final String TEST_VERSION_DEFAULT = Constants.DEFAULT_JOB_TEMPLATE_VERSION;
  private static final String TEST_VERSION_V1 = "v1";
  private static final String TEST_VERSION_V2 = "v2";

  private MaestroJobTemplateDao jobTemplateDao;
  private JobTemplate jobTemplate;

  @Before
  public void setUp() throws IOException {
    jobTemplateDao = new MaestroJobTemplateDao(DATA_SOURCE, MAPPER, CONFIG, metricRepo);
    jobTemplate = loadObject("fixtures/stepruntime/job_template.json", JobTemplate.class);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeJobTemplate(DATA_SOURCE, TEST_JOB_TYPE1, null);
  }

  @Test
  public void testUpsertAndGetJobTemplate() {
    jobTemplate.getDefinition().setVersion(TEST_VERSION_DEFAULT);
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    JobTemplate actual = jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_DEFAULT);
    assertEquals(jobTemplate, actual);
  }

  @Test
  public void testUpsertUpdatesExistingTemplate() {
    jobTemplateDao.upsertJobTemplate(jobTemplate);

    jobTemplate.getDefinition().setVersion(TEST_VERSION_DEFAULT);
    jobTemplate.getMetadata().setStatus("DEPRECATED");
    jobTemplateDao.upsertJobTemplate(jobTemplate);

    JobTemplate actual = jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_DEFAULT);
    assertEquals(jobTemplate, actual);
    assertEquals("DEPRECATED", actual.getMetadata().getStatus());
  }

  @Test
  public void testUpsertWithNullVersion() {
    jobTemplate.getDefinition().setVersion(null);
    AssertHelper.assertThrows(
        "Invalid null version",
        MaestroDatabaseError.class,
        "null value in column \"version\" of relation \"maestro_job_template\" violates not-null constraint",
        () -> jobTemplateDao.upsertJobTemplate(jobTemplate));
  }

  @Test
  public void testGetNonExistentVersionTemplate() {
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    assertNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_DEFAULT));
    assertNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V2));
    assertNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, null));
  }

  @Test
  public void testGetNonExistentTemplate() {
    assertNull(jobTemplateDao.getJobTemplate("non-existent", TEST_VERSION_DEFAULT));
  }

  @Test
  public void testUpsertMultipleVersions() {
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    jobTemplate.getDefinition().setVersion(TEST_VERSION_V2);
    jobTemplateDao.upsertJobTemplate(jobTemplate);

    JobTemplate retrieved1 = jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V1);
    JobTemplate retrieved2 = jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V2);

    assertNotNull(retrieved1);
    assertEquals(jobTemplate, retrieved2);
    assertEquals(TEST_VERSION_V1, retrieved1.getDefinition().getVersion());
    assertEquals(TEST_VERSION_V2, retrieved2.getDefinition().getVersion());
  }

  @Test
  public void testRemoveJobTemplateByVersion() {
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    jobTemplate.getDefinition().setVersion(TEST_VERSION_V2);
    jobTemplateDao.upsertJobTemplate(jobTemplate);

    int removed = jobTemplateDao.removeJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V1);
    assertEquals(1, removed);

    assertNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V1));
    assertNotNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V2));
  }

  @Test
  public void testRemoveAllVersionsForJobType() {
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    jobTemplate.getDefinition().setVersion(TEST_VERSION_V2);
    jobTemplateDao.upsertJobTemplate(jobTemplate);

    int removed = jobTemplateDao.removeJobTemplate(TEST_JOB_TYPE1, null);
    assertEquals(2, removed);

    assertNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V1));
    assertNull(jobTemplateDao.getJobTemplate(TEST_JOB_TYPE1, TEST_VERSION_V2));
  }

  @Test
  public void testRemoveNonExistentTemplate() {
    int removed = jobTemplateDao.removeJobTemplate("non-existent", TEST_VERSION_DEFAULT);
    assertEquals(0, removed);
  }

  @Test
  public void testGetJobTemplates() throws Exception {
    jobTemplateDao.upsertJobTemplate(jobTemplate);
    var jobTemplate2 = loadObject("fixtures/stepruntime/job_template.json", JobTemplate.class);
    jobTemplate2.getDefinition().setJobType(TEST_JOB_TYPE2);
    jobTemplateDao.upsertJobTemplate(jobTemplate2);

    List<JobTemplate> res = jobTemplateDao.getJobTemplates(TEST_VERSION_DEFAULT);
    assertTrue(res.isEmpty());

    res = jobTemplateDao.getJobTemplates(TEST_VERSION_V1);
    assertEquals(2, res.size());
    assertTrue(res.containsAll(List.of(jobTemplate, jobTemplate2)));

    MaestroTestHelper.removeJobTemplate(DATA_SOURCE, TEST_JOB_TYPE2, null);
  }
}
