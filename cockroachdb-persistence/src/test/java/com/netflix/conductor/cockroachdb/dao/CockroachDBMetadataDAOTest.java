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
package com.netflix.conductor.cockroachdb.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.MetadataDAO;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class CockroachDBMetadataDAOTest extends CockroachDBBaseTest {
  private MetadataDAO dao;

  @Before
  public void setUp() {
    dao = new CockroachDBMetadataDAO(dataSource, objectMapper, config);
  }

  @Test(expected = ApplicationException.class)
  public void testDuplicate() {
    WorkflowDef wfd = new WorkflowDef();
    wfd.setName("foo");
    wfd.setVersion(1);

    dao.createWorkflowDef(wfd);
    dao.createWorkflowDef(wfd);
  }

  @Test
  public void testWorkflowDefOperations() throws Exception {
    WorkflowDef wfd = new WorkflowDef();
    wfd.setName("test");
    wfd.setVersion(1);
    wfd.setDescription("description");
    wfd.setCreatedBy("unit_test");
    wfd.setCreateTime(1L);
    wfd.setOwnerApp("ownerApp");
    wfd.setUpdatedBy("unit_test2");
    wfd.setUpdateTime(2L);

    dao.createWorkflowDef(wfd);

    List<WorkflowDef> all = dao.getAllWorkflowDefs();
    assertNotNull(all);
    assertEquals(1, all.size());
    assertEquals("test", all.get(0).getName());
    assertEquals(1, all.get(0).getVersion());

    WorkflowDef found = dao.getWorkflowDef("test", 1).get();
    assertEquals(wfd, found);

    wfd.setVersion(2);
    dao.createWorkflowDef(wfd);

    all = dao.getAllWorkflowDefs();
    assertNotNull(all);
    assertEquals(2, all.size());
    assertEquals("test", all.get(0).getName());
    assertEquals("test", all.get(1).getName());
    assertEquals(2, all.get(0).getVersion());
    assertEquals(1, all.get(1).getVersion());

    found = dao.getLatestWorkflowDef(wfd.getName()).get();
    assertEquals(wfd.getName(), found.getName());
    assertEquals(wfd.getVersion(), found.getVersion());
    assertEquals(2, found.getVersion());

    wfd.setDescription("updated");
    dao.updateWorkflowDef(wfd);
    found = dao.getWorkflowDef(wfd.getName(), wfd.getVersion()).get();
    assertEquals(wfd.getDescription(), found.getDescription());

    dao.removeWorkflowDef("test", 1);
    Optional<WorkflowDef> deleted = dao.getWorkflowDef("test", 1);
    assertFalse(deleted.isPresent());
  }

  @Test
  public void testTaskDefOperations() throws Exception {
    TaskDef def = new TaskDef("taskA");
    def.setDescription("description");
    def.setCreatedBy("unit_test");
    def.setCreateTime(1L);
    def.setInputKeys(Arrays.asList("a", "b", "c"));
    def.setOutputKeys(Arrays.asList("01", "o2"));
    def.setOwnerApp("ownerApp");
    def.setRetryCount(3);
    def.setRetryDelaySeconds(100);
    def.setRetryLogic(TaskDef.RetryLogic.FIXED);
    def.setTimeoutPolicy(TaskDef.TimeoutPolicy.ALERT_ONLY);
    def.setUpdatedBy("unit_test2");
    def.setUpdateTime(2L);

    dao.createTaskDef(def);

    TaskDef found = dao.getTaskDef(def.getName());
    assertEquals(def, found);

    def.setDescription("updated description");
    dao.updateTaskDef(def);
    found = dao.getTaskDef(def.getName());
    assertEquals(def, found);
    assertEquals("updated description", found.getDescription());

    for (int i = 0; i < 9; i++) {
      TaskDef tdf = new TaskDef("taskA" + i);
      dao.createTaskDef(tdf);
    }

    List<TaskDef> all = dao.getAllTaskDefs();
    assertNotNull(all);
    assertEquals(10, all.size());
    Set<String> allnames = all.stream().map(TaskDef::getName).collect(Collectors.toSet());
    assertEquals(10, allnames.size());
    List<String> sorted = allnames.stream().sorted().collect(Collectors.toList());
    assertEquals(def.getName(), sorted.get(0));

    for (int i = 0; i < 9; i++) {
      assertEquals(def.getName() + i, sorted.get(i + 1));
    }

    for (int i = 0; i < 9; i++) {
      dao.removeTaskDef(def.getName() + i);
    }
    all = dao.getAllTaskDefs();
    assertNotNull(all);
    assertEquals(1, all.size());
    assertEquals(def.getName(), all.get(0).getName());
  }

  @Test(expected = ApplicationException.class)
  public void testRemoveTaskDef() throws Exception {
    dao.removeTaskDef("test" + UUID.randomUUID().toString());
  }
}
