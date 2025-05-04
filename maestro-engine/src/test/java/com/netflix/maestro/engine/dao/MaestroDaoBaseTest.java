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
package com.netflix.maestro.engine.dao;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.MaestroDatabaseHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.utils.IdHelper;
import javax.sql.DataSource;

public abstract class MaestroDaoBaseTest extends MaestroEngineBaseTest {
  protected DatabaseConfiguration config = MaestroDatabaseHelper.getConfig();
  protected DataSource dataSource = MaestroDatabaseHelper.getDataSource();

  protected WorkflowDefinition loadWorkflow(String workflowId) throws Exception {
    WorkflowDefinition wfd =
        loadObject(
            "fixtures/workflows/definition/" + workflowId + ".json", WorkflowDefinition.class);
    assertEquals(workflowId, wfd.getWorkflow().getId());
    wfd.setTriggerUuids(IdHelper.toTriggerUuids(wfd.getWorkflow()));
    return wfd;
  }
}
