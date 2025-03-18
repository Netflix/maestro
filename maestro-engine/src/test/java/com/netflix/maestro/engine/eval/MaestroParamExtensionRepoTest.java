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
package com.netflix.maestro.engine.eval;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.models.initiator.ManualInitiator;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroParamExtensionRepoTest extends MaestroEngineBaseTest {

  @Mock MaestroStepInstanceDao stepInstanceDao;
  private MaestroParamExtensionRepo extensionRepo;

  @Before
  public void before() throws Exception {
    extensionRepo = new MaestroParamExtensionRepo(stepInstanceDao, null, MAPPER);
    extensionRepo.initialize();
  }

  @After
  public void tearDown() {
    extensionRepo.shutdown();
  }

  @Test
  public void testResetGetClear() throws Exception {
    extensionRepo.reset(
        Collections.emptyMap(),
        null,
        InstanceWrapper.builder().workflowId("foo").initiator(new ManualInitiator()).build());
    assertNotNull(extensionRepo.get());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor
        .submit(
            () -> {
              assertNull(extensionRepo.get());
              extensionRepo.clear();
            })
        .get();
    assertNotNull(extensionRepo.get());
    extensionRepo.clear();
    assertNull(extensionRepo.get());
    executor.shutdown();
  }
}
