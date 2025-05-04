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
package com.netflix.maestro.engine;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.eval.ExprEvaluator;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.properties.SelProperties;
import com.netflix.spectator.api.DefaultRegistry;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Maestro engine test base class. */
public abstract class MaestroEngineBaseTest extends MaestroBaseTest {
  public static ExprEvaluator evaluator;
  public static ParamEvaluator paramEvaluator;
  public static MaestroMetricRepo metricRepo;
  public static MaestroParamExtensionRepo paramExtensionRepo;

  /** start up. */
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
    paramExtensionRepo = new MaestroParamExtensionRepo(null, null, MaestroBaseTest.MAPPER);
    evaluator =
        new ExprEvaluator(
            SelProperties.builder()
                .threadNum(3)
                .timeoutMillis(120000)
                .stackLimit(128)
                .loopLimit(10000)
                .arrayLimit(10000)
                .lengthLimit(10000)
                .visitLimit(100000000L)
                .memoryLimit(10000000L)
                .build(),
            paramExtensionRepo);
    evaluator.postConstruct();
    paramEvaluator = new ParamEvaluator(evaluator, MaestroBaseTest.MAPPER);
  }

  /** clean up. */
  @AfterClass
  public static void destroy() {
    evaluator.preDestroy();
    MaestroBaseTest.destroy();
  }

  static {
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
  }
}
