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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.utils.Checks;
import com.netflix.sel.ext.Extension;
import com.netflix.sel.type.SelUtilFunc;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;

/** A repository to hold maestro param extensions for the param evaluation. */
@SuppressWarnings({"PMD.DoNotUseThreads", "PMD.BeanMembersShouldSerialize"})
@Slf4j
public class MaestroParamExtensionRepo {
  private static final int THREAD_NUM = 3;
  private final ThreadLocal<Extension> repos = new ThreadLocal<>();
  private final MaestroStepInstanceDao stepInstanceDao;
  private final ObjectMapper objectMapper;
  private final String env;
  private ExecutorService executor;

  /** Constructor. */
  public MaestroParamExtensionRepo(
      MaestroStepInstanceDao stepInstanceDao, String env, ObjectMapper objectMapper) {
    this.stepInstanceDao = stepInstanceDao;
    this.objectMapper = objectMapper;
    this.env = env;
  }

  /** Reset repo by creating a new param extension wrapper for the current thread. */
  public void reset(
      Map<String, Map<String, Object>> allStepOutputData,
      @Nullable SignalHandler signalHandler,
      InstanceWrapper instanceWrapper) {
    Extension ext =
        new MaestroParamExtension(
            executor,
            stepInstanceDao,
            env,
            allStepOutputData,
            signalHandler,
            instanceWrapper,
            objectMapper);
    repos.set(ext);
  }

  /** Clear the current param extension. */
  public void clear() {
    repos.remove();
  }

  /** Get the current param extension. */
  public Extension get() {
    return repos.get();
  }

  /** Initialize the ExtensionRepo. */
  void initialize() {
    LOG.info("Initializing ExtensionRepo within Spring boot...");
    SelUtilFunc.register("toJson", this::toJsonExtFunction);
    executor = Executors.newFixedThreadPool(THREAD_NUM);
    ((ThreadPoolExecutor) executor).prestartAllCoreThreads();
  }

  /** Gracefully shutdown the ExtensionRepo. */
  @SuppressWarnings({"PMD.NullAssignment"})
  void shutdown() {
    LOG.info("Shutdown ExtensionRepo within Spring boot...");
    executor.shutdown();
    executor = null;
  }

  // Add a SEL function to convert the input object to a JSON string. If there are more, will
  // refactor them to a class.
  private String toJsonExtFunction(Object[] value) {
    Checks.checkTrue(
        value != null && value.length == 1, "toJson function requires exactly one argument");
    try {
      return objectMapper.writeValueAsString(value[0]);
    } catch (JsonProcessingException e) {
      throw new MaestroUnprocessableEntityException(
          "Failed to write an object to json string due to [%s]", e.getMessage());
    }
  }
}
