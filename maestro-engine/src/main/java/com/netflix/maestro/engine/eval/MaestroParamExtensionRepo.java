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
import com.netflix.maestro.engine.concurrency.EngineExecutors;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.properties.ThreadingProperties;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.utils.Checks;
import com.netflix.sel.ext.Extension;
import com.netflix.sel.type.SelUtilFunc;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * A repository to hold maestro param extensions for the param evaluation.
 *
 * <p>Owns the I/O-bound executor that backs {@link MaestroParamExtension}'s blocking JDBC and
 * signal lookups. The executor is constructed by {@link EngineExecutors} during {@link
 * #initialize()} according to the supplied {@link ThreadingProperties}, and is shut down on
 * {@link #shutdown()}.
 */
@Slf4j
public class MaestroParamExtensionRepo {
  private final ThreadLocal<Extension> repos = new ThreadLocal<>();
  private final MaestroStepInstanceDao stepInstanceDao;
  private final ObjectMapper objectMapper;
  private final String env;
  private final ThreadingProperties threadingProperties;
  private ExecutorService ioExecutor;

  /** Constructor with default threading properties (virtual threads). */
  public MaestroParamExtensionRepo(
      MaestroStepInstanceDao stepInstanceDao, String env, ObjectMapper objectMapper) {
    this(stepInstanceDao, env, objectMapper, new ThreadingProperties());
  }

  /**
   * Constructor.
   *
   * @param stepInstanceDao DAO used by the param extension for JDBC lookups
   * @param env execution environment identifier
   * @param objectMapper Jackson object mapper used to deserialize runtime summaries
   * @param threadingProperties controls the executor model (virtual vs. platform) and tuning
   */
  public MaestroParamExtensionRepo(
      MaestroStepInstanceDao stepInstanceDao,
      String env,
      ObjectMapper objectMapper,
      ThreadingProperties threadingProperties) {
    this.stepInstanceDao = stepInstanceDao;
    this.objectMapper = objectMapper;
    this.env = env;
    this.threadingProperties = threadingProperties;
  }

  /** Reset repo by creating a new param extension wrapper for the current thread. */
  public void reset(
      Map<String, Map<String, Object>> allStepOutputData,
      @Nullable SignalHandler signalHandler,
      InstanceWrapper instanceWrapper) {
    Extension ext =
        new MaestroParamExtension(
            ioExecutor,
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

  /** Initialize the ExtensionRepo and its I/O executor. */
  void initialize() {
    LOG.info("Initializing ExtensionRepo within Spring boot...");
    SelUtilFunc.register("toJson", this::toJsonExtFunction);
    ioExecutor = EngineExecutors.newIoExecutor(threadingProperties);
  }

  /** Gracefully shutdown the ExtensionRepo and its I/O executor. */
  void shutdown() {
    LOG.info("Shutdown ExtensionRepo within Spring boot...");
    ExecutorService toShutdown = ioExecutor;
    ioExecutor = null;
    if (toShutdown == null) {
      return;
    }
    toShutdown.shutdown();
    try {
      if (!toShutdown.awaitTermination(5, TimeUnit.SECONDS)) {
        toShutdown.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      toShutdown.shutdownNow();
    }
  }

  // Add a SEL function to convert the input object to a JSON string. If there are more, will
  // refactor them to a class.
  @SuppressWarnings("PMD.PreserveStackTrace")
  private String toJsonExtFunction(Object... value) {
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
