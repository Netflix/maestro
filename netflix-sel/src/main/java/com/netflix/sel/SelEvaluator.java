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
package com.netflix.sel;

import com.netflix.sel.ext.Extension;
import com.netflix.sel.security.SelSecurityManager;
import com.netflix.sel.security.SelThread;
import com.netflix.sel.security.SelThreadFactory;
import com.netflix.sel.type.SelType;
import com.netflix.sel.type.SelTypeUtil;
import com.netflix.sel.util.MemoryCounter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Multi-thread SEL evaluator with access control enabled */
public final class SelEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(SelEvaluator.class);

  private final ExecutorService executor;
  private final Set<String> whitelistVars;
  private final int timeout;

  public SelEvaluator(
      int threadNum,
      int timeout,
      int stackLimit,
      int loopLimit,
      int arrayLimit,
      int lengthLimit,
      long visitLimit,
      long sizeLimit) {
    MemoryCounter.setMemoryLimit(sizeLimit);
    this.executor =
        Executors.newFixedThreadPool(
            threadNum,
            new SelThreadFactory(stackLimit, loopLimit, arrayLimit, lengthLimit, visitLimit));
    this.whitelistVars = new HashSet<>(SelTypeUtil.STATIC_OBJECTS.keySet());
    this.timeout = timeout;
    start();
    warmUp();
  }

  public SelType evaluate(String expr, Map<String, Object> varsMap, Extension ext)
      throws Exception {
    LOG.debug("Expression to evaluate is: " + expr);
    Future<SelType> f =
        executor.submit(() -> ((SelThread) Thread.currentThread()).evaluate(expr, varsMap, ext));
    SelType res = f.get(timeout, TimeUnit.MILLISECONDS);
    LOG.debug(String.format("Result (type: %s) is: %s", res.type(), res));
    return res;
  }

  public SelType evaluate(String expr, Map<String, Object> varsMap) throws Exception {
    return evaluate(expr, varsMap, null);
  }

  public Set<String> validate(String expr) throws Exception {
    LOG.debug("Expression to validate is: " + expr);
    Future<Set<String>> f =
        executor.submit(() -> ((SelThread) Thread.currentThread()).validate(expr, whitelistVars));
    Set<String> res = f.get(timeout, TimeUnit.MILLISECONDS);
    LOG.debug("Found expr variables: " + res);
    return res;
  }

  private void start() {
    LOG.info("Start SEL Evaluator ...");
    if (System.getSecurityManager() != null) {
      throw new IllegalStateException("ERROR: security manager has already been set.");
    }
    System.setSecurityManager(new SelSecurityManager());
    LOG.info("Enable security manager for SEL threads.");
  }

  private void warmUp() {
    ((ThreadPoolExecutor) this.executor).prestartCoreThread();
  }

  public void stop() {
    LOG.info("Shutdown SEL Evaluator ...");
    executor.shutdown();
    try {
      executor.awaitTermination(3000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException iex) {
      throw new RuntimeException("failed to shutdown executor", iex);
    }
    System.setSecurityManager(null);
    LOG.info("Shutdown SEL Evaluator and reset security manager. Bye.");
  }
}
