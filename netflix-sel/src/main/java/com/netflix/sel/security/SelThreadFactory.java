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
package com.netflix.sel.security;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** Thread factory to create SEL Thread with access control enabled */
public final class SelThreadFactory implements ThreadFactory {

  private final AtomicInteger threadNumber = new AtomicInteger(1);

  private final int stackLimit;
  private final int loopLimit;
  private final int arrayLimit;
  private final int lengthLimit;
  private final long visitLimit;

  public SelThreadFactory(
      int stackLimit, int loopLimit, int arrayLimit, int lengthLimit, long visitLimit) {
    this.stackLimit = stackLimit;
    this.loopLimit = loopLimit;
    this.arrayLimit = arrayLimit;
    this.lengthLimit = lengthLimit;
    this.visitLimit = visitLimit;
  }

  @Override
  public Thread newThread(Runnable r) {
    return new SelThread(
        "SelThread-" + threadNumber.getAndIncrement(),
        r,
        stackLimit,
        loopLimit,
        arrayLimit,
        lengthLimit,
        visitLimit);
  }
}
