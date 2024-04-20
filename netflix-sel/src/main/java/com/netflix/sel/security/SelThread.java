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

import com.netflix.sel.ast.ASTExecute;
import com.netflix.sel.ast.SelParser;
import com.netflix.sel.ast.SelParserVisitor;
import com.netflix.sel.ext.Extension;
import com.netflix.sel.type.SelType;
import com.netflix.sel.visitor.SelParserEvaluationVisitor;
import com.netflix.sel.visitor.SelParserValidationVisitor;
import java.io.ByteArrayInputStream;
import java.security.AccessControlContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Thread to execute SEL with access control enabled */
public final class SelThread extends Thread {
  private static final ThreadGroup SEL_THREAD_GROUP = new ThreadGroup("SelThreadGroup");

  private final AccessControlContext acc = SelAccessController.INSTANCE.accessControlContext();
  private final ClassLoader classLoader = SelClassLoader.INSTANCE;

  private final SelParserEvaluationVisitor selEvaluator;
  private final SelParser selParser;
  private final SelParserVisitor validator;
  private final int lengthLimit;

  SelThread(
      String name,
      Runnable target,
      int stackLimit,
      int loopLimit,
      int arrayLimit,
      int lengthLimit,
      long visitLimit) {
    super(SEL_THREAD_GROUP, target, name);
    this.selParser = new SelParser(new ByteArrayInputStream("".getBytes()));
    this.selEvaluator =
        new SelParserEvaluationVisitor(stackLimit, loopLimit, arrayLimit, visitLimit);
    this.validator = new SelParserValidationVisitor();
    this.lengthLimit = lengthLimit;
  }

  private void checkExprLength(String expr) {
    if (expr.length() >= lengthLimit) {
      throw new IllegalArgumentException(
          "Expression length is over the limit: " + lengthLimit + " vs length: " + expr.length());
    }
  }

  public SelType evaluate(String expr, Map<String, Object> varsMap, Extension ext)
      throws Exception {
    checkExprLength(expr);
    selParser.ReInit(new ByteArrayInputStream(expr.getBytes()));
    ASTExecute n = selParser.Execute();
    try {
      selEvaluator.resetWithInput(varsMap, ext);
      return (SelType) n.jjtAccept(selEvaluator, null);
    } finally {
      selEvaluator.clearState();
    }
  }

  public Set<String> validate(String expr, Set<String> whitelistVars) throws Exception {
    checkExprLength(expr);
    selParser.ReInit(new ByteArrayInputStream(expr.getBytes()));
    ASTExecute n = selParser.Execute();
    Map<String, Boolean> vars = new HashMap<>();
    n.jjtAccept(validator, vars);

    Set<String> res = new HashSet<>();
    for (Map.Entry<String, Boolean> entry : vars.entrySet()) {
      if (entry.getValue() && !whitelistVars.contains(entry.getKey())) {
        res.add(entry.getKey());
      }
    }
    return res;
  }

  @Override
  public void run() {
    SecurityManager sm = System.getSecurityManager();
    if (!(sm instanceof SelSecurityManager)) {
      throw new IllegalStateException("Invalid security manager: " + sm);
    }
    Thread.currentThread().setContextClassLoader(this.classLoader);
    ((SelSecurityManager) sm).setAccessControl(this.acc);
    super.run();
  }
}
