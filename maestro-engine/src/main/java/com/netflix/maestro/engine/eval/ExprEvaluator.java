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

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.properties.SelProperties;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroInvalidExpressionException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.sel.SelEvaluator;
import com.netflix.sel.ext.Extension;
import com.netflix.sel.type.SelType;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/** SEL expression evaluator wrapper. */
@Slf4j
public class ExprEvaluator {
  private static final String SEMICOLON = ";";

  private final SelProperties properties;
  private final MaestroParamExtensionRepo extensionRepo;
  private SelEvaluator evaluator;

  /** Construct ExprEvaluator. */
  public ExprEvaluator(SelProperties properties, @Nullable MaestroParamExtensionRepo repo) {
    this.properties = properties;
    this.extensionRepo = repo;
  }

  /** start ExprEvaluator after construction. */
  public void postConstruct() {
    extensionRepo.initialize();
    if (evaluator == null) {
      LOG.info("Constructing ExprEvaluator within Spring boot...");
      evaluator =
          new SelEvaluator(
              properties.getThreadNum(),
              properties.getTimeoutMillis(),
              properties.getStackLimit(),
              properties.getLoopLimit(),
              properties.getArrayLimit(),
              properties.getLengthLimit(),
              properties.getVisitLimit(),
              properties.getMemoryLimit());
    }
  }

  /** stop ExprEvaluator before destroying. */
  @SuppressWarnings({"PMD.NullAssignment"})
  public void preDestroy() {
    if (evaluator != null) {
      LOG.info("Destroying ExprEvaluator within Spring boot...");
      evaluator.stop();
      evaluator = null;
    }
    if (extensionRepo != null) {
      extensionRepo.shutdown();
    }
  }

  /**
   * evaluate a SEL expression.
   *
   * @param expr SEL expression in a string
   * @param params Map of input parameter names and their values
   * @return evaluated result
   */
  public Object eval(String expr, Map<String, Object> params) {
    try {
      Extension ext = extensionRepo == null ? null : extensionRepo.get();
      SelType result = evaluator.evaluate(sanitize(expr), params, ext);

      switch (result.type()) {
        case STRING:
        case LONG:
        case DOUBLE:
        case BOOLEAN:
          return result.getInternalVal();
        case STRING_ARRAY:
        case LONG_ARRAY:
        case DOUBLE_ARRAY:
        case BOOLEAN_ARRAY:
        case MAP:
          return result.unbox();
        case ERROR:
          throw new MaestroInvalidExpressionException(
              "Expression throws an error [%s] for expr=[%s]", result, expr);
        default:
          throw new MaestroInvalidExpressionException(
              "Invalid return type [%s] for expr=[%s]", result.type(), expr);
      }
    } catch (MaestroRuntimeException me) {
      throw me;
    } catch (ExecutionException ee) {
      throw new MaestroInvalidExpressionException(
          ee, "Expression evaluation throws an exception for expr=[%s]", expr);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e, "Expression evaluation is failed with an exception for expr=[%s]", expr);
    }
  }

  /**
   * validates and returns referenced param names of a SEL expression.
   *
   * @param expr SEL expression in a string.
   * @return a set of parameter names referenced by the given SEL expression.
   */
  public Set<String> validate(String expr) {
    try {
      return evaluator.validate(sanitize(expr));
    } catch (ExecutionException ee) {
      throw new MaestroInvalidExpressionException(
          ee, "Expression validator throws an exception for expr=[%s]", expr);
    } catch (Exception e) {
      throw new MaestroInternalError(
          e, "Expression validation is failed with an exception for expr=[%s]" + expr);
    }
  }

  // SEL requires the statement ending with a semicolon and this is a temporary solution
  // to remove this constraint.
  private String sanitize(String expression) {
    String expr = expression.trim();
    if (expr.isEmpty()
        || expr.endsWith(SEMICOLON)
        || (!expr.startsWith("{\"") && expr.endsWith("}"))) {
      return expr;
    } else {
      return expr + SEMICOLON;
    }
  }
}
