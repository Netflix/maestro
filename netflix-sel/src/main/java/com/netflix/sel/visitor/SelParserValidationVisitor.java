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
package com.netflix.sel.visitor;

import com.netflix.sel.ast.ASTName;
import com.netflix.sel.ast.ASTVariableDeclaratorId;
import com.netflix.sel.ast.SelParserDefaultVisitor;
import com.netflix.sel.ast.SimpleNode;
import java.util.Map;

/**
 * Visitor to validate an input expression It returns a list of parameters required by a valid
 * input.
 */
public final class SelParserValidationVisitor extends SelParserDefaultVisitor {
  @Override
  public Object defaultVisit(SimpleNode node, Object data) {
    node.childrenAccept(this, data);
    return SelResult.NONE;
  }

  @Override
  public Object visit(ASTName node, Object data) {
    @SuppressWarnings("unchecked")
    Map<String, Boolean> map = (Map<String, Boolean>) data;
    String var = (String) node.jjtGetValue();

    if (!map.containsKey(var)) {
      map.put(var, Boolean.TRUE);
    }
    return SelResult.NONE;
  }

  @Override
  public Object visit(ASTVariableDeclaratorId node, Object data) {
    @SuppressWarnings("unchecked")
    Map<String, Boolean> map = (Map<String, Boolean>) data;
    String var = (String) node.jjtGetValue();

    if (!map.containsKey(var)) {
      map.put(var, Boolean.FALSE);
    }
    return SelResult.NONE;
  }
}
