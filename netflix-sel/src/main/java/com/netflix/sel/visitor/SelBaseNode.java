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

import com.netflix.sel.ast.SelParser;
import com.netflix.sel.ast.SelParserTreeConstants;
import com.netflix.sel.ast.SelParserVisitor;
import com.netflix.sel.ast.SimpleNode;

/** Customized base node for AST */
public abstract class SelBaseNode extends SimpleNode {
  public SelBaseNode(int id) {
    super(id);
  }

  public SelBaseNode(SelParser p, int id) {
    super(p, id);
  }

  @Override
  public SelBaseNode jjtGetChild(int i) {
    return (SelBaseNode) children[i];
  }

  @Override
  public SelResult childrenAccept(SelParserVisitor visitor, Object data) {
    SelResult res = SelResult.NONE;
    if (children != null) {
      for (int i = 0; i < children.length; ++i) {
        res = (SelResult) children[i].jjtAccept(visitor, data);
        switch (res) {
          case BREAK:
            return SelResult.BREAK;
          case CONTINUE:
            return SelResult.CONTINUE;
          case RETURN:
            return SelResult.RETURN;
        }
      }
    }
    return res;
  }

  @Override
  public String toString() {
    return SelParserTreeConstants.jjtNodeName[id] + (value == null ? "" : ": " + value);
  }
}
