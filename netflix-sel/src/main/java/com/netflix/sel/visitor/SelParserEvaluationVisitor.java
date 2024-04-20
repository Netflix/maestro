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

import static com.netflix.sel.ast.SelParserTreeConstants.JJTEXPRESSION;
import static com.netflix.sel.ast.SelParserTreeConstants.JJTFORINIT;
import static com.netflix.sel.ast.SelParserTreeConstants.JJTFORUPDATE;
import static com.netflix.sel.ast.SelParserTreeConstants.JJTLITERAL;
import static com.netflix.sel.ast.SelParserTreeConstants.JJTMETHOD;
import static com.netflix.sel.ast.SelParserTreeConstants.JJTSTATEMENT;
import static com.netflix.sel.type.SelTypeUtil.checkTypeMatch;
import static com.netflix.sel.type.SelTypeUtil.fromStringToSelType;

import com.netflix.sel.ast.ASTAllocationExpression;
import com.netflix.sel.ast.ASTArgs;
import com.netflix.sel.ast.ASTArrayDims;
import com.netflix.sel.ast.ASTArrayIdx;
import com.netflix.sel.ast.ASTArrayInitializer;
import com.netflix.sel.ast.ASTAssignment;
import com.netflix.sel.ast.ASTAssignmentOperator;
import com.netflix.sel.ast.ASTBinaryExpr;
import com.netflix.sel.ast.ASTBlock;
import com.netflix.sel.ast.ASTBreakStatement;
import com.netflix.sel.ast.ASTClassType;
import com.netflix.sel.ast.ASTContinueStatement;
import com.netflix.sel.ast.ASTExecute;
import com.netflix.sel.ast.ASTExpression;
import com.netflix.sel.ast.ASTForInit;
import com.netflix.sel.ast.ASTForStatement;
import com.netflix.sel.ast.ASTForUpdate;
import com.netflix.sel.ast.ASTIfStatement;
import com.netflix.sel.ast.ASTImportDeclaration;
import com.netflix.sel.ast.ASTLiteral;
import com.netflix.sel.ast.ASTLocalVariableDeclaration;
import com.netflix.sel.ast.ASTMethod;
import com.netflix.sel.ast.ASTName;
import com.netflix.sel.ast.ASTParams;
import com.netflix.sel.ast.ASTPrimaryExpression;
import com.netflix.sel.ast.ASTPrimarySuffix;
import com.netflix.sel.ast.ASTPrimitiveType;
import com.netflix.sel.ast.ASTReturnStatement;
import com.netflix.sel.ast.ASTStatement;
import com.netflix.sel.ast.ASTTernary;
import com.netflix.sel.ast.ASTThrowStatement;
import com.netflix.sel.ast.ASTType;
import com.netflix.sel.ast.ASTUnary;
import com.netflix.sel.ast.ASTVariableDeclarator;
import com.netflix.sel.ast.ASTVariableDeclaratorId;
import com.netflix.sel.ast.ASTWhileStatement;
import com.netflix.sel.ast.SelParserVisitor;
import com.netflix.sel.ast.SimpleNode;
import com.netflix.sel.ast.Token;
import com.netflix.sel.ext.Extension;
import com.netflix.sel.type.SelArray;
import com.netflix.sel.type.SelBoolean;
import com.netflix.sel.type.SelError;
import com.netflix.sel.type.SelLong;
import com.netflix.sel.type.SelParams;
import com.netflix.sel.type.SelString;
import com.netflix.sel.type.SelType;
import com.netflix.sel.type.SelTypes;
import java.util.Map;

/**
 * Evaluation visitor to traverse AST and compute the expression
 *
 * <p>todo: support terminating execution in the middle if timeout
 */
public final class SelParserEvaluationVisitor implements SelParserVisitor {
  private static final SelBaseNode TRUE_NODE = new ASTLiteral(JJTLITERAL);

  static {
    TRUE_NODE.jjtSetValue(SelBoolean.of(true));
  }

  private final int loopLimit;
  private final int arrayLimit;
  private final long visitLimit; // limit for the number of visited nodes in AST.
  private final SelVisitorState state;

  public SelParserEvaluationVisitor(
      int stackLimit, int loopLimit, int arrayLimit, long visitLimit) {
    this.loopLimit = loopLimit;
    this.arrayLimit = arrayLimit;
    this.visitLimit = visitLimit;
    this.state = new SelVisitorState(stackLimit);
  }

  public void clearState() {
    this.state.clear();
  }

  public void resetWithInput(Map<String, Object> input, Extension ext) {
    this.state.resetWithInput(input, ext);
  }

  @Override
  public Object visit(SimpleNode node, Object data) {
    throw new IllegalStateException("Should not visit the node: " + node);
  }

  @Override
  public Object visit(ASTExecute node, Object data) {
    visitAndCheckLimit();
    SelResult res = SelResult.NONE;
    for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
      if (res == SelResult.DATA) {
        state.pop();
      }
      res = (SelResult) node.jjtGetChild(i).jjtAccept(this, data);
      if (res == SelResult.RETURN) {
        res = SelResult.DATA;
        break;
      }
    }

    if (res == SelResult.DATA) {
      SelType ret = state.pop();
      if (!state.isStackEmpty()) {
        throw new RuntimeException(
            "Invalid expression (stack is not empty): " + getSubTreeImage(node));
      }
      return ret;
    } else if (res == SelResult.NONE) {
      return SelType.VOID;
    } else {
      throw new RuntimeException(
          "Invalid expression : " + getSubTreeImage(node) + " with wrong return type: " + res);
    }
  }

  @Override
  public Object visit(ASTImportDeclaration node, Object data) {
    visitAndCheckLimit();
    return SelResult.NONE; // no-op node
  }

  @Override
  public Object visit(ASTVariableDeclarator node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    String varName = (String) node.jjtGetChild(0).jjtGetValue();

    SelType rhs;
    SelTypes type;
    if (node.jjtGetNumChildren() == 1) {
      type = (SelTypes) state.readWithOffset(0);
      rhs = type.newSelTypeObj();
    } else if (node.jjtGetNumChildren() == 2) {
      rhs = state.pop();
      type = (SelTypes) state.readWithOffset(0);
      checkTypeMatch(type, rhs.type());
    } else {
      throw new IllegalArgumentException(
          "Invalid local variable declaration: " + getSubTreeImage(node));
    }

    state.put(varName, rhs);
    return SelResult.NONE;
  }

  @Override
  public Object visit(ASTVariableDeclaratorId node, Object data) {
    visitAndCheckLimit();
    return SelResult.NONE; // no-op
  }

  @Override
  public Object visit(ASTArrayInitializer node, Object data) {
    visitAndCheckLimit();
    if (node.jjtGetNumChildren() >= arrayLimit) {
      throw new IllegalArgumentException(
          "New array size is over the limit: "
              + arrayLimit
              + " vs length: "
              + node.jjtGetNumChildren());
    }
    node.childrenAccept(this, data);

    SelTypes type = (SelTypes) state.readWithOffset(node.jjtGetNumChildren());
    SelArray array = type.newSelTypeObjArray(node.jjtGetNumChildren());

    for (int i = node.jjtGetNumChildren() - 1; i >= 0; --i) {
      array.set(i, state.pop());
    }
    state.pop();
    state.push(array);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTType node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    SelTypes type = (SelTypes) state.pop();
    if (node.jjtGetValue() == Boolean.TRUE) {
      switch (type) {
        case STRING:
          type = SelTypes.STRING_ARRAY;
          break;
        case LONG:
          type = SelTypes.LONG_ARRAY;
          break;
        case DOUBLE:
          type = SelTypes.DOUBLE_ARRAY;
          break;
        case BOOLEAN:
          type = SelTypes.BOOLEAN_ARRAY;
          break;
        default:
          throw new IllegalArgumentException("Cannot create an array of " + type);
      }
    }
    state.push(type);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTPrimitiveType node, Object data) {
    visitAndCheckLimit();
    String clazz = (String) node.jjtGetValue();
    SelTypes type = fromStringToSelType(clazz);
    state.push(type);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTClassType node, Object data) {
    visitAndCheckLimit();
    String clazz = (String) node.jjtGetValue();
    SelTypes type = fromStringToSelType(clazz);
    state.push(type);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTName node, Object data) {
    visitAndCheckLimit();
    String key = (String) node.jjtGetValue();
    state.push(state.get(key));
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTExpression node, Object data) {
    visitAndCheckLimit();
    data = node.childrenAccept(this, data);
    checkWithData(data, node, "expression");
    return data;
  }

  @Override
  public Object visit(ASTAssignment node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);

    String varId = (String) node.jjtGetChild(0).jjtGetValue();
    SelOp op = (SelOp) node.jjtGetChild(node.jjtGetNumChildren() - 2).jjtGetValue();
    SelType rhs = state.pop();

    SelType lhs;
    if (node.jjtGetValue() == Boolean.TRUE) {
      int idx = ((SelLong) state.pop()).intVal();
      SelArray lhsArray = (SelArray) state.get(varId);
      lhs = lhsArray.get(idx);
    } else {
      if (op == SelOp.ASSIGN) {
        state.createIfMissing(varId, rhs.type());
      }
      lhs = state.get(varId);
    }

    lhs.assignOps(op, rhs);
    state.push(lhs);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTAssignmentOperator node, Object data) {
    visitAndCheckLimit();
    return SelResult.NONE; // no-op
  }

  @Override
  public Object visit(ASTTernary node, Object data) {
    visitAndCheckLimit();
    return processConditionBlock(node, data, true);
  }

  @Override
  public Object visit(ASTBinaryExpr node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    SelType rhs = state.pop();
    SelType lhs = state.pop();
    SelType ret = lhs.binaryOps((SelOp) node.jjtGetValue(), rhs);
    state.push(ret);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTUnary node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    SelOp op = (SelOp) node.jjtGetValue();
    SelType rhs = state.pop();
    SelType ret = rhs.binaryOps(op, rhs);
    state.push(ret);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTPrimaryExpression node, Object data) {
    visitAndCheckLimit();
    data = node.childrenAccept(this, data);
    checkWithData(data, node, "primary expression");
    return data;
  }

  @Override
  public Object visit(ASTArrayIdx node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    SelType idx = state.pop();
    SelType obj;
    switch (idx.type()) {
      case LONG:
        SelArray array = (SelArray) state.pop();
        obj = array.get(((SelLong) idx).intVal());
        break;
      case STRING:
        SelParams params = (SelParams) state.pop();
        obj = params.field((SelString) idx);
        break;
      default:
        throw new IllegalArgumentException("Invalid index type: " + idx.type());
    }
    state.push(obj);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTPrimarySuffix node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    if (node.jjtGetNumChildren() == 1 && node.jjtGetChild(0).getId() == JJTMETHOD) {
      SelString field = (SelString) state.pop();
      SelType obj = state.pop();
      SelType ret = obj.field(field);
      state.push(ret);
    }
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTLiteral node, Object data) {
    visitAndCheckLimit();
    state.push((SelType) node.jjtGetValue());
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTMethod node, Object data) {
    visitAndCheckLimit();
    state.push((SelType) node.jjtGetValue());
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTParams node, Object data) {
    visitAndCheckLimit();
    SelType[] params = visitAndCollectResults(node, data);

    SelString method = (SelString) state.pop();
    SelType obj = state.pop();
    String methodName = method.getInternalVal();
    SelType ret = obj.call(methodName, params);
    if (ret == SelType.VOID) {
      throw new UnsupportedOperationException(
          "void return method " + methodName + " is not supported yet");
    }
    state.push(ret);
    return SelResult.DATA;
  }

  private SelType[] visitAndCollectResults(SelBaseNode node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    SelType[] results = new SelType[node.jjtGetNumChildren()];
    for (int i = node.jjtGetNumChildren() - 1; i >= 0; --i) {
      results[i] = state.pop();
    }
    return results;
  }

  @Override
  public Object visit(ASTArgs node, Object data) {
    visitAndCheckLimit();
    SelType[] args = visitAndCollectResults(node, data);
    SelType type = state.pop();
    SelType newObj = type.call(SelTypes.CONSTRUCTOR, args);
    state.push(newObj);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTAllocationExpression node, Object data) {
    visitAndCheckLimit();
    return node.childrenAccept(this, data);
  }

  @Override
  public Object visit(ASTArrayDims node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    SelLong len = (SelLong) state.pop();
    if (len.intVal() >= arrayLimit) {
      throw new IllegalArgumentException(
          "New array size is over the limit: " + arrayLimit + " vs length: " + len);
    }
    SelTypes type = (SelTypes) state.pop();
    SelArray array = type.newSelTypeObjArray(len.intVal());
    state.push(array);
    return SelResult.DATA;
  }

  @Override
  public Object visit(ASTStatement node, Object data) {
    visitAndCheckLimit();
    if (!state.isStackEmpty()) {
      state.pop(); // remove the last statement result if not empty
    }

    return node.childrenAccept(this, data);
  }

  public Object visit(ASTBlock node, Object data) {
    visitAndCheckLimit();
    return node.childrenAccept(this, data); // no-op
  }

  @Override
  public Object visit(ASTLocalVariableDeclaration node, Object data) {
    visitAndCheckLimit();
    node.childrenAccept(this, data);
    state.pop();
    return SelResult.NONE;
  }

  @Override
  public Object visit(ASTIfStatement node, Object data) {
    visitAndCheckLimit();
    SelResult res = processConditionBlock(node, data, node.jjtGetNumChildren() == 3);
    switch (res) {
      case DATA:
        state.pop();
        return SelResult.NONE;
      case NONE:
        return SelResult.NONE;
      case RETURN:
        return SelResult.RETURN;
      case BREAK:
        return SelResult.BREAK;
      case CONTINUE:
        return SelResult.CONTINUE;
      default:
        throw new IllegalStateException(
            "Invalid if statement: " + getSubTreeImage(node) + " with a result: " + res);
    }
  }

  @Override
  public Object visit(ASTWhileStatement node, Object data) {
    visitAndCheckLimit();
    return processLoopBlock(node, data, 0, 1, -1);
  }

  @Override
  public Object visit(ASTForStatement node, Object data) {
    visitAndCheckLimit();
    if (node.jjtGetNumChildren() != 4) { // fill the ForLoop if missing node
      SelBaseNode initNode = null, exprNode = null, updateNode = null, loopNode = null;
      for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
        switch (node.jjtGetChild(i).getId()) {
          case JJTFORINIT:
            initNode = node.jjtGetChild(i);
            break;
          case JJTEXPRESSION:
            exprNode = node.jjtGetChild(i);
            break;
          case JJTFORUPDATE:
            updateNode = node.jjtGetChild(i);
            break;
          case JJTSTATEMENT:
            loopNode = node.jjtGetChild(i);
            break;
          default:
            throw new RuntimeException(
                "Invalid ForLoop: "
                    + getSubTreeImage(node)
                    + " with invalid child node: "
                    + node.jjtGetChild(i));
        }
      }
      initNode = initNode == null ? TRUE_NODE : initNode;
      exprNode = exprNode == null ? TRUE_NODE : exprNode;
      updateNode = updateNode == null ? TRUE_NODE : updateNode;
      node.jjtAddChild(loopNode, 3);
      node.jjtAddChild(updateNode, 2);
      node.jjtAddChild(exprNode, 1);
      node.jjtAddChild(initNode, 0);
    }

    node.jjtGetChild(0).jjtAccept(this, data); // visit init node
    return processLoopBlock(node, data, 1, 3, 2);
  }

  @Override
  public Object visit(ASTForInit node, Object data) {
    visitAndCheckLimit();
    data = node.childrenAccept(this, data);
    if (data == SelResult.DATA) {
      state.pop();
    }
    return SelResult.NONE;
  }

  @Override
  public Object visit(ASTForUpdate node, Object data) {
    visitAndCheckLimit();
    data = node.childrenAccept(this, data);
    checkWithData(data, node, "for-update");
    state.pop();
    return SelResult.NONE;
  }

  @Override
  public Object visit(ASTBreakStatement node, Object data) {
    visitAndCheckLimit();
    return SelResult.BREAK;
  }

  @Override
  public Object visit(ASTContinueStatement node, Object data) {
    visitAndCheckLimit();
    return SelResult.CONTINUE;
  }

  @Override
  public Object visit(ASTReturnStatement node, Object data) {
    visitAndCheckLimit();
    node.jjtGetChild(0).jjtAccept(this, data);
    return SelResult.RETURN;
  }

  @Override
  public Object visit(ASTThrowStatement node, Object data) {
    visitAndCheckLimit();
    node.jjtGetChild(0).jjtAccept(this, data);
    SelType res = state.pop();
    if (res.type() == SelTypes.STRING) {
      state.push(SelError.of(((SelString) res).getInternalVal()));
      return SelResult.RETURN;
    }
    throw new IllegalArgumentException(
        "Can only throw a string error message. Invalid thrown object " + res);
  }

  private String getSubTreeImage(SimpleNode node) {
    StringBuilder sb = new StringBuilder();
    Token t = node.jjtGetFirstToken();
    while (t != node.jjtGetLastToken()) {
      sb.append(t.image);
      sb.append(' ');
      t = t.next;
    }
    sb.append(t.image);
    return sb.toString();
  }

  private void checkWithData(Object data, SelBaseNode node, String description) {
    if (data != SelResult.DATA) {
      throw new RuntimeException(
          "Error, the "
              + description
              + " { "
              + getSubTreeImage(node)
              + " } returns invalid result "
              + data);
    }
  }

  private SelResult processConditionBlock(
      SelBaseNode node, Object data, boolean hasFalseStatement) {
    node.jjtGetChild(0).jjtAccept(this, data);
    SelBoolean condResult = (SelBoolean) state.pop();
    if (condResult.booleanVal()) {
      return (SelResult) node.jjtGetChild(1).jjtAccept(this, data); // true
    } else if (hasFalseStatement) {
      return (SelResult) node.jjtGetChild(2).jjtAccept(this, data); // false
    } else {
      return SelResult.NONE;
    }
  }

  private SelResult processLoopBlock(
      SelBaseNode node, Object data, int condNodeIdx, int loopNodeIdx, int updateNodeIdx) {
    int loopCnt = 0;
    for (node.jjtGetChild(condNodeIdx).jjtAccept(this, data);
        ((SelBoolean) state.pop()).booleanVal();
        node.jjtGetChild(condNodeIdx).jjtAccept(this, data)) {
      SelResult res = (SelResult) node.jjtGetChild(loopNodeIdx).jjtAccept(this, data);
      switch (res) {
        case DATA:
          state.pop();
          break;
        case RETURN:
          return SelResult.RETURN;
        case BREAK:
          return SelResult.NONE;
        case NONE:
        case CONTINUE:
          break;
        default:
          throw new IllegalStateException(
              "Invalid loop: " + getSubTreeImage(node) + " with result " + res);
      }
      if (updateNodeIdx >= 0) {
        node.jjtGetChild(updateNodeIdx).jjtAccept(this, data);
      }
      loopCnt++;
      if (loopCnt >= loopLimit) {
        throw new IllegalStateException(
            "Loop execution aborted " + "as the iteration is over the loop limit " + loopLimit);
      }
    }
    return SelResult.NONE;
  }

  private void visitAndCheckLimit() {
    state.visited++;
    if (state.visited >= visitLimit) {
      throw new IllegalStateException(
          "SEL evaluation aborted "
              + "as it takes too many operations over the limit "
              + visitLimit);
    }
  }
}
