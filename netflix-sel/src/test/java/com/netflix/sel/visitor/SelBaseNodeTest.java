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

import static com.netflix.sel.ast.SelParserTreeConstants.JJTEXECUTE;
import static org.junit.Assert.*;

import com.netflix.sel.ast.ASTExecute;
import com.netflix.sel.ast.Node;
import com.netflix.sel.ast.SelParserVisitor;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class SelBaseNodeTest {

  private ASTExecute root;
  private Node breakNode;
  private Node continueNode;
  private Node returnNode;
  private Node dataNode;
  private Node noneNode;

  private int[] visited = new int[5];

  @Before
  public void setUp() throws Exception {
    Arrays.fill(visited, 0);
    root = new ASTExecute(JJTEXECUTE);
    breakNode =
        new SelBaseNode(-1) {
          @Override
          public Object jjtAccept(SelParserVisitor visitor, Object data) {
            visited[0]++;
            return SelResult.BREAK;
          }
        };
    continueNode =
        new SelBaseNode(-2) {
          @Override
          public Object jjtAccept(SelParserVisitor visitor, Object data) {
            visited[1]++;
            return SelResult.CONTINUE;
          }
        };
    returnNode =
        new SelBaseNode(-3) {
          @Override
          public Object jjtAccept(SelParserVisitor visitor, Object data) {
            visited[2]++;
            return SelResult.RETURN;
          }
        };
    dataNode =
        new SelBaseNode(-4) {
          @Override
          public Object jjtAccept(SelParserVisitor visitor, Object data) {
            visited[3]++;
            return SelResult.DATA;
          }
        };
    noneNode =
        new SelBaseNode(-5) {
          @Override
          public Object jjtAccept(SelParserVisitor visitor, Object data) {
            visited[4]++;
            return SelResult.NONE;
          }
        };
  }

  @Test
  public void testVisitedBreakNode() {
    root.jjtAddChild(breakNode, 2);
    root.jjtAddChild(breakNode, 1);
    root.jjtAddChild(breakNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.BREAK, res);
    assertArrayEquals(new int[] {1, 0, 0, 0, 0}, visited);
  }

  @Test
  public void testVisitedContinueNode() {
    root.jjtAddChild(continueNode, 2);
    root.jjtAddChild(continueNode, 1);
    root.jjtAddChild(continueNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.CONTINUE, res);
    assertArrayEquals(new int[] {0, 1, 0, 0, 0}, visited);
  }

  @Test
  public void testVisitedReturnNode() {
    root.jjtAddChild(returnNode, 2);
    root.jjtAddChild(returnNode, 1);
    root.jjtAddChild(returnNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.RETURN, res);
    assertArrayEquals(new int[] {0, 0, 1, 0, 0}, visited);
  }

  @Test
  public void testVisitedAllDataNode() {
    root.jjtAddChild(dataNode, 2);
    root.jjtAddChild(dataNode, 1);
    root.jjtAddChild(dataNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.DATA, res);
    assertArrayEquals(new int[] {0, 0, 0, 3, 0}, visited);
  }

  @Test
  public void testVisitedSomeDataNodeAndBreak() {
    root.jjtAddChild(dataNode, 2);
    root.jjtAddChild(breakNode, 1);
    root.jjtAddChild(dataNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.BREAK, res);
    assertArrayEquals(new int[] {1, 0, 0, 1, 0}, visited);
  }

  @Test
  public void testVisitedSomeDataNodeAndContinue() {
    root.jjtAddChild(dataNode, 2);
    root.jjtAddChild(continueNode, 1);
    root.jjtAddChild(dataNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.CONTINUE, res);
    assertArrayEquals(new int[] {0, 1, 0, 1, 0}, visited);
  }

  @Test
  public void testVisitedSomeDataNodeAndReturn() {
    root.jjtAddChild(dataNode, 2);
    root.jjtAddChild(returnNode, 1);
    root.jjtAddChild(dataNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.RETURN, res);
    assertArrayEquals(new int[] {0, 0, 1, 1, 0}, visited);
  }

  @Test
  public void testVisitedNoneDataNode() {
    root.jjtAddChild(noneNode, 2);
    root.jjtAddChild(noneNode, 1);
    root.jjtAddChild(noneNode, 0);
    SelResult res = root.childrenAccept(null, null);
    assertEquals(SelResult.NONE, res);
    assertArrayEquals(new int[] {0, 0, 0, 0, 3}, visited);
  }

  @Test
  public void testToString() {
    assertEquals("Execute", root.toString());
  }

  @Test
  public void testToStringWithValue() {
    root.jjtSetValue("foo");
    assertEquals("Execute: foo", root.toString());
  }
}
