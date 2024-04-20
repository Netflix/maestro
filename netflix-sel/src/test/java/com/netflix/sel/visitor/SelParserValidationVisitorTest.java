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

import static org.junit.Assert.*;

import com.netflix.sel.ast.ASTExecute;
import com.netflix.sel.ast.ParseException;
import com.netflix.sel.ast.SelParser;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SelParserValidationVisitorTest {

  private SelParserValidationVisitor visitor = new SelParserValidationVisitor();
  private SelParser selParser = new SelParser(new ByteArrayInputStream("".getBytes()));

  private Map<String, Boolean> run(String expr) throws Exception {
    selParser.ReInit(new ByteArrayInputStream(expr.getBytes()));
    ASTExecute n = selParser.Execute();
    Map<String, Boolean> res = new HashMap<>();
    n.jjtAccept(visitor, res);
    return res;
  }

  @Test
  public void testGetLocalVarName() throws Exception {
    Map<String, Boolean> res = run("int x = 1;");
    assertEquals("{x=false}", res.toString());
  }

  @Test
  public void testGetParamVarName() throws Exception {
    Map<String, Boolean> res = run("x.length;");
    assertEquals("{x=true}", res.toString());
  }

  @Test
  public void testGetParamsVarName() throws Exception {
    Map<String, Boolean> res = run("params.get('foo');");
    assertEquals("{params=true}", res.toString());
  }

  @Test(expected = ParseException.class)
  public void testInvalidExpr() throws Exception {
    run("param");
  }
}
