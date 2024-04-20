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
import com.netflix.sel.type.SelType;
import com.netflix.sel.util.MemoryCounter;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelParserEvaluationVisitorTest {
  private final SelParser selParser = new SelParser(new ByteArrayInputStream("".getBytes()));

  private SelParserEvaluationVisitor visitor;

  private SelType run(String expr) throws Exception {
    selParser.ReInit(new ByteArrayInputStream(expr.getBytes()));
    ASTExecute n = selParser.Execute();
    Map<String, Object> varMap = new HashMap<>();
    varMap.put("foo", "bar");
    visitor.resetWithInput(varMap, null);
    return (SelType) n.jjtAccept(visitor, null);
  }

  @Before
  public void setUp() {
    MemoryCounter.setMemoryLimit(1024 * 1024);
    MemoryCounter.reset();
    visitor = new SelParserEvaluationVisitor(10, 10, 10, 1000);
  }

  @After
  public void tearDown() {
    MemoryCounter.setMemoryLimit(1024 * 1024);
    MemoryCounter.reset();
  }

  @Test
  public void testExpressions() throws Exception {
    SelType res = run("x=1+1;");
    assertEquals("LONG: 2", res.type() + ": " + res);
    res = run("int x = 1;");
    assertEquals("VOID: VOID", res.type() + ": " + res);
    res = run("int x = 1;");
    assertEquals("VOID: VOID", res.type() + ": " + res);
    res = run("x=1; for(i=2; i<10; i+=1) { x+=1; x+=1;} return x;");
    assertEquals("LONG: 17", res.type() + ": " + res);
    res = run("x=1; for(i=2; i<10; i+=1) { x+=1; if (x>10) continue; x+=1;} return x;");
    assertEquals("LONG: 14", res.type() + ": " + res);
    res = run("x=1; for(i=2; i<10; i+=1) { x+=1; if (x>10) return x; x+=1;} return x;");
    assertEquals("LONG: 12", res.type() + ": " + res);
    res = run("x=1; for(i=2; i<10; i+=1) { x+=1; if (x>10) break; x+=1;} return x;");
    assertEquals("LONG: 12", res.type() + ": " + res);
    res = run("x=1; if (x>1) x=3; else if (x<1) x=-3; else x=-x; return x;");
    assertEquals("LONG: -1", res.type() + ": " + res);
    res = run("x=1; while (x<5) {x+=1; x+=1; x+=1;} return x;");
    assertEquals("LONG: 7", res.type() + ": " + res);
    res = run("x=1; x>0 && x<10;");
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = run("x=1; x<=0 || x>=10 ? x : -x;");
    assertEquals("LONG: -1", res.type() + ": " + res);
    res = run("x=1; for(;;){x+=1; if (x>10) break; x+=1;} return x;");
    assertEquals("LONG: 12", res.type() + ": " + res);
    res = run("x=1; while(true){x+=1; if (x>10) break; x+=1;} return x;");
    assertEquals("LONG: 12", res.type() + ": " + res);
    res = run("x=1; //x>0 && x<10;");
    assertEquals("LONG: 1", res.type() + ": " + res);
    res = run("zzzzz;");
    assertEquals("NULL: NULL", res.type() + ": " + res);
    res = run("/* x=1; */ z>0 && z<10;");
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = run("int[] x = new int[]{1,2}; x[1];");
    assertEquals("LONG: 2", res.type() + ": " + res);
    res = run("params['foo'];");
    assertEquals("STRING: bar", res.type() + ": " + res);
    res = run("params['ab'];");
    assertEquals("NULL: NULL", res.type() + ": " + res);
  }

  @Test
  public void testMapExpressions() throws Exception {
    SelType res = run("x=new Map(); x.put('foo', 123); x.put('bar', 'baz'); return x;");
    assertEquals("MAP: {bar=baz, foo=123}", res.type() + ": " + res);
    res = run("x=new Map(); x.put('foo', 123); x.put('bar', 'baz'); return x.get('bar');");
    assertEquals("STRING: baz", res.type() + ": " + res);
    res =
        run(
            "x=new Map(); x.put('foo', 123); x.put('bar', new Map()); "
                + "x.get('bar').put('bat', 123); return x;");
    assertEquals("MAP: {bar={bat=123}, foo=123}", res.type() + ": " + res);
    res = run("x=new Map(); y=123; x.put('foo', y); y=111; return x;");
    assertEquals("MAP: {foo=111}", res.type() + ": " + res);
    res =
        run(
            "x=new Map(); x.put('foo', 123); x.put('bar', new Map()); "
                + "x.get('bar').put('bat', 123); return x.get('bar').get('bat');");
    assertEquals("LONG: 123", res.type() + ": " + res);
    res = run("x=new Map(); x.put('foo', 123); x.put('bar', new long[]{1,2,3}); return x;");
    assertEquals("MAP: {bar=[1, 2, 3], foo=123}", res.type() + ": " + res);
    res =
        run(
            "x=new Map(); x.put('foo', 123); x.put('bar', new long[]{1,2,3}); return x.get('bar')[1];");
    assertEquals("LONG: 2", res.type() + ": " + res);
    res =
        run(
            "x=new Map(); x.put('foo', 123); x.put('bar', new Map()); x.get('bar').put('bat', new Map()); "
                + "x.get('bar').get('bat').put('bat', new boolean[]{true, false}); return x;");
    assertEquals("MAP: {bar={bat={bat=[true, false]}}, foo=123}", res.type() + ": " + res);
    res =
        run(
            "x=new Map(); x.put('foo', 123); x.put('bar', new Map()); x.get('bar').put('bat', new Map()); "
                + "x.get('bar').get('bat').put('bat', new boolean[]{true, false}); return x.get('bar').get('bat').get('bat')[0];");
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
  }

  @Test(expected = ParseException.class)
  public void testInvalidExpr1() throws Exception {
    run("1+1");
  }

  @Test(expected = ParseException.class)
  public void testInvalidExpr2() throws Exception {
    run("int x=1+1=3;");
  }

  @Test(expected = ParseException.class)
  public void testInvalidExpr3() throws Exception {
    run("method();");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidExpr4() throws Exception {
    run("java.util.UUID.randomUUID();");
  }

  @Test(expected = ParseException.class)
  public void testInvalidFor() throws Exception {
    run("x=1; for(i=2, j=1; i<10; i+=1) x+=1; return x;");
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidIndexAccess() throws Exception {
    run("param['ab'];");
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidIndexAccess2() throws Exception {
    run("params[1];");
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testStackOverLimit() throws Exception {
    run("int[] x = new int[]{1,2,3,4,5,6,7,8,9};");
  }

  @Test(expected = IllegalStateException.class)
  public void testLoopOverLimit() throws Exception {
    run("while(true) 1+1;");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayOverLimit1() throws Exception {
    run("int[] x = new int[100];");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayOverLimit2() throws Exception {
    visitor = new SelParserEvaluationVisitor(10, 10, 1, 100);
    run("int[] x = new int[]{1,2,3,4,5};");
  }

  @Test
  public void testLargeNestedForLoopExpr() throws Exception {
    visitor = new SelParserEvaluationVisitor(128, 10000, 10000, 1000000000L);
    MemoryCounter.setMemoryLimit(500000000);
    SelType res =
        run(
            "m=0; "
                + "for(i = 0;i < 1000;i += 1) {"
                + "  for(j = 0; j < 100; j += 1) {"
                + "    for(k = 0; k < 100; k += 1) {\n"
                + "      m += 1;"
                + "  }}}"
                + "for(i = 0;i < 100;i += 1) {"
                + "  for(j = 0; j < 100; j += 1) {"
                + "    for(k = 0; k < 100; k += 1) {\n"
                + "      m += 1;"
                + "  }}}"
                + "return m;");
    assertEquals("LONG: 11000000", res.type() + ": " + res);
  }

  @Test(expected = IllegalStateException.class)
  public void testVisitOverLimit() throws Exception {
    visitor = new SelParserEvaluationVisitor(128, 10000, 10000, 10);
    run("j=0; for (i = 0; i < 100; i += 1) j+=1; return j;");
  }

  @Test(expected = IllegalStateException.class)
  public void testMapOverLimit() throws Exception {
    visitor = new SelParserEvaluationVisitor(128, 10000, 10000, 10000);
    MemoryCounter.setMemoryLimit(100);
    run(
        "m = new HashMap(); for (i = 0; i < 100; i += 1) m.put(String.valueOf(i), String.valueOf(i)); return m;");
  }

  @Test(expected = IllegalStateException.class)
  public void testFunctionOverLimit() throws Exception {
    visitor = new SelParserEvaluationVisitor(128, 10000, 10000, 10000);
    MemoryCounter.setMemoryLimit(100);
    run("return Util.intsBetween(0, 100, 1);");
  }
}
