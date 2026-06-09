/*
 * Copyright 2026 Netflix, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.netflix.sel.type.SelType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Proves SEL confines hostile expressions through its language and type system alone, driving the
 * production entry point {@link SelEvaluator#evaluate}. {@link #testEveryHostileProbeIsRejected()}
 * runs a large adversarial corpus where each probe must be rejected; the targeted tests pin the
 * specific rejection reason for each mechanism; the isolation tests cover caller-memory protection,
 * variable leakage across reuse, and concurrent evaluation.
 */
public class SelJailbreakTest {

  private SelEvaluator evaluator;

  @Before
  public void setUp() {
    evaluator = new SelEvaluator(4, 5000, 50, 100, 50, 256, 100000, 1024 * 1024);
  }

  @After
  public void tearDown() {
    evaluator.stop();
  }

  private SelType run(String expr) throws Exception {
    return evaluator.evaluate(expr, new HashMap<>());
  }

  private SelType run(String expr, Map<String, Object> input) throws Exception {
    return evaluator.evaluate(expr, input);
  }

  /**
   * Asserts the expression is refused with the given cause type and message substring. Evaluation
   * runs on a worker thread, so a failure surfaces as ExecutionException wrapping the real cause.
   */
  private void assertRefused(
      String expr, Class<? extends Throwable> causeType, String msgContains) {
    ExecutionException wrapper =
        assertThrows(
            "expected [" + expr + "] to be refused", ExecutionException.class, () -> run(expr));
    Throwable cause = wrapper.getCause();
    assertTrue(
        "[" + expr + "] expected cause " + causeType.getSimpleName() + " but was " + cause,
        causeType.isInstance(cause));
    assertTrue(
        "[" + expr + "] cause message was: " + cause.getMessage(),
        cause.getMessage() != null && cause.getMessage().contains(msgContains));
  }

  // Classes outside SEL's registry: naming, constructing, or calling them must be refused.
  private static final String[] FORBIDDEN_CLASSES = {
    "Runtime",
    "ProcessBuilder",
    "Process",
    "ProcessHandle",
    "File",
    "FileInputStream",
    "FileOutputStream",
    "FileReader",
    "FileWriter",
    "RandomAccessFile",
    "Files",
    "Paths",
    "Path",
    "FileChannel",
    "Socket",
    "ServerSocket",
    "DatagramSocket",
    "URL",
    "URI",
    "URLConnection",
    "InetAddress",
    "HttpClient",
    "HttpURLConnection",
    "Thread",
    "ThreadGroup",
    "ThreadLocal",
    "Executors",
    "ThreadPoolExecutor",
    "Semaphore",
    "ReentrantLock",
    "CountDownLatch",
    "CompletableFuture",
    "ClassLoader",
    "URLClassLoader",
    "Class",
    "Method",
    "Field",
    "Constructor",
    "MethodHandles",
    "ObjectInputStream",
    "ObjectOutputStream",
    "Serializable",
    "ScriptEngineManager",
    "Logger",
    "LogManager",
    "Properties",
    "Unsafe",
    "Console",
    "System"
  };

  private static List<String> classForms(String c) {
    List<String> out = new ArrayList<>();
    out.add("new " + c + "();");
    out.add("new " + c + "('arg');");
    out.add(c + ".getInstance();");
    out.add(c + ".getRuntime();");
    out.add("import java.lang." + c + "; " + c + ".getInstance();");
    out.add(c + " x = new " + c + "(); return x;");
    return out;
  }

  // Reflection / object-protocol methods that must not be callable on any supported value.
  private static final String[] FORBIDDEN_METHODS = {
    "getClass()",
    "getClassLoader()",
    "forName('java.lang.Runtime')",
    "newInstance()",
    "getDeclaredMethod('x')",
    "getMethod('x')",
    "getDeclaredField('x')",
    "getField('x')",
    "getDeclaredMethods()",
    "getMethods()",
    "getConstructors()",
    "getProtectionDomain()",
    "getResource('x')",
    "getResourceAsStream('x')",
    "setAccessible(true)",
    "invoke(null)",
    "wait()",
    "notify()",
    "notifyAll()",
    "clone()",
    "finalize()",
    "hashCode()"
  };

  private static final String[] RECEIVERS = {
    "'abc'", "1", "1L", "1.0", "true", "new Map()", "new DateTime(0L)", "new int[]{1}", "params"
  };

  // Direct attempts at the dangerous namesakes of the safe System / Math stand-ins.
  private static final String[] SYSTEM_MATH_ABUSE = {
    "System.exit(1);",
    "System.setProperty('a','b');",
    "System.getProperty('user.home');",
    "System.getenv('PATH');",
    "System.setSecurityManager(null);",
    "System.load('/x');",
    "System.loadLibrary('x');",
    "System.gc();",
    "System.console();",
    "System.inheritedChannel();",
    "System.out.println('x');",
    "System.err.println('x');",
    "System.arraycopy(1,1,1,1,1);",
    "Math.sqrt(2.0);",
    "Math.abs(-1);",
    "Math.floor(1.5);",
    "Math.exp(1.0);",
    "Math.getClass();"
  };

  // Network egress across varied targets: public name, loopback name/ip, link-local/metadata,
  // private ranges, and sensitive host:port pairs. None of the classes are reachable.
  private static final String[] NETWORK_TARGETS = {
    "google.com", "localhost", "127.0.0.1", "0.0.0.0", "169.254.169.254", "10.0.0.1",
    "192.168.0.1", "::1", "localhost:22", "127.0.0.1:6379", "10.0.0.1:3306", "localhost:80"
  };

  private static List<String> networkForms() {
    List<String> out = new ArrayList<>();
    for (String t : NETWORK_TARGETS) {
      out.add("InetAddress.getByName('" + t + "');");
      out.add("new Socket('" + t + "', 80);");
      out.add("new URL('http://" + t + "/').openStream();");
    }
    return out;
  }

  // Process / file / thread / dynamic-reflection / deserialization chains.
  private static final String[] CAPABILITY_CHAINS = {
    "Runtime.getRuntime().exec('id');",
    "Runtime.getRuntime().halt(0);",
    "new ProcessBuilder('sh').start();",
    "new File('/etc/passwd').delete();",
    "new FileInputStream('/etc/passwd').read();",
    "Files.readAllBytes(Paths.get('/etc/passwd'));",
    "new Thread().start();",
    "Thread.currentThread().setContextClassLoader(null);",
    "Class.forName('java.lang.' + 'Runtime');",
    "Class.forName('java.lang.Runtime').getMethod('exec');",
    "new ScriptEngineManager().getEngineByName('js');",
    "Logger.getLogger('x').info('y');",
    "new ObjectInputStream(params).readObject();",
    "Unsafe.getUnsafe();"
  };

  private static Set<String> buildCorpus() {
    Set<String> probes = new LinkedHashSet<>();
    for (String c : FORBIDDEN_CLASSES) {
      probes.addAll(classForms(c));
    }
    for (String r : RECEIVERS) {
      for (String m : FORBIDDEN_METHODS) {
        probes.add(r + "." + m + ";");
      }
    }
    Collections.addAll(probes, SYSTEM_MATH_ABUSE);
    probes.addAll(networkForms());
    Collections.addAll(probes, CAPABILITY_CHAINS);
    return probes;
  }

  @Test
  public void testEveryHostileProbeIsRejected() {
    Set<String> corpus = buildCorpus();
    assertTrue("corpus should be large", corpus.size() >= 300);

    List<String> escaped = new ArrayList<>();
    for (String probe : corpus) {
      try {
        run(probe);
        escaped.add(probe);
      } catch (Throwable expected) {
        // rejected, as required
      }
    }
    if (!escaped.isEmpty()) {
      fail("Probes NOT rejected (" + escaped.size() + "/" + corpus.size() + "): " + escaped);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Targeted cases: pin the specific rejection reason for the load-bearing mechanisms.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRejectsFullyQualifiedClassName() {
    // A FQN inside `new` is a parse error (the grammar's class type is a bare identifier only);
    // through the worker it surfaces as ExecutionException wrapping ParseException.
    assertRefused(
        "new java.io.FileWriter('/tmp/x');", com.netflix.sel.ast.ParseException.class, "");
    // A FQN as a bare expression is field access on identifier `java`, which resolves to NULL.
    assertRefused(
        "java.lang.Runtime.getRuntime();",
        UnsupportedOperationException.class,
        "NULL DO NOT support accessing field: lang");
  }

  @Test
  public void testRejectsConstructingUnknownClass() {
    assertRefused(
        "new ProcessBuilder('sh');",
        UnsupportedOperationException.class,
        "does not include class:  ProcessBuilder");
  }

  @Test
  public void testImportDoesNotEnableArbitraryClass() {
    assertRefused(
        "import java.lang.Runtime; Runtime.getRuntime().exec('id');",
        UnsupportedOperationException.class,
        "NULL DO NOT support calling getRuntime");
  }

  @Test
  public void testSystemStandInRejectsDangerousMethods() throws Exception {
    assertEquals("LONG", run("System.currentTimeMillis();").type().name());
    assertRefused(
        "System.exit(1);",
        UnsupportedOperationException.class,
        "PRESET_MISC_FUNC_FIELDS DO NOT support calling method: exit");
  }

  @Test
  public void testMathStandInRejectsNonWhitelistedMethods() {
    assertRefused(
        "Math.sqrt(2.0);",
        UnsupportedOperationException.class,
        "MATH DO NOT support calling method: sqrt");
  }

  @Test
  public void testRejectsReflection() {
    assertRefused(
        "'abc'.getClass();",
        UnsupportedOperationException.class,
        "STRING DO NOT support calling method: getClass");
  }

  @Test
  public void testRejectsUnknownTimeZoneIdWithoutDiskAccess() {
    assertRefused(
        "DateTimeZone.forID('../../etc/passwd');",
        IllegalArgumentException.class,
        "is not recognised");
  }

  @Test
  public void testAllowsBenignExpression() throws Exception {
    assertEquals("LONG", run("DateTime d = new DateTime(0L); d.getYear();").type().name());
  }

  // ---------------------------------------------------------------------------------------------
  // Isolation: an expression cannot reach the caller's objects, leak across reuse, or across
  // threads.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testExpressionCannotMutateCallerMapParam() throws Exception {
    Map<String, Object> nested = new HashMap<>();
    nested.put("k", "original");
    Map<String, Object> input = new HashMap<>();
    input.put("cfg", nested);

    run("params.get('cfg').put('k', 'tampered'); return 1L;", input);

    assertEquals("caller's nested map must be unchanged", "original", nested.get("k"));
  }

  @Test
  public void testExpressionCannotMutateCallerArrayParam() throws Exception {
    long[] arr = {1L, 2L, 3L};
    Map<String, Object> input = new HashMap<>();
    input.put("xs", arr);

    run("long[] xs = params.get('xs'); xs[0] = 99; return 1L;", input);

    assertEquals("caller's array must be unchanged", 1L, arr[0]);
  }

  @Test
  public void testExpressionCannotWriteThroughParams() {
    Map<String, Object> input = new HashMap<>();
    input.put("a", "x");

    // params exposes only get/containsKey; there is no write path back into the caller's bag.
    assertThrows(ExecutionException.class, () -> run("params.put('a', 'z'); return 1L;", input));
    assertEquals("caller map entry must be unchanged", "x", input.get("a"));
    assertEquals("caller map size must be unchanged", 1, input.size());
  }

  @Test
  public void testReusedEvaluatorDoesNotLeakVariablesAcrossEvaluations() throws Exception {
    assertEquals("LONG", run("x = 5; return x;").type().name());
    // A later evaluation must not see x: SelThread clears its state after each evaluation.
    assertEquals("NULL", run("return x;").type().name());
  }

  /**
   * Many evaluations submitted concurrently to the evaluator's own thread pool, each with a
   * distinct input, must each return their own value and never observe another's. This is the
   * production concurrency model (SelEvaluator owns the pool of SelThreads).
   */
  @Test
  public void testConcurrentEvaluationsAreIsolated() throws Exception {
    int count = 2000;
    AtomicReference<String> failure = new AtomicReference<>();
    List<Thread> drivers = new ArrayList<>();
    for (int t = 0; t < 8; t++) {
      final int id = t;
      Thread driver =
          new Thread(
              () -> {
                for (int i = 0; i < count / 8 && failure.get() == null; i++) {
                  String secret = "driver-" + id + "-" + i;
                  Map<String, Object> input = new HashMap<>();
                  input.put("secret", secret);
                  try {
                    SelType res = run("return params.get('secret');", input);
                    if (!secret.equals(res.getInternalVal())) {
                      failure.compareAndSet(
                          null, "expected " + secret + " got " + res.getInternalVal());
                    }
                  } catch (Throwable e) {
                    failure.compareAndSet(null, "driver " + id + " threw " + e);
                  }
                }
              });
      drivers.add(driver);
    }
    for (Thread d : drivers) {
      d.start();
    }
    for (Thread d : drivers) {
      d.join();
    }
    if (failure.get() != null) {
      fail("concurrent isolation violated: " + failure.get());
    }
  }
}
