package com.netflix.maestro.dsl;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class WorkflowDslTest extends BaseTest {

  @Test
  public void testDslWorkflowSerde() throws IOException {
    var wf = loadObject("fixtures/sample-dsl-wf-1.yaml", DslWorkflowDef.class);
    String serialized = mapper.writeValueAsString(wf);
    var deserialized = mapper.readValue(serialized, DslWorkflowDef.class);
    assertEquals(wf, deserialized);
  }
}
