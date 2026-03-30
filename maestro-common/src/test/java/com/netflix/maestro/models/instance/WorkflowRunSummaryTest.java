package com.netflix.maestro.models.instance;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowRunSummaryTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    for (String filename :
        Arrays.asList("workflow-run-summary.json", "workflow-run-summary-in-progress.json")) {
      WorkflowRunSummary expected =
          loadObject("fixtures/aggregated/" + filename, WorkflowRunSummary.class);
      String ser1 = MAPPER.writeValueAsString(expected);
      WorkflowRunSummary actual =
          MAPPER.readValue(MAPPER.writeValueAsString(expected), WorkflowRunSummary.class);
      String ser2 = MAPPER.writeValueAsString(actual);
      assertEquals(expected, actual);
      assertEquals(ser1, ser2);
    }
  }
}
