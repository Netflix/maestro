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
      WorkflowRunSummary summary =
          loadObject("fixtures/aggregated/" + filename, WorkflowRunSummary.class);
      assertEquals(
          summary, MAPPER.readValue(MAPPER.writeValueAsString(summary), WorkflowRunSummary.class));
    }
  }
}
