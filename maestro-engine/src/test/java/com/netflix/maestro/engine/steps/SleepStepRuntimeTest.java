package com.netflix.maestro.engine.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.utils.TimeUtils;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class SleepStepRuntimeTest extends MaestroEngineBaseTest {
  private final SleepStepRuntime sleepRuntime = new SleepStepRuntime();
  private final WorkflowSummary workflowSummary = new WorkflowSummary();
  private final Parameter sleepParam = buildParam("sleep_seconds", 10L);
  private final StepRuntimeSummary runtimeSummary =
      StepRuntimeSummary.builder()
          .stepId("test")
          .stepAttemptId(1)
          .stepInstanceUuid("uuid")
          .stepName("testSleep")
          .stepInstanceId(1)
          .type(StepType.SLEEP)
          .params(Map.of("sleep_seconds", sleepParam))
          .build();

  @BeforeClass
  public static void init() {
    MaestroEngineBaseTest.init();
  }

  @Test
  public void testSleepExecuteWithPollingInterval() {
    sleepParam.setEvaluatedResult(10L);

    // Set start time to simulate elapsed time
    runtimeSummary.getRuntimeState().setStartTime(System.currentTimeMillis() - 3000);
    TimeUtils.sleep(1);

    StepRuntime.Result result =
        sleepRuntime.execute(workflowSummary, new TypedStep(), runtimeSummary);

    // Should return CONTINUE with remaining time as polling interval
    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.nextPollingDelayInMillis() < 7000L);
    assertTrue(result.nextPollingDelayInMillis() > 0L);
  }

  @Test
  public void testSleepExecuteCompleted() {
    sleepParam.setEvaluatedResult(5L);

    // Set start time to simulate sleep duration has elapsed
    runtimeSummary.getRuntimeState().setStartTime(System.currentTimeMillis() - 6000);

    StepRuntime.Result result =
        sleepRuntime.execute(workflowSummary, new TypedStep(), runtimeSummary);

    // Should return DONE with no polling interval
    assertEquals(StepRuntime.State.DONE, result.state());
    assertNull(result.nextPollingDelayInMillis());
  }

  @Test
  public void testSleepExecuteZeroDuration() {
    sleepParam.setEvaluatedResult(0L);
    runtimeSummary.getRuntimeState().setStartTime(System.currentTimeMillis() - 3000);

    StepRuntime.Result result =
        sleepRuntime.execute(workflowSummary, new TypedStep(), runtimeSummary);

    // Should return DONE immediately for zero duration
    assertEquals(StepRuntime.State.DONE, result.state());
    assertNull(result.nextPollingDelayInMillis());
  }
}
