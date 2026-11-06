package com.netflix.maestro.models.stepruntime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.ParamType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class JobTemplateTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    JobTemplate expected = loadObject("fixtures/stepruntime/job_template.json", JobTemplate.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    assertEquals(Map.of("foo", "bar"), actual.getMetadata().getExtraInfo());
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdeWithSteps() throws Exception {
    JobTemplate expected =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class);
    assertEquals(StepType.TEMPLATE, expected.getDefinition().getStepType());
    TemplatePrepare prepare = expected.getDefinition().getPrepare();
    assertEquals(TemplatePrepare.STATIC, prepare.getType());
    assertEquals(3, prepare.getSteps().size());
    assertEquals(
        List.of("write", "audit", "publish_snapshot"),
        prepare.getSteps().stream().map(Step::getId).toList());
    assertEquals(
        Map.of("audit", "true"), prepare.getSteps().getFirst().getTransition().getSuccessors());
    // the IMMUTABLE param with an expression is the one the template promises outward
    assertEquals(Set.of("published_snapshot"), expected.getDefinition().promisedParams().keySet());
    assertEquals(
        "publish_snapshot__snapshot;",
        expected.getDefinition().promisedParams().get("published_snapshot").getExpression());
    Map<String, TemplateStepInput> positions = expected.getDefinition().getStepInputs();
    assertEquals(List.of("writes", "after_publish"), List.copyOf(positions.keySet()));
    assertEquals(List.of("audit"), positions.get("writes").getSuccessors());
    assertNull(positions.get("writes").getPredecessors());
    assertEquals(List.of("target_table"), positions.get("writes").getParams());
    assertEquals(ParamType.LONG, positions.get("writes").getOutputs().get("row_count").getType());
    assertTrue(positions.get("writes").hasOutputs());
    assertEquals(List.of("publish_snapshot"), positions.get("after_publish").getPredecessors());
    assertEquals(
        List.of("target_table", "published_snapshot"), positions.get("after_publish").getParams());
    assertFalse(positions.get("after_publish").hasOutputs());
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerdeWithStepPreparer() throws Exception {
    JobTemplate expected =
        loadObject("fixtures/stepruntime/job_template_with_step_preparer.json", JobTemplate.class);
    assertEquals(TemplatePrepare.STEP, expected.getDefinition().getPrepare().getType());
    assertEquals("generate", expected.getDefinition().getPrepare().getStep().getId());
    String ser1 = MAPPER.writeValueAsString(expected);
    JobTemplate actual = MAPPER.readValue(ser1, JobTemplate.class);
    assertEquals(expected, actual);
    assertEquals(ser1, MAPPER.writeValueAsString(actual));
  }
}
