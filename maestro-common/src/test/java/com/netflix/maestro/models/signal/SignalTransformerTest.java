package com.netflix.maestro.models.signal;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.parameter.MapParameter;
import java.io.IOException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for SignalTransformer class.
 *
 * @author jun-he
 */
public class SignalTransformerTest extends MaestroBaseTest {
  @Test
  public void testTransformSignalDependencyDefinitionToMap() throws IOException {
    SignalDependenciesDefinition definition =
        loadObject(
            "fixtures/signal/sample-signal-dependencies-definition.json",
            SignalDependenciesDefinition.class);
    MapParameter param = SignalTransformer.transform(definition.definitions().getFirst());
    Assert.assertFalse(param.isEvaluated());
    Assert.assertTrue(param.isLiteral());
    Assert.assertEquals("signal_a", param.getValue().get("name").getValue());
    Assert.assertEquals("test123", param.getValue().get("param_a").getValue());
  }

  @Test
  public void testTransformMapToSignalDependency() throws IOException {
    SignalDependenciesDefinition definition =
        loadObject(
            "fixtures/signal/sample-signal-dependencies-definition.json",
            SignalDependenciesDefinition.class);
    MapParameter param =
        MapParameter.builder()
            .evaluatedResult(Map.of("name", "signal_a", "param_a", "test123"))
            .build();
    var dependency = SignalTransformer.transform(definition.definitions().getFirst(), param);
    Assert.assertEquals("signal_a", dependency.getName());
    Assert.assertEquals(StepDependencyMatchStatus.PENDING, dependency.getStatus());
    Assert.assertEquals(
        Map.of(
            "param_a",
            SignalMatchParam.builder()
                .value(SignalParamValue.of("test123"))
                .operator(SignalOperator.EQUALS_TO)
                .build()),
        dependency.getMatchParams());
  }

  @Test
  public void testTransformSignalOutputDefinitionToMap() throws IOException {
    SignalOutputsDefinition definition =
        loadObject(
            "fixtures/step_outputs/step_outputs_definition.json", SignalOutputsDefinition.class);
    MapParameter param = SignalTransformer.transform(definition.definitions().getFirst());
    Assert.assertFalse(param.isEvaluated());
    Assert.assertTrue(param.isLiteral());
    Assert.assertEquals("out", param.getValue().get("name").getValue());
    Assert.assertEquals(Long.valueOf(1), param.getValue().get("p1").getValue());
    Assert.assertEquals("1+1", param.getValue().get("p2").getExpression());
  }

  @Test
  public void testTransformMapToSignalOutput() throws IOException {
    SignalOutputsDefinition definition =
        loadObject(
            "fixtures/step_outputs/step_outputs_definition.json", SignalOutputsDefinition.class);
    MapParameter param =
        MapParameter.builder().evaluatedResult(Map.of("name", "out", "p1", 1, "p2", 2)).build();
    var output = SignalTransformer.transform(definition.definitions().getFirst(), param);
    Assert.assertEquals("out", output.getName());
    Assert.assertEquals(
        Map.of("p1", SignalParamValue.of(1), "p2", SignalParamValue.of(2)), output.getParams());
  }

  @Test
  public void testTransformMapOnlyToSignalOutput() throws IOException {
    MapParameter param =
        MapParameter.builder().evaluatedResult(Map.of("name", "out", "p1", 1, "p2", 2)).build();
    var output = SignalTransformer.transform(param);
    Assert.assertEquals("out", output.getName());
    Assert.assertEquals(
        Map.of("p1", SignalParamValue.of(1), "p2", SignalParamValue.of(2)), output.getParams());
  }
}
