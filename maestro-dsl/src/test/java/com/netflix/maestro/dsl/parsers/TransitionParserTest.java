package com.netflix.maestro.dsl.parsers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.definition.StepTransition;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TransitionParserTest {
  @Test
  public void testParseTransition() {
    assertNull(TransitionParser.parse(null));
    assertNull(TransitionParser.parse(List.of()));

    // simple
    parseAndCompare(List.of("job.2"), Map.of("job.2", "true"));

    // conditional
    parseAndCompare(List.of("IF param1 > 0 THEN job.2"), Map.of("job.2", "param1 > 0"));
    parseAndCompare(
        List.of("IF params.getFromStep('job.3', 'param2') > 0 THEN job.5"),
        Map.of("job.5", "params.getFromStep('job.3', 'param2') > 0"));

    // otherwise case
    parseAndCompare(List.of("OTHERWISE job.2"), Map.of("job.2", "true"));

    // mixed
    parseAndCompare(
        List.of("IF param2 < 0 THEN job.2", "OTHERWISE job.3"),
        Map.of("job.2", "param2 < 0", "job.3", "!((param2 < 0))"));
    parseAndCompare(
        List.of("if param2 < 0 THEN job.2", "OTHErwISE job.3"),
        Map.of("job.2", "param2 < 0", "job.3", "!((param2 < 0))"));
    parseAndCompare(
        List.of("IF param1 > 0 THEN job.2", "IF param2 < 10 THEN job.3", "OTHERWISE job.4"),
        Map.of(
            "job.2",
            "param1 > 0",
            "job.3",
            "param2 < 10",
            "job.4",
            "!((param1 > 0) || (param2 < 10))"));

    parseAndCompare(List.of("job.1", "", "job.2"), Map.of("job.1", "true", "job.2", "true"));
  }

  private void parseAndCompare(List<String> transitionList, Map<String, String> expected) {
    StepTransition result = TransitionParser.parse(transitionList);
    assertEquals(expected, result.getSuccessors());
  }

  @Test(expected = MaestroValidationException.class)
  public void testParseInvalidFormat() {
    parseAndCompare(List.of("invalid value"), Map.of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseInvalidList() {
    parseAndCompare(
        List.of("IF param1 > 0 THEN job.2", "OTHERWISE job.4", "IF param2 < 10 THEN job.3"),
        Map.of());
  }
}
