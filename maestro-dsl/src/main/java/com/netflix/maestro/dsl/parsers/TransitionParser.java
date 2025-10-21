package com.netflix.maestro.dsl.parsers;

import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse DSL transition definitions to Maestro StepTransition objects.
 *
 * <p>Supported formats:
 *
 * <ul>
 *   <li>"job.1" - always-true successor
 *   <li>"IF param2 < 0 THEN job.2" - conditional successor with SEL expression
 *   <li>"OTHERWISE job.3" - fallback successor (negation of all previous IF conditions)
 * </ul>
 */
final class TransitionParser {
  // Pattern to match a simple successor job id (no IF/OTHERWISE keywords)
  private static final Pattern SIMPLE_PATTERN =
      Pattern.compile("^\\s*([_a-zA-Z][.\\-_a-zA-Z0-9]*+)\\s*$");
  // CASE_INSENSITIVE pattern to match: IF <condition> THEN <successor_job_id>
  private static final Pattern CONDITIONAL_PATTERN =
      Pattern.compile(
          "^\\s*IF\\s+(.+?)\\s+THEN\\s+([_a-zA-Z][.\\-_a-zA-Z0-9]*+)\\s*$",
          Pattern.CASE_INSENSITIVE);
  // CASE_INSENSITIVE Pattern to match: OTHERWISE <successor_job_id>
  private static final Pattern OTHERWISE_PATTERN =
      Pattern.compile(
          "^\\s*OTHERWISE\\s+([_a-zA-Z][.\\-_a-zA-Z0-9]*+)\\s*$", Pattern.CASE_INSENSITIVE);
  private static final String NO_CONDITION = "true";

  private TransitionParser() {}

  /**
   * Parse a list of dsl transition strings into a Maestro StepTransition object.
   *
   * @param transitionList list of dsl transition strings
   * @return parsed StepTransition object
   */
  public static StepTransition parse(List<String> transitionList) {
    if (transitionList == null || transitionList.isEmpty()) {
      return null;
    }

    StepTransition stepTransition = new StepTransition();
    List<String> ifConditions = new ArrayList<>();
    for (int i = 0; i < transitionList.size(); i++) {
      String next = transitionList.get(i);
      if (ObjectHelper.isNullOrEmpty(next)) {
        continue;
      }

      Matcher simpleMatcher = SIMPLE_PATTERN.matcher(next);
      if (simpleMatcher.matches()) {
        String nextJobId = simpleMatcher.group(1);
        stepTransition.getSuccessors().put(nextJobId, NO_CONDITION);
        continue;
      }

      Matcher conditionalMatcher = CONDITIONAL_PATTERN.matcher(next);
      if (conditionalMatcher.matches()) {
        String condition = conditionalMatcher.group(1);
        String nextJobId = conditionalMatcher.group(2);
        stepTransition.getSuccessors().put(nextJobId, condition);
        ifConditions.add(condition);
        continue;
      }

      Matcher otherwiseMatcher = OTHERWISE_PATTERN.matcher(next);
      if (otherwiseMatcher.matches()) {
        Checks.checkTrue(
            i == transitionList.size() - 1,
            "'OTHERWISE' must be the last row in the transition list: [%s]",
            transitionList);
        String jobId = otherwiseMatcher.group(1);
        String otherwiseCondition =
            ifConditions.isEmpty()
                ? NO_CONDITION
                : "!((" + String.join(") || (", ifConditions) + "))";
        stepTransition.getSuccessors().put(jobId, otherwiseCondition);
        continue;
      }

      throw new MaestroValidationException(
          "Invalid transition: [%s]. Expected formats: 'job_id', 'IF condition THEN job_id', or 'OTHERWISE job_id'",
          next);
    }
    return stepTransition;
  }
}
