package com.netflix.maestro.dsl.jobs;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class While extends BaseJob {
  /** reserved fields cannot be set within jobParams. */
  private static final Set<String> RESERVED_FIELDS =
      Stream.concat(
              BaseJob.RESERVED_FIELDS.stream(),
              Stream.of("loop_params", "params", "condition", "jobs"))
          .collect(Collectors.toUnmodifiableSet());

  @JsonAlias({"params"})
  private Map<String, Object> loopParams;

  private String condition; // SEL expression to be evaluated before each iteration

  private List<Job> jobs;

  @Override
  Set<String> getReservedFields() {
    return RESERVED_FIELDS;
  }
}
