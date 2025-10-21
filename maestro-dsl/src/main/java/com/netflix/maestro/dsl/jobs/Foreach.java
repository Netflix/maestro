package com.netflix.maestro.dsl.jobs;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.dsl.Dag;
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
public class Foreach extends BaseJob {
  /** reserved fields cannot be set within jobParams. */
  private static final Set<String> RESERVED_FIELDS =
      Stream.concat(
              BaseJob.RESERVED_FIELDS.stream(),
              Stream.of(
                  "concurrency",
                  "job_concurrency",
                  "loop_params",
                  "params",
                  "ranges",
                  "jobs",
                  "dag"))
          .collect(Collectors.toUnmodifiableSet());

  @JsonAlias({"job_concurrency"})
  private Long concurrency; // null means to use system default

  @JsonAlias({"params"})
  private Map<String, Object> loopParams;

  private Map<String, Range> ranges;

  private List<Job> jobs;
  private Dag dag;

  @Override
  Set<String> getReservedFields() {
    return RESERVED_FIELDS;
  }

  /**
   * Range for Foreach loop.
   *
   * @param from nullable starting point, if null, defaults to 0
   * @param to exclusive ending point
   * @param increment nullable, if null, defaults to 1. It supports negative numbers for reverse
   *     loops
   */
  public record Range(@Nullable Long from, Long to, @Nullable Long increment) {}
}
