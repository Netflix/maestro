package com.netflix.maestro.dsl.jobs;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
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
public class Subworkflow extends BaseJob {
  /** reserved fields cannot be set within jobParams. */
  private static final Set<String> RESERVED_FIELDS =
      Stream.concat(
              BaseJob.RESERVED_FIELDS.stream(),
              Stream.of(
                  "workflow_id",
                  "subworkflow_id",
                  "version",
                  "subworkflow_version",
                  "sync",
                  "explicit_params"))
          .collect(Collectors.toUnmodifiableSet());

  @JsonAlias({"subworkflow_id"})
  private String workflowId;

  @JsonAlias({"subworkflow_version"})
  private String version;

  private Boolean sync;

  private Boolean explicitParams;

  @Override
  Set<String> getReservedFields() {
    return RESERVED_FIELDS;
  }
}
