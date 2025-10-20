package com.netflix.maestro.dsl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * DSL Workflow Definition.
 *
 * @param workflow DSL workflow
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record DslWorkflowDef(@JsonAlias({"Workflow"}) DslWorkflow workflow) {}
