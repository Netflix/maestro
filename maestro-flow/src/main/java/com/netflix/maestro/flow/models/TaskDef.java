package com.netflix.maestro.flow.models;

import com.netflix.maestro.annotations.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Task definition data model. It does not include timeout as its value is still unknown. Note that
 * task timeout only emits a timeout trigger to inform the maestro engine. Maestro engine will then
 * handle the timeout. Then the flow status will set as timeout.
 *
 * @param taskReferenceName unique id within a flow definition.
 * @param type task type
 * @param input task input data
 * @param joinOn this is nullable and optional field.
 * @author jun-he
 */
public record TaskDef(
    String taskReferenceName,
    String type,
    @Nullable Map<String, Object> input,
    @Nullable List<String> joinOn) {}
