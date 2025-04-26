package com.netflix.maestro.models.signal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transform signal related data model to map parameter for the evaluation. It also transforms the
 * definition with the evaluated params back to the related data model. The transformer currently
 * only is for signal dependency and signal outputs.
 *
 * @author jun-he
 */
public final class SignalTransformer {
  private static final String NAME_FIELD = "name";
  private static final String SIGNAL_PARAM_NAME = "maestro_signal_param";

  /** private constructor for utility class. */
  private SignalTransformer() {}

  @JsonIgnore
  public static MapParameter transform(
      SignalDependenciesDefinition.SignalDependencyDefinition definition) {
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    if (definition.getMatchParams() != null) {
      definition.getMatchParams().forEach((k, v) -> params.put(k, v.getParam()));
    }
    params.put(
        NAME_FIELD,
        StringParamDefinition.builder().name(NAME_FIELD).value(definition.getName()).build());
    return MapParameter.builder().name(SIGNAL_PARAM_NAME).value(params).build();
  }

  public static SignalDependencies.SignalDependency transform(
      SignalDependenciesDefinition.SignalDependencyDefinition definition, MapParameter mapParam) {
    var dependency = new SignalDependencies.SignalDependency();
    Map<String, SignalMatchParam> params = new LinkedHashMap<>();
    mapParam
        .getEvaluatedResult()
        .forEach(
            (k, v) -> {
              if (NAME_FIELD.equals(k)) {
                dependency.setName((String) v);
              } else {
                SignalParamValue val =
                    v instanceof String
                        ? SignalParamValue.of((String) v)
                        : SignalParamValue.of((long) v);
                params.put(
                    k,
                    SignalMatchParam.builder()
                        .value(val)
                        .operator(definition.getMatchParams().get(k).getOperator())
                        .build());
              }
            });
    dependency.setMatchParams(params);
    dependency.setStatus(StepDependencyMatchStatus.PENDING);
    return dependency;
  }

  public static MapParameter transform(SignalOutputsDefinition.SignalOutputDefinition definition) {
    Map<String, ParamDefinition> params = new LinkedHashMap<>();
    if (definition.getPayload() != null) {
      params.putAll(definition.getPayload());
    }
    if (definition.getParams() != null) {
      params.putAll(definition.getParams());
    }
    params.put(
        NAME_FIELD,
        StringParamDefinition.builder().name(NAME_FIELD).value(definition.getName()).build());
    return MapParameter.builder().name(SIGNAL_PARAM_NAME).value(params).build();
  }

  public static SignalOutputs.SignalOutput transform(
      SignalOutputsDefinition.SignalOutputDefinition definition, MapParameter mapParam) {
    var output = new SignalOutputs.SignalOutput();
    Map<String, SignalParamValue> params = new LinkedHashMap<>();
    Map<String, Object> payload = new LinkedHashMap<>();
    mapParam
        .getEvaluatedResult()
        .forEach(
            (k, v) -> {
              if (NAME_FIELD.equals(k)) {
                output.setName((String) v);
              } else if (definition.getParams() != null && definition.getParams().containsKey(k)) {
                SignalParamValue val =
                    v instanceof String
                        ? SignalParamValue.of((String) v)
                        : SignalParamValue.of(Long.parseLong(String.valueOf(v)));
                params.put(k, val);
              } else {
                payload.put(k, v);
              }
            });
    if (!params.isEmpty()) {
      output.setParams(params);
    }
    if (!payload.isEmpty()) {
      output.setPayload(payload);
    }
    return output;
  }

  /**
   * Transform a dynamic output signals. As the signal definition is unknown, it puts string and
   * long param into params and other params into payloads.
   *
   * @param mapParam map param to transform
   * @return the signal output
   */
  public static SignalOutputs.SignalOutput transform(MapParameter mapParam) {
    var output = new SignalOutputs.SignalOutput();
    Map<String, SignalParamValue> params = new LinkedHashMap<>();
    Map<String, Object> payload = new LinkedHashMap<>();
    mapParam
        .getEvaluatedResult()
        .forEach(
            (k, v) -> {
              if (NAME_FIELD.equals(k)) {
                output.setName((String) v);
              } else if (v instanceof String) {
                params.put(k, SignalParamValue.of((String) v));
              } else {
                var val = ObjectHelper.toNumeric(String.valueOf(v));
                if (val.isPresent()) {
                  params.put(k, SignalParamValue.of(val.getAsLong()));
                } else {
                  payload.put(k, v);
                }
              }
            });
    if (!params.isEmpty()) {
      output.setParams(params);
    }
    if (!payload.isEmpty()) {
      output.setPayload(payload);
    }
    return output;
  }
}
