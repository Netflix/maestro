/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.engine.params;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.parameter.AbstractParamDefinition;
import com.netflix.maestro.models.parameter.InternalParamMode;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.ParamValidator;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.With;

/** Helper for Params merging and cleanup. */
public final class ParamsMergeHelper {

  private static final Set<ParamMode> DEFAULT_UPDATE_MODES =
      EnumSet.of(ParamMode.MUTABLE, ParamMode.MUTABLE_ON_START, ParamMode.MUTABLE_ON_START_RESTART);
  private static final Set<ParamMode> RESTART_UPDATE_MODES =
      EnumSet.of(ParamMode.MUTABLE, ParamMode.MUTABLE_ON_START_RESTART);

  /** Allowed update modes for each stage for non system. */
  private static final Map<ParamSource, Set<ParamMode>> ALLOWED_UPDATE_MODES =
      Map.of(
          ParamSource.DEFINITION, DEFAULT_UPDATE_MODES,
          ParamSource.FOREACH, DEFAULT_UPDATE_MODES,
          ParamSource.WHILE, DEFAULT_UPDATE_MODES,
          ParamSource.LAUNCH, DEFAULT_UPDATE_MODES,
          ParamSource.SIGNAL, DEFAULT_UPDATE_MODES,
          ParamSource.SUBWORKFLOW, DEFAULT_UPDATE_MODES,
          ParamSource.TEMPLATE, DEFAULT_UPDATE_MODES,
          ParamSource.TIME_TRIGGER, DEFAULT_UPDATE_MODES,
          ParamSource.OUTPUT_PARAMETER, EnumSet.of(ParamMode.MUTABLE),
          ParamSource.RESTART, RESTART_UPDATE_MODES);

  /** Mapping of internal param mode to mode. */
  private static final Map<InternalParamMode, ParamMode> INTERNAL_PARAM_MODE_TO_MODE =
      Map.of(
          InternalParamMode.OPTIONAL,
          ParamMode.MUTABLE,
          InternalParamMode.REQUIRED,
          ParamMode.MUTABLE,
          InternalParamMode.PROVIDED,
          ParamMode.MUTABLE,
          InternalParamMode.RESERVED,
          ParamMode.CONSTANT);

  /** Cannot merge into these modes. */
  private static final Set<InternalParamMode> RESTRICTED_INTERNAL_MODES =
      EnumSet.of(InternalParamMode.RESERVED);

  /** Param mode strictness in increasing order. */
  private static final List<ParamMode> PARAM_MODE_STRICTNESS =
      Arrays.asList(
          ParamMode.MUTABLE,
          ParamMode.MUTABLE_ON_START_RESTART,
          ParamMode.MUTABLE_ON_START,
          ParamMode.IMMUTABLE,
          ParamMode.CONSTANT);

  /** private constructor for utility class. */
  private ParamsMergeHelper() {}

  /** MergeContext object for tracking merge stage. */
  @Getter
  @Setter
  @AllArgsConstructor
  @RequiredArgsConstructor
  public static class MergeContext {
    private final ParamSource mergeSource;
    private final boolean isSystem;
    private final boolean isUpstreamMerge;
    private final boolean isRestartMerge;
    // this is used for nested param merge to determine which mode to use.
    @With @Nullable private ParamMode parentParamMode;

    /** Create merge context for workflow params. */
    public static MergeContext workflowCreate(ParamSource mergeSource, RunRequest request) {
      boolean isUpstreamMerge = request.isSystemInitiatedRun();
      return workflowCreate(mergeSource, isUpstreamMerge, !request.isFreshRun());
    }

    /** Create merge context for step params. */
    public static MergeContext stepCreate(ParamSource mergeSource) {
      return workflowCreate(mergeSource, false);
    }

    /** Create merge context for workflow params. */
    public static MergeContext workflowCreate(ParamSource mergeSource, boolean isUpstreamMerge) {
      return workflowCreate(mergeSource, isUpstreamMerge, false);
    }

    public static MergeContext workflowCreate(
        ParamSource mergeSource, boolean isUpstreamMerge, boolean isRestartMerge) {
      boolean isSystem = mergeSource.isSystemStage();
      return new MergeContext(mergeSource, isSystem, isUpstreamMerge, isRestartMerge);
    }

    /** Copy the original merge context and update the parentParamMode. */
    public static MergeContext copyWithParentMode(MergeContext orig, ParamMode parentParamMode) {
      ParamMode modeToUse;
      if (parentParamMode == null) {
        // we first check if the passed in parent param mode is null or not, if null, keep the
        // current one.
        modeToUse = orig.getParentParamMode();
      } else if (orig.getParentParamMode() == null) {
        // if the current one is null and the passed in parent is not null, then choose the parent
        // one.
        modeToUse = parentParamMode;
      } else {
        // if both are not null, choose the more strict one.
        modeToUse =
            PARAM_MODE_STRICTNESS.indexOf(parentParamMode)
                    > PARAM_MODE_STRICTNESS.indexOf(orig.getParentParamMode())
                ? parentParamMode
                : orig.getParentParamMode();
      }

      return orig.withParentParamMode(modeToUse);
    }
  }

  /**
   * Merge paramsToMerge into params.
   *
   * @param params params
   * @param paramsToMerge params to merge
   * @param context Merge context
   */
  public static void mergeParams(
      Map<String, ParamDefinition> params,
      Map<String, ParamDefinition> paramsToMerge,
      MergeContext context) {
    if (paramsToMerge == null) {
      return;
    }
    Stream.concat(params.keySet().stream(), paramsToMerge.keySet().stream())
        .forEach(
            name -> {
              ParamDefinition paramToMerge = paramsToMerge.get(name);
              if (paramToMerge == null) {
                return;
              }

              if (paramToMerge.getType() == ParamType.MAP && paramToMerge.isLiteral()) {
                Map<String, ParamDefinition> baseMap = mapValueOrEmpty(params, name);
                Map<String, ParamDefinition> toMergeMap = mapValueOrEmpty(paramsToMerge, name);
                mergeParams(
                    baseMap,
                    toMergeMap,
                    MergeContext.copyWithParentMode(
                        context, params.getOrDefault(name, paramToMerge).getMode()));
                params.put(
                    name,
                    buildMergedParamDefinition(
                        name, paramToMerge, params.get(name), context, baseMap));
              } else if (paramToMerge.getType() == ParamType.STRING_MAP
                  && paramToMerge.isLiteral()) {
                Map<String, String> baseMap = stringMapValueOrEmpty(params, name);
                Map<String, String> toMergeMap = stringMapValueOrEmpty(paramsToMerge, name);
                baseMap.putAll(toMergeMap);
                params.put(
                    name,
                    buildMergedParamDefinition(
                        name, paramToMerge, params.get(name), context, baseMap));
              } else {
                params.put(
                    name,
                    buildMergedParamDefinition(
                        name, paramToMerge, params.get(name), context, paramToMerge.getValue()));
              }
            });
  }

  /** Merge output params into parameter map. */
  public static void mergeOutputDataParams(
      Map<String, Parameter> allParams, Map<String, Parameter> params) {
    params.forEach(
        (name, param) -> {
          if (!allParams.containsKey(name)) {
            throw new MaestroValidationException(
                "Invalid output parameter [%s], not defined in params", name);
          }
          MergeContext context = MergeContext.stepCreate(ParamSource.OUTPUT_PARAMETER);
          if (param.getType() == ParamType.MAP && param.isLiteral()) {
            ParamDefinition baseDef = allParams.get(name).toDefinition();
            Map<String, ParamDefinition> baseMap = baseDef.asMapParamDef().getValue();
            ParamDefinition toMergeDef = param.toDefinition();
            Map<String, ParamDefinition> toMergeMap = toMergeDef.asMapParamDef().getValue();

            mergeParams(baseMap, toMergeMap, context);
            Parameter mergedParam =
                buildMergedParamDefinition(name, toMergeDef, baseDef, context, baseMap)
                    .toParameter();
            populateEvaluatedResultAndTime(mergedParam, param.getEvaluatedTime());
            allParams.put(name, mergedParam);
          } else if (param.getType() == ParamType.STRING_MAP && param.isLiteral()) {
            ParamDefinition baseDef = allParams.get(name).toDefinition();
            Map<String, String> baseMap = baseDef.asStringMapParamDef().getValue();
            ParamDefinition toMergeDef = param.toDefinition();
            Map<String, String> toMergeMap = toMergeDef.asStringMapParamDef().getValue();

            baseMap.putAll(toMergeMap);
            Parameter mergedParam =
                buildMergedParamDefinition(name, toMergeDef, baseDef, context, baseMap)
                    .toParameter();
            populateEvaluatedResultAndTime(mergedParam, param.getEvaluatedTime());
            allParams.put(name, mergedParam);
          } else {
            ParamDefinition paramDefinition =
                ParamsMergeHelper.buildMergedParamDefinition(
                    name,
                    param.toDefinition(),
                    allParams.get(name).toDefinition(),
                    MergeContext.stepCreate(ParamSource.OUTPUT_PARAMETER),
                    param.getValue());
            Parameter parameter = paramDefinition.toParameter();
            parameter.setEvaluatedResult(param.getEvaluatedResult());
            parameter.setEvaluatedTime(param.getEvaluatedTime());
            allParams.put(name, parameter);
          }
        });
  }

  /** Use the parameter value to populate the evaluated result for output parameter. */
  private static Parameter populateEvaluatedResultAndTime(Parameter param, Long evaluatedTime) {
    if (param.getType() == ParamType.MAP) {
      Map<String, Object> evaluatedResult = new LinkedHashMap<>();
      Map<String, ParamDefinition> mapParamValue = param.getValue();
      for (Map.Entry<String, ParamDefinition> entry : mapParamValue.entrySet()) {
        ParamDefinition def = entry.getValue();
        Parameter populatedParam = populateEvaluatedResultAndTime(def.toParameter(), evaluatedTime);
        evaluatedResult.put(entry.getKey(), populatedParam.getEvaluatedResult());
      }
      param.setEvaluatedResult(evaluatedResult);
    } else {
      param.setEvaluatedResult(param.getValue());
    }
    param.setEvaluatedTime(evaluatedTime);
    return param;
  }

  /**
   * Convert nested Param Definition map to Parameter map.
   *
   * @param params param definitions
   * @return corresponding parameters
   */
  public static Map<String, Parameter> convertToParameters(Map<String, ParamDefinition> params) {
    return params.entrySet().stream()
        .collect(
            MapHelper.toListMap(
                Map.Entry::getKey,
                p -> {
                  ParamDefinition param = p.getValue();
                  if (param.getType() == ParamType.MAP) {
                    MapParamDefinition childMap = param.asMapParamDef();
                    return MapParameter.builder()
                        .name(childMap.getName())
                        .value(childMap.getValue())
                        .expression(childMap.getExpression())
                        .validator(childMap.getValidator())
                        .mode(childMap.getMode())
                        .meta(childMap.getMeta())
                        .tags(childMap.getTags())
                        .build();
                  } else {
                    return p.getValue().toParameter();
                  }
                }));
  }

  /** Cleanup parameters, remove unused optional params. */
  @SuppressWarnings({"SimplifyBooleanReturn"})
  public static Map<String, ParamDefinition> cleanupParams(Map<String, ParamDefinition> params) {
    if (params == null || params.isEmpty()) {
      return params;
    }
    Map<String, ParamDefinition> mapped =
        params.entrySet().stream()
            .collect(
                MapHelper.toListMap(
                    Map.Entry::getKey,
                    p -> {
                      ParamDefinition param = p.getValue();
                      if (param.getType() == ParamType.MAP) {
                        MapParamDefinition mapParamDef = param.asMapParamDef();
                        if (mapParamDef.getValue() == null
                            && (mapParamDef.getInternalMode() == InternalParamMode.OPTIONAL)) {
                          return mapParamDef;
                        }
                        return MapParamDefinition.builder()
                            .name(mapParamDef.getName())
                            .value(cleanupParams(mapParamDef.getValue()))
                            .expression(mapParamDef.getExpression())
                            .name(mapParamDef.getName())
                            .validator(mapParamDef.getValidator())
                            .tags(mapParamDef.getTags())
                            .mode(mapParamDef.getMode())
                            .meta(mapParamDef.getMeta())
                            .build();
                      } else {
                        return param;
                      }
                    }));
    Map<String, ParamDefinition> filtered =
        mapped.entrySet().stream()
            .filter(
                p -> {
                  ParamDefinition param = p.getValue();
                  if (param.getInternalMode() == InternalParamMode.OPTIONAL) {
                    if (param.getValue() == null && param.getExpression() == null) {
                      return false;
                    } else if (param.getType() == ParamType.MAP
                        && param.asMapParamDef().getValue() != null
                        && param.asMapParamDef().getValue().isEmpty()) {
                      return false;
                    } else {
                      return true;
                    }
                  } else {
                    Checks.checkTrue(
                        param.getValue() != null || param.getExpression() != null,
                        String.format(
                            "[%s] is a required parameter (type=[%s])",
                            p.getKey(), param.getType()));
                    return true;
                  }
                })
            .collect(MapHelper.toListMap(Map.Entry::getKey, Map.Entry::getValue));
    return cleanIntermediateMetadata(filtered);
  }

  /**
   * Build merged parameter definition from new and previous parameter definition.
   *
   * <p>An example sequence is
   *
   * <p>
   *
   * <ol>
   *   <li>For each in Workflow defined Parameter (mergeFrom) is merged into Default workflow
   *       parameter (mergeBaseline)
   *   <li>For each in Run Request defined Parameter (mergeFrom) is merged into the parameters from
   *       step 1
   *   <li>and so on.
   * </ol>
   *
   * <p>Another example could be merging each parameter in a workflow (mergeFrom) is merged into the
   * template schema defined parameters (mergeBaseline).
   *
   * <p>Similarly, for parent-subworkflow case each parameter in subworkflow (mergeFrom) is merge
   * into the parameters defined in the parent workflow (mergeBaseline)
   *
   * <p>Note: What is returned is neither a mergeFrom now a mergeBaseline definition, rather a new
   * definition created by the process of merging these two together
   *
   * @param name Name of the parameter trying to merge
   * @param mergeBaseline The destination definition that we are trying to merge
   * @param mergeFrom The source definition we are trying to merge
   * @param value The value that is finally merged
   * @return resulting merged ParamDefinition
   */
  private static ParamDefinition buildMergedParamDefinition(
      String name,
      ParamDefinition mergeFrom,
      ParamDefinition mergeBaseline,
      MergeContext context,
      Object value) {

    TagList tagList = generateMergedTagList(mergeFrom, mergeBaseline);
    ParamValidator validator = generateValidator(mergeFrom, mergeBaseline);
    Map<String, Object> targetMeta = ((AbstractParamDefinition) mergeFrom).getMeta();
    Map.Entry<ParamMode, InternalParamMode> modes =
        generateParamModes(mergeFrom, mergeBaseline, context);
    ParamMode mode = modes.getKey();
    InternalParamMode internalMode = modes.getValue();

    // now we have all the updated fields for target. We should reassemble the mergeFrom to make
    // sure the mergeFrom vs mergeBaseLine compare is up-to-date.
    ParamDefinition updatedMergeFrom =
        mergeFrom.copyAndUpdate(
            mergeFrom.getValue(), mergeFrom.getExpression(), mode, targetMeta, tagList, validator);

    // handle type conversion.
    ParamDefinition finalizedMergeFrom;
    Object mergedValue = generateMergedValueOrThrow(name, updatedMergeFrom, mergeBaseline, value);
    String mergedExpression = ObjectHelper.valueOrDefault(updatedMergeFrom.getExpression(), null);
    if (mergeBaseline != null) {
      finalizedMergeFrom =
          mergeBaseline.copyAndUpdate(
              mergedValue,
              mergedExpression,
              updatedMergeFrom.getMode(),
              targetMeta,
              updatedMergeFrom.getTags(),
              validator);
    } else {
      finalizedMergeFrom =
          updatedMergeFrom.copyAndUpdate(
              mergedValue,
              mergedExpression,
              updatedMergeFrom.getMode(),
              targetMeta,
              updatedMergeFrom.getTags(),
              validator);
    }

    // now that all the type casting is done and all the fields are updated, validate if this is
    // merge-able.
    validateParamModesIfMergeable(context, name, finalizedMergeFrom, mergeBaseline, internalMode);

    // enrich result's metadata with param source and internal mode. We can do the metadata
    // enrichment last because it is not a part of equal analysis.
    ParamSource source = generateParamSource(name, updatedMergeFrom, mergeBaseline, context);
    Map<String, Object> meta = new LinkedHashMap<>();
    if (source != null) {
      meta.put(Constants.METADATA_SOURCE_KEY, source.name());
    }
    if (internalMode != null) {
      meta.put(Constants.METADATA_INTERNAL_PARAM_MODE, internalMode.name());
    }

    return finalizedMergeFrom.copyAndUpdate(
        finalizedMergeFrom.getValue(),
        finalizedMergeFrom.getExpression(),
        finalizedMergeFrom.getMode(),
        meta,
        finalizedMergeFrom.getTags(),
        validator);
  }

  /**
   * Merging values based on mergeFrom and mergeBaseline.
   *
   * @param name key whose value is being merged
   * @param mergeFrom Type that needs to be cast from
   * @param mergeBaseline Type that needs to be cast to
   * @param value to be cast
   * @return mergedValue after cast
   * @throws MaestroValidationException if unable to cast the values
   */
  private static Object generateMergedValueOrThrow(
      String name, ParamDefinition mergeFrom, ParamDefinition mergeBaseline, Object value) {

    if (value == null || !isChangedParamDef(mergeFrom, mergeBaseline)) {
      return value;
    }

    try {
      Parameter castFromParameter = mergeFrom.toParameter();
      castFromParameter.setEvaluatedResult(value);

      Parameter castIntoParameter = mergeBaseline.toParameter();
      castIntoParameter.setEvaluatedResult(castFromParameter.getEvaluatedResult());

      return castIntoParameter.getEvaluatedResult();
    } catch (Exception e) {
      throw new MaestroValidationException(
          e,
          "ParameterDefinition type mismatch name [%s] from [%s] != to [%s]",
          name,
          mergeFrom.getType(),
          mergeBaseline.getType());
    }
  }

  /** Validate merge is valid for mode and context. */
  private static Map.Entry<ParamMode, InternalParamMode> generateParamModes(
      ParamDefinition target, ParamDefinition previousDef, MergeContext context) {

    ParamMode mode = null;
    if (target.getMode() != null) {
      mode = target.getMode();
    } else if (previousDef != null) {
      mode = previousDef.getMode();
    }

    InternalParamMode internalMode;
    if (previousDef != null && previousDef.getInternalMode() != null) {
      internalMode = previousDef.getInternalMode();
    } else {
      internalMode = target.getInternalMode();
    }

    // check mapped mode from internal mode if missing mode or more strict
    if (internalMode != null) {
      ParamMode mappedMode = INTERNAL_PARAM_MODE_TO_MODE.get(internalMode);
      if (mode == null
          || PARAM_MODE_STRICTNESS.indexOf(mappedMode) > PARAM_MODE_STRICTNESS.indexOf(mode)) {
        mode = mappedMode;
      }
    }
    // Set default MUTABLE if not set
    mode = ObjectHelper.valueOrDefault(mode, Defaults.DEFAULT_PARAM_MODE);
    // If this merging is initiated from parent (subworkflow, foreach and restart previous instance
    // merge) AND the mode of the previous param definition is allowed to be updated, it should be
    // allowed even if the paramToMerge mode is lower than previousParam mode
    // since the parent has full control over the children workflows.
    if (context.isUpstreamMerge()) {
      if (previousDef != null
          && previousDef.getMode() != null
          && PARAM_MODE_STRICTNESS.indexOf(mode)
              < PARAM_MODE_STRICTNESS.indexOf(previousDef.getMode())) {
        mode = previousDef.getMode();
      }
    }

    // If upper layer passes down a more strict mode, use it.
    if (context.getParentParamMode() != null
        && PARAM_MODE_STRICTNESS.indexOf(context.getParentParamMode())
            > PARAM_MODE_STRICTNESS.indexOf(mode)) {
      mode = context.getParentParamMode();
    }
    return new AbstractMap.SimpleImmutableEntry<>(mode, internalMode);
  }

  /**
   * Validate if the mergeFrom param can be merged into mergeBaseline based on their param modes.
   *
   * @param context The merge context.
   * @param name The name of param that is being merged.
   * @param mergeFrom The source definition we are trying to merge.
   * @param mergeBaseline The destination definition that we are trying to merge.
   * @param internalMode The internal mode of the param being merged, generated from
   *     generateParamModes.
   */
  private static void validateParamModesIfMergeable(
      MergeContext context,
      String name,
      ParamDefinition mergeFrom,
      ParamDefinition mergeBaseline,
      InternalParamMode internalMode) {
    if (!context.isSystem()
        && isChangedParamDef(mergeFrom, mergeBaseline)
        && !isUpstreamSystemMode(mergeFrom, context)) {
      if (!getAllowedModes(context).contains(mergeFrom.getMode())) {
        throw new MaestroValidationException(
            "Cannot modify param with mode [%s] for parameter [%s]",
            mergeFrom.getMode().toString(), name);
      }
      if (mergeBaseline != null) {
        // Don't allow setting to less strict mode
        if (mergeBaseline.getMode() != null
            && PARAM_MODE_STRICTNESS.indexOf(mergeFrom.getMode())
                < PARAM_MODE_STRICTNESS.indexOf(mergeBaseline.getMode())) {
          throw new MaestroValidationException(
              "Cannot modify param mode to be less strict for parameter [%s] from [%s] to [%s]",
              name, mergeBaseline.getMode(), mergeFrom.getMode());
        }
      }
      if (mergeFrom.getInternalMode() != null) {
        throw new MaestroValidationException("Cannot modify system mode for parameter [%s]", name);
      }
      if (RESTRICTED_INTERNAL_MODES.contains(internalMode)) {
        throw new MaestroValidationException(
            "Cannot modify param with system mode [%s] for parameter [%s]", internalMode, name);
      }
    }
  }

  /**
   * This method determines the allowed modes. It first gets the allowed set from
   * ALLOWED_UPDATE_MODES by merge context. If this is an upstream restart, restart a subworkflow
   * step for instance, then it will intersect with the allowed set under restart circumstance.
   */
  private static Set<ParamMode> getAllowedModes(MergeContext context) {
    final Set<ParamMode> allowedModesFromMergeSource =
        EnumSet.copyOf(
            ALLOWED_UPDATE_MODES.getOrDefault(context.getMergeSource(), DEFAULT_UPDATE_MODES));
    if (!context.isRestartMerge()) {
      return allowedModesFromMergeSource;
    }

    allowedModesFromMergeSource.retainAll(RESTART_UPDATE_MODES);
    return allowedModesFromMergeSource;
  }

  /**
   * See if change is an upstream change allowed by system. Typically, we would not allow user to
   * change reserved modes through run params, but if this is a param sent by parent and not user,
   * we can allow it.
   */
  private static boolean isUpstreamSystemMode(ParamDefinition target, MergeContext context) {
    return context.isUpstreamMerge()
        && target != null
        && target.getSource() != null
        && target.getSource() != ParamSource.DEFINITION;
  }

  private static ParamSource generateParamSource(
      String name, ParamDefinition target, ParamDefinition previousDef, MergeContext context) {
    // source for param, carry old source if not changed
    String source;
    if (isUnchangedParamDef(target, previousDef)
        && previousDef.getSource() != null
        && (context.isSystem() || context.isUpstreamMerge())) {
      // if the target is not changed and is a system or upstream merge, not a user merge, previous
      // source should be used. However, if this is a user merge, we should update its source.
      source = previousDef.getSource().name();
    } else if (previousDef == null && context.isSystem() && target.getSource() != null) {
      // e.g. restarting a workflow and system first merges all the previous instances params. Since
      // those params already have sources, we should respect them.
      source = target.getSource().name();
    } else {
      source = context.getMergeSource().name();
    }

    // prevent user source changes
    if (!context.isSystem() && !context.isUpstreamMerge()) {
      if (isChangedParamDef(target, previousDef) && target.getSource() != null) {
        throw new MaestroValidationException("Cannot modify source for parameter [%s]", name);
      }
    }
    return ParamSource.create(source);
  }

  private static ParamValidator generateValidator(
      ParamDefinition target, ParamDefinition previousDef) {
    if (previousDef == null || ((AbstractParamDefinition) target).getValidator() != null) {
      return ((AbstractParamDefinition) target).getValidator();
    } else {
      return ((AbstractParamDefinition) previousDef).getValidator();
    }
  }

  private static TagList generateMergedTagList(
      ParamDefinition target, ParamDefinition previousDef) {
    Set<Tag> tags = new HashSet<>();
    if (previousDef != null && previousDef.getTags() != null) {
      tags.addAll(previousDef.getTags().getTags());
    }
    if (target.getTags() != null) {
      tags.addAll(target.getTags().getTags());
    }
    return new TagList(new ArrayList<>(tags));
  }

  private static Map<String, ParamDefinition> mapValueOrEmpty(
      Map<String, ParamDefinition> params, String key) {
    if (params != null && params.containsKey(key)) {
      ParamDefinition paramDef = params.get(key);
      if (paramDef.getType() != ParamType.MAP) {
        throw new MaestroValidationException(
            "ParameterDefinition type mismatch, [%s] is not a [MAP] but [%s]",
            key, paramDef.getType().toString());
      }
      Checks.checkTrue(
          paramDef.isLiteral(),
          "MAP param [%s] definition exp=[%s] is not a literal",
          key,
          paramDef.getExpression());
      return paramDef.asMapParamDef().getValue();
    } else {
      return new LinkedHashMap<>();
    }
  }

  private static Map<String, String> stringMapValueOrEmpty(
      Map<String, ParamDefinition> params, String key) {
    if (params != null && params.containsKey(key)) {
      ParamDefinition paramDef = params.get(key);
      if (paramDef.getType() != ParamType.STRING_MAP) {
        throw new MaestroValidationException(
            "ParameterDefinition type mismatch, [%s] is not a [STRING_MAP] but [%s]",
            key, paramDef.getType().toString());
      }
      Checks.checkTrue(
          paramDef.isLiteral(),
          "STRING_MAP param [%s] definition exp=[%s] is not a literal",
          key,
          paramDef.getExpression());
      return paramDef.asStringMapParamDef().getValue();
    } else {
      return new LinkedHashMap<>();
    }
  }

  /** Check and return true if a parameter is changed from previous version. */
  private static boolean isChangedParamDef(ParamDefinition target, ParamDefinition previousDef) {
    return previousDef != null && !previousDef.equals(target);
  }

  /** Check and return true if existing parameter is getting carried forward. */
  private static boolean isUnchangedParamDef(ParamDefinition target, ParamDefinition previousDef) {
    return previousDef != null && previousDef.equals(target);
  }

  private static Map<String, ParamDefinition> cleanIntermediateMetadata(
      Map<String, ParamDefinition> params) {
    if (params == null) {
      return null;
    }
    return params.entrySet().stream()
        .collect(
            MapHelper.toListMap(
                Map.Entry::getKey,
                p -> {
                  ParamDefinition param = p.getValue();
                  Map<String, Object> meta = ((AbstractParamDefinition) param).getMeta();
                  if (meta != null) {
                    meta.remove(Constants.METADATA_INTERNAL_PARAM_MODE);
                  }
                  if (param.getType() == ParamType.MAP) {
                    MapParamDefinition mapParamDef = param.asMapParamDef();
                    return MapParamDefinition.builder()
                        .name(mapParamDef.getName())
                        .value(cleanIntermediateMetadata(mapParamDef.getValue()))
                        .expression(mapParamDef.getExpression())
                        .name(mapParamDef.getName())
                        .validator(mapParamDef.getValidator())
                        .tags(mapParamDef.getTags())
                        .mode(mapParamDef.getMode())
                        .meta(mapParamDef.getMeta())
                        .build();
                  } else {
                    return param;
                  }
                }));
  }
}
