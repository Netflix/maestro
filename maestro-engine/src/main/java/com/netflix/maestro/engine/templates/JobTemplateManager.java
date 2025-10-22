/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.engine.templates;

import com.netflix.maestro.engine.dao.MaestroJobTemplateDao;
import com.netflix.maestro.engine.params.ParamsMergeHelper;
import com.netflix.maestro.engine.properties.JobTemplateCacheProperties;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Job schema manager to manage job template definitions from DB. The job (i.e. step subtype)
 * schemas are loaded from database table, which are defined by the subtype owners by making an API
 * call. For example, we can define different jobs based on kubernetes with different schemas. Each
 * job can be identified as a subtype of kubernetes step type.
 */
public class JobTemplateManager {
  private record JobKey(String jobType, String version) {}

  private record Schema(JobTemplate.Definition definition, long expiredAt) {
    public boolean isExpired() {
      return expiredAt > 0 && System.currentTimeMillis() > expiredAt;
    }
  }

  // Job template cache has no size limit. If needed, we can add LRU cache here.
  private final Map<JobKey, Schema> schemaCache = new ConcurrentHashMap<>();

  private final MaestroJobTemplateDao jobTemplateDao;
  private final JobTemplateCacheProperties properties;

  public JobTemplateManager(
      MaestroJobTemplateDao jobTemplateDao, JobTemplateCacheProperties properties) {
    this.jobTemplateDao = jobTemplateDao;
    this.properties = properties;
  }

  /**
   * Load runtime parameters based on the step type and its subtype.
   *
   * @return a collection of runtime generated parameters to inject
   * @param step step definition
   * @param version job template version
   */
  public Map<String, ParamDefinition> loadRuntimeParams(Step step, String version) {
    var allTemplates = loadAllTemplates(step, version);
    return allTemplates.stream()
        .map(JobTemplate.Definition::getParams)
        .filter(Objects::nonNull)
        .flatMap(p -> p.entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (u, v) -> v, LinkedHashMap::new));
  }

  /**
   * Load runtime parameters based on the step type and its subtype.
   *
   * @return a collection of runtime generated parameters to inject
   * @param step step definition
   * @param version job template version
   */
  public Collection<Tag> loadTags(Step step, String version) {
    var allTemplates = loadAllTemplates(step, version);
    var allTags =
        allTemplates.stream()
            .map(JobTemplate.Definition::getTags)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(Tag::getName, t -> t, (u, v) -> v, LinkedHashMap::new));
    return allTags.values();
  }

  /**
   * Load all related (itself and ancestors) job template definitions based on its subtype and
   * version.
   *
   * @return a collection of related job template definitions
   * @param step step definition
   * @param version job template version
   */
  private List<JobTemplate.Definition> loadAllTemplates(Step step, String version) {
    List<JobTemplate.Definition> parentTemplates = new ArrayList<>();
    var jobTemplateDef = loadJobTemplateSchema(step.getSubType(), version);
    if (jobTemplateDef != null) {
      Checks.checkTrue(
          jobTemplateDef.getStepType() == step.getType(),
          "Job template definition step type [%s] does not match the current step type [%s]",
          jobTemplateDef.getStepType(),
          step.getType());

      if (jobTemplateDef.getInheritFrom() != null) {
        Set<String> visited = new HashSet<>();
        for (var parent : jobTemplateDef.getInheritFrom().entrySet()) {
          fetchParentTemplates(step, parent.getKey(), parent.getValue(), parentTemplates, visited);
        }
      }

      parentTemplates.add(jobTemplateDef);
    }
    return parentTemplates;
  }

  /**
   * Recursively fetch templates from parent job template definitions.
   *
   * @param jobType the job type to fetch its related job templates
   * @param version the version of the job template
   * @param allTemplates List to accumulate all fetched job templates
   * @param visited set to track visited job types for cycle detection
   */
  private void fetchParentTemplates(
      Step step,
      String jobType,
      String version,
      List<JobTemplate.Definition> allTemplates,
      Set<String> visited) {
    Checks.checkTrue(
        !visited.contains(jobType),
        "Cyclic dependency detected for step [%s][%s][%s] when inheriting job type [%s]",
        step.getId(),
        step.getType().name(),
        step.getSubType(),
        jobType);
    visited.add(jobType);

    var jobTemplateDef = loadJobTemplateSchema(jobType, version);
    if (jobTemplateDef != null) {
      if (jobTemplateDef.getInheritFrom() != null) {
        for (var parent : jobTemplateDef.getInheritFrom().entrySet()) {
          fetchParentTemplates(step, parent.getKey(), parent.getValue(), allTemplates, visited);
        }
      }

      allTemplates.add(jobTemplateDef);
    }

    visited.remove(jobType);
  }

  private JobTemplate.Definition loadJobTemplateSchema(String jobType, String version) {
    if (jobType != null) {
      var jobKey = new JobKey(jobType, version);
      Schema schema =
          schemaCache.computeIfAbsent(
              jobKey,
              key -> {
                var jobTemplate = jobTemplateDao.getJobTemplate(jobType, version);
                if (jobTemplate != null) {
                  return new Schema(
                      jobTemplate.getDefinition(),
                      System.currentTimeMillis() + properties.getCacheTtl());
                } else {
                  return null;
                }
              });
      if (schema != null && !schema.isExpired()) {
        return schema.definition();
      } else if (schema != null) {
        schemaCache.remove(jobKey);
        return loadJobTemplateSchema(jobType, version);
      }
    }
    return null;
  }

  /**
   * Merge workflow parameters into job template schema parameters. Only parameters that exist in
   * schema will be merged.
   *
   * @param schemaParams the job template schema parameters to merge into
   * @param workflowParams the workflow parameters to merge from
   */
  public void mergeWorkflowParamsIntoSchemaParams(
      Map<String, ParamDefinition> schemaParams, Map<String, Parameter> workflowParams) {
    if (workflowParams == null || workflowParams.isEmpty()) {
      return;
    }
    Map<String, ParamDefinition> workflowParamDefs =
        workflowParams.keySet().stream()
            .filter(schemaParams::containsKey)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    paramName -> workflowParams.get(paramName).toDefinition()));

    ParamsMergeHelper.mergeParams(
        schemaParams,
        workflowParamDefs,
        new ParamsMergeHelper.MergeContext(ParamSource.WORKFLOW_PARAMETER, false, true, false));
  }
}
