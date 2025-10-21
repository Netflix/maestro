package com.netflix.maestro.dsl.parsers;

import com.netflix.maestro.dsl.Dag;
import com.netflix.maestro.dsl.DslWorkflow;
import com.netflix.maestro.dsl.DslWorkflowDef;
import com.netflix.maestro.dsl.jobs.BaseJob;
import com.netflix.maestro.dsl.jobs.Foreach;
import com.netflix.maestro.dsl.jobs.Job;
import com.netflix.maestro.dsl.jobs.Subworkflow;
import com.netflix.maestro.dsl.jobs.TypedJob;
import com.netflix.maestro.dsl.jobs.While;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Criticality;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.SubworkflowStep;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.WhileStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.parameter.LongArrayParamDefinition;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Convert DSL workflow definitions to Maestro common data model. */
public class WorkflowParser {
  private static final Map<String, StepType> KNOWN_STEP_TYPES =
      Stream.of(StepType.values())
          .collect(Collectors.toUnmodifiableMap(t -> t.getType().toUpperCase(Locale.US), t -> t));
  private static final String RANGE_EXPR_TEMPLATE = "Util.intsBetween(%s, %s, %s);";
  private static final String SUBWORKFLOW_ID_PARAM_NAME = StepType.SUBWORKFLOW.getType() + "_id";
  private static final String SUBWORKFLOW_VERSION_PARAM_NAME =
      StepType.SUBWORKFLOW.getType() + "_version";
  private static final String SEQUENTIAL_DAG_ORDER = "SEQUENTIAL";
  private static final String PARALLEL_DAG_ORDER = "PARALLEL";

  private final Function<String, StepType> stepTypeResolver;

  public WorkflowParser(Function<String, StepType> stepTypeResolver) {
    this.stepTypeResolver = Checks.notNull(stepTypeResolver, "stepTypeResolver cannot be null");
  }

  /**
   * Convert a DSL WorkflowDefinition to a WorkflowCreateRequest.
   *
   * @param dslWorkflowDef the DSL workflow definition
   * @return the WorkflowCreateRequest for the Maestro API
   */
  public WorkflowCreateRequest toWorkflowCreateRequest(DslWorkflowDef dslWorkflowDef) {
    Checks.notNull(dslWorkflowDef, "workflowDefinition cannot be null");
    Checks.notNull(dslWorkflowDef.workflow(), "workflow cannot be null");

    DslWorkflow dslWorkflow = dslWorkflowDef.workflow();

    WorkflowCreateRequest request = new WorkflowCreateRequest();
    request.setProperties(buildProperties(dslWorkflow));
    request.setWorkflow(buildWorkflow(dslWorkflow));
    request.setIsActive(Defaults.DEFAULT_WORKFLOW_ACTIVE_FLAG);

    return request;
  }

  /**
   * Build Properties from DSL Workflow.
   *
   * @param dslWorkflow the DSL workflow
   * @return Properties instance
   */
  private Properties buildProperties(DslWorkflow dslWorkflow) {
    Properties properties = new Properties();

    if (dslWorkflow.getRunStrategy() != null) {
      RunStrategy.Rule rule = RunStrategy.Rule.create(dslWorkflow.getRunStrategy());
      properties.setRunStrategy(new RunStrategy(rule, dslWorkflow.getWorkflowConcurrency()));
    } else {
      properties.setRunStrategy(Defaults.DEFAULT_RUN_STRATEGY);
    }

    properties.setStepConcurrency(Defaults.DEFAULT_STEP_CONCURRENCY);
    properties.setDescription(dslWorkflow.getDescription());
    properties.setTags(convertToTagList(dslWorkflow.getTags()));

    return properties;
  }

  /**
   * Convert a List of tag name strings to a TagList.
   *
   * @param tagNames the list of tag name strings
   * @return the TagList
   */
  private TagList convertToTagList(List<String> tagNames) {
    if (tagNames == null) {
      return null;
    } else if (tagNames.isEmpty()) {
      return TagList.EMPTY_TAG_LIST;
    }
    return new TagList(tagNames.stream().map(Tag::create).collect(Collectors.toList()));
  }

  /**
   * Build common model Workflow from DSL Workflow.
   *
   * @param dslWorkflow the DSL workflow
   * @return common model Workflow
   */
  private Workflow buildWorkflow(DslWorkflow dslWorkflow) {
    var builder = Workflow.builder();
    builder.id(dslWorkflow.getId());
    builder.name(dslWorkflow.getName());
    builder.description(dslWorkflow.getDescription());
    builder.tags(convertToTagList(dslWorkflow.getTags()));
    if (dslWorkflow.getTimeout() != null) {
      builder.timeout(ParsableLong.of(dslWorkflow.getTimeout()));
    }
    if (dslWorkflow.getCriticality() != null) {
      builder.criticality(Criticality.create(dslWorkflow.getCriticality()));
    }
    builder.params(ParamParser.parse(dslWorkflow.getWorkflowParams()));
    if (dslWorkflow.getDag() != null) {
      addTransitionsToJobs(dslWorkflow.getJobs(), dslWorkflow.getDag());
    }
    builder.steps(convertJobsToSteps(dslWorkflow.getJobs()));

    return builder.build();
  }

  /**
   * Add transition info from dag field to jobs if not already present.
   *
   * @param jobs a list jobs
   * @param dag the dag info
   */
  private void addTransitionsToJobs(List<Job> jobs, Dag dag) {
    if (jobs == null || dag == null) {
      return;
    }

    if (dag.order() != null) {
      if (SEQUENTIAL_DAG_ORDER.equalsIgnoreCase(dag.order())) {
        for (int i = 0; i < jobs.size() - 1; i++) {
          var cur = jobs.get(i);
          String toJobId = jobs.get(i + 1).getId();
          if (ObjectHelper.isCollectionEmptyOrNull(cur.getTransition())) {
            cur.setTransition(List.of(toJobId));
          }
          // if job already has transitions, we do not override them
        }
      } else if (!PARALLEL_DAG_ORDER.equalsIgnoreCase(dag.order())) {
        throw new MaestroValidationException(
            "Invalid DAG order: "
                + dag.order()
                + ". Supported orders are SEQUENTIAL and PARALLEL.");
      }
      // if PARALLEL order, do nothing as no transitions are needed
    } else if (dag.transitions() != null) {
      var jobMap = jobs.stream().collect(Collectors.toMap(Job::getId, j -> j));
      for (var entry : dag.transitions().entrySet()) {
        var cur = jobMap.get(entry.getKey());
        Checks.checkTrue(
            cur != null, "DAG transition refers to an unknown job id: [%s]", entry.getKey());
        if (ObjectHelper.isCollectionEmptyOrNull(cur.getTransition())) {
          cur.setTransition(entry.getValue());
        }
        // if job already has transitions, we do not override them
      }
    }
  }

  /**
   * Convert a list of DSL Jobs to a list of common model Steps.
   *
   * @param jobs the list of DSL jobs
   * @return list of Steps
   */
  private List<Step> convertJobsToSteps(List<Job> jobs) {
    if (jobs == null) {
      return null;
    } else if (jobs.isEmpty()) {
      return List.of();
    }
    return jobs.stream().map(this::convertJobToStep).toList();
  }

  /**
   * Convert a single DSL Job to a common model Step.
   *
   * @param job the DSL job
   * @return the corresponding Step
   */
  private Step convertJobToStep(Job job) {
    return switch (job) {
      case TypedJob typedJob -> convertTypedJob(typedJob);
      case Subworkflow subworkflow -> convertSubworkflow(subworkflow);
      case Foreach foreach -> convertForeach(foreach);
      case While whileJob -> convertWhile(whileJob);
      default ->
          throw new MaestroValidationException("Unknown job type: " + job.getClass().getName());
    };
  }

  /**
   * Convert a TypedJob to a TypedStep.
   *
   * @param typedJob the DSL typed job
   * @return the TypedStep
   */
  private TypedStep convertTypedJob(TypedJob typedJob) {
    TypedStep step = new TypedStep();
    fillBaseStepFieldsFromJob(step, typedJob);

    Checks.notNull(typedJob.getType(), "type cannot be null for job id: [%s]", typedJob.getId());

    String typeStr = typedJob.getType().toUpperCase(Locale.US);
    String subType = null;
    StepType stepType = KNOWN_STEP_TYPES.get(typeStr);

    if (stepType == null) {
      subType = typedJob.getType();
      stepType = stepTypeResolver.apply(subType); // get sub_type's step type
    }

    Checks.notNull(
        stepType,
        "cannot find step type for job id: [%s] with job type: [%s]",
        typedJob.getId(),
        typedJob.getType());

    step.setType(stepType);
    step.setSubType(subType);

    return step;
  }

  /**
   * Populate common base step fields from a DSL job.
   *
   * @param step the target Step
   * @param baseJob the source base job
   */
  private void fillBaseStepFieldsFromJob(AbstractStep step, BaseJob baseJob) {
    step.setId(baseJob.getId());
    step.setName(baseJob.getName());
    step.setDescription(baseJob.getDescription());
    step.setFailureMode(baseJob.getFailureMode());
    step.setTags(convertToTagList(baseJob.getTags()));
    if (baseJob.getTimeout() != null) {
      step.setTimeout(ParsableLong.of(baseJob.getTimeout()));
    }
    step.setParams(ParamParser.parse(baseJob.getJobParams()));
    if (!ObjectHelper.isCollectionEmptyOrNull(baseJob.getTransition())) {
      step.setTransition(TransitionParser.parse(baseJob.getTransition()));
    }
  }

  /**
   * Convert a Subworkflow job to a SubworkflowStep.
   *
   * @param subworkflow the DSL subworkflow job
   * @return the SubworkflowStep
   */
  private SubworkflowStep convertSubworkflow(Subworkflow subworkflow) {
    SubworkflowStep step = new SubworkflowStep();
    fillBaseStepFieldsFromJob(step, subworkflow);

    step.setSync(subworkflow.getSync());
    step.setExplicitParams(subworkflow.getExplicitParams());
    Map<String, ParamDefinition> params = step.getParams();
    if (params == null) {
      params = new LinkedHashMap<>();
      step.setParams(params);
    }

    String subworkflowId =
        ObjectHelper.valueOrDefault(subworkflow.getWorkflowId(), subworkflow.getId());
    params.put(
        SUBWORKFLOW_ID_PARAM_NAME,
        ParamDefinition.buildParamDefinition(SUBWORKFLOW_ID_PARAM_NAME, subworkflowId));
    if (subworkflow.getVersion() != null) { // Add version to params if specified
      params.put(
          SUBWORKFLOW_VERSION_PARAM_NAME,
          ParamDefinition.buildParamDefinition(
              SUBWORKFLOW_VERSION_PARAM_NAME, subworkflow.getVersion()));
    }

    return step;
  }

  /**
   * Convert a Foreach job to a ForeachStep.
   *
   * @param foreach the DSL foreach job
   * @return the ForeachStep
   */
  private ForeachStep convertForeach(Foreach foreach) {
    ForeachStep step = new ForeachStep();
    fillBaseStepFieldsFromJob(step, foreach);

    step.setConcurrency(foreach.getConcurrency());
    if (foreach.getDag() != null) {
      addTransitionsToJobs(foreach.getJobs(), foreach.getDag());
    }
    step.setSteps(convertJobsToSteps(foreach.getJobs())); // Convert nested jobs to steps
    Map<String, ParamDefinition> params = step.getParams();
    if (params == null) {
      params = new LinkedHashMap<>();
      step.setParams(params);
    }

    Map<String, ParamDefinition> loopParamsMap;
    if (foreach.getLoopParams() != null) {
      loopParamsMap = ParamParser.parse(foreach.getLoopParams());
    } else {
      loopParamsMap = new LinkedHashMap<>();
    }
    if (foreach.getRanges() != null) {
      for (Map.Entry<String, Foreach.Range> entry : foreach.getRanges().entrySet()) {
        String expr =
            String.format(
                RANGE_EXPR_TEMPLATE,
                ObjectHelper.valueOrDefault(entry.getValue().from(), 0),
                entry.getValue().to(),
                ObjectHelper.valueOrDefault(entry.getValue().increment(), 1));
        loopParamsMap.put(
            entry.getKey(),
            LongArrayParamDefinition.builder().name(entry.getKey()).expression(expr).build());
      }
    }
    params.put(
        Constants.LOOP_PARAMS_NAME,
        MapParamDefinition.builder().name(Constants.LOOP_PARAMS_NAME).value(loopParamsMap).build());

    return step;
  }

  /**
   * Convert a While job to a WhileStep.
   *
   * @param whileJob the DSL while job
   * @return the WhileStep
   */
  private WhileStep convertWhile(While whileJob) {
    WhileStep step = new WhileStep();
    fillBaseStepFieldsFromJob(step, whileJob);

    step.setCondition(whileJob.getCondition());
    if (whileJob.getDag() != null) {
      addTransitionsToJobs(whileJob.getJobs(), whileJob.getDag());
    }
    step.setSteps(convertJobsToSteps(whileJob.getJobs())); // Convert nested jobs to steps
    Map<String, ParamDefinition> params = step.getParams();
    if (params == null) {
      params = new LinkedHashMap<>();
      step.setParams(params);
    }

    if (whileJob.getLoopParams() != null) {
      Map<String, ParamDefinition> loopParamsMap = ParamParser.parse(whileJob.getLoopParams());
      params.put(
          Constants.LOOP_PARAMS_NAME,
          MapParamDefinition.builder()
              .name(Constants.LOOP_PARAMS_NAME)
              .value(loopParamsMap)
              .build());
    }

    return step;
  }
}
