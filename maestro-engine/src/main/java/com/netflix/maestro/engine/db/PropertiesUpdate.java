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
package com.netflix.maestro.engine.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.WorkflowPropertiesUpdateRequest;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

@Data
public class PropertiesUpdate {
  private boolean resetRunStrategyRule;
  private boolean resetWorkflowConcurrency;
  private boolean resetStepConcurrency;
  private Type type;

  public PropertiesUpdate(Type type) {
    this.type = type;
  }

  public PropertiesUpdate(WorkflowPropertiesUpdateRequest request) {
    this.resetRunStrategyRule = request.isResetRunStrategyRule();
    this.resetWorkflowConcurrency = request.isResetWorkflowConcurrency();
    this.resetStepConcurrency = request.isResetStepConcurrency();
    this.type = Type.UPDATE_PROPERTIES;
  }

  public Properties getNewProperties(Properties proposedChange, PropertiesSnapshot prevSnapshot) {
    return type.getNewProperties(proposedChange, prevSnapshot, this);
  }

  /** The update type of workflow properties update. */
  public enum Type {
    /** general update workflow properties. */
    UPDATE_PROPERTIES {
      @Override
      public Properties getNewProperties(
          Properties proposedChange, PropertiesSnapshot prevSnapshot, PropertiesUpdate update) {
        Properties current = prevSnapshot.extractProperties();
        if (update != null) {
          if (update.isResetRunStrategyRule()) {
            proposedChange.setRunStrategy(null);
            current.setRunStrategy(null);
          }
          if (update.isResetWorkflowConcurrency() && !update.isResetRunStrategyRule()) {
            if (current.getRunStrategy() != null
                && current.getRunStrategy().getRule() == RunStrategy.Rule.PARALLEL) {
              proposedChange.setRunStrategy(null);
              current.setRunStrategy(RunStrategy.create(Defaults.DEFAULT_PARALLELISM));
            }
          }
          if (update.isResetStepConcurrency()) {
            proposedChange.setStepConcurrency(null);
            current.setStepConcurrency(null);
          }
        }
        return Properties.merge(proposedChange, current);
      }
    },
    /** add a single workflow tag. */
    ADD_WORKFLOW_TAG {
      @Override
      public Properties getNewProperties(
          Properties proposedChange, PropertiesSnapshot prevSnapshot, PropertiesUpdate update) {
        Checks.checkTrue(
            proposedChange.getTags() != null && proposedChange.getTags().getTags().size() == 1,
            "only a single tag must be present in order to perform add workflow tag.");

        // create a new list of tags;
        TagList currTagList = new TagList(null);

        // merge the previous tags if any;
        TagList prevTagList = prevSnapshot.getTags();
        if (prevTagList == null) {
          currTagList = upsertTag(new ArrayList<>(), proposedChange.getTags().getTags().get(0));
        } else {
          currTagList = upsertTag(prevTagList.getTags(), proposedChange.getTags().getTags().get(0));
        }

        // form a properties change.
        Properties propChanges = new Properties();
        propChanges.setTags(currTagList);

        return UPDATE_PROPERTIES.getNewProperties(propChanges, prevSnapshot, update);
      }

      /** Merge tags to the tag list and override any existing duplicates with new tags. */
      @JsonIgnore
      @VisibleForTesting
      TagList upsertTag(@Nullable final List<Tag> tags, @Nullable final Tag newTag) {
        if (tags == null || newTag == null) {
          return TagList.EMPTY_TAG_LIST;
        }
        final TagList newTagList = new TagList(null);
        boolean replacedExistingTag = false;
        for (final Tag tag : tags) {
          if (newTag.getName().equals(tag.getName())) {
            newTagList.getTags().add(newTag);
            replacedExistingTag = true;
          } else {
            newTagList.getTags().add(tag);
          }
        }
        if (!replacedExistingTag) {
          newTagList.getTags().add(newTag);
        }
        return newTagList;
      }
    },
    /** delete a single workflow tag. */
    DELETE_WORKFLOW_TAG {
      @Override
      public Properties getNewProperties(
          Properties proposedChange, PropertiesSnapshot prevSnapshot, PropertiesUpdate update) {
        Checks.checkTrue(
            proposedChange.getTags() != null && proposedChange.getTags().getTags().size() == 1,
            "only a single tag must be present in order to perform delete workflow tag.");

        Tag workflowTag = proposedChange.getTags().getTags().get(0);
        // remove the tag from the previous workflow tag list if any; otherwise, throw not
        // found exception.
        TagList prevTagList = prevSnapshot.getTags();
        if (prevTagList == null
            || prevTagList.getTags().stream()
                .noneMatch(tag -> tag.getName().equals(workflowTag.getName()))) {
          throw new MaestroNotFoundException(
              "no workflow tag with name [{}] found in the current workflow tags for workflow [{}].",
              workflowTag.getName(),
              prevSnapshot.getWorkflowId());
        }
        TagList currTagList =
            new TagList(
                prevTagList.getTags().stream()
                    .filter(tag -> !tag.getName().equals(workflowTag.getName()))
                    .collect(Collectors.toList()));

        // form a properties change.
        Properties propChanges = new Properties();
        propChanges.setTags(currTagList);
        return UPDATE_PROPERTIES.getNewProperties(propChanges, prevSnapshot, update);
      }
    },
    /** new workflow definition push. */
    ADD_WORKFLOW_DEFINITION {
      @Override
      public Properties getNewProperties(
          Properties newProperties, PropertiesSnapshot prevSnapshot, PropertiesUpdate update) {
        Properties currentProperties = prevSnapshot.extractProperties();

        // If certain properties are undefined in newly pushed wf definition, reset them
        if (newProperties.getRunStrategy() == null) {
          currentProperties.setRunStrategy(null);
        }
        if (newProperties.getStepConcurrency() == null) {
          currentProperties.setStepConcurrency(null);
        }
        if (newProperties.getAlerting() == null) {
          currentProperties.setAlerting(null);
        }
        return Properties.merge(newProperties, currentProperties);
      }
    };

    public abstract Properties getNewProperties(
        Properties newProperties, PropertiesSnapshot prevSnapshot, PropertiesUpdate update);
  }
}
