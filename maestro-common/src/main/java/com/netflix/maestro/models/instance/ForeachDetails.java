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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.utils.Checks;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Foreach iteration details for the inline workflow instances managed by the same foreach step. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@ToString
@EqualsAndHashCode
@Getter
@SuppressWarnings("PMD.LooseCoupling")
public class ForeachDetails {
  @JsonValue @NotNull private final EnumMap<WorkflowInstance.Status, List<Interval>> info;

  /** buffer to hold the updates. */
  @JsonIgnore @NotNull private final EnumMap<WorkflowInstance.Status, List<Interval>> pendingInfo;

  /** constructor and Json deserializer. */
  @JsonCreator
  public ForeachDetails(EnumMap<WorkflowInstance.Status, List<Interval>> input) {
    this.info = input;
    this.pendingInfo = new EnumMap<>(WorkflowInstance.Status.class);
  }

  /** Interval class to define a range, where start and end are inclusive. */
  @EqualsAndHashCode
  @ToString
  @Getter
  @JsonDeserialize(using = IntervalDeserializer.class)
  static class Interval {
    private final long start;
    private long end;

    /** constructor. */
    Interval(long s, long e) {
      this.start = s;
      this.end = e;
    }

    /** Json serializer. */
    @JsonValue
    Object toJson() {
      if (start == end) {
        return start;
      } else {
        return Arrays.asList(start, end);
      }
    }
  }

  /** Deserializer with the validation check. */
  static class IntervalDeserializer extends JsonDeserializer<Interval> {
    @Override
    public Interval deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode value = p.getCodec().readTree(p);
      Checks.checkTrue(
          value.isNumber() || (value.isArray() && value.size() == 2),
          "Interval value only supports Long or Array[2] and not support the type: "
              + value.getClass());
      if (value.isArray()) {
        Iterator<JsonNode> itr = value.elements();
        return new Interval(itr.next().asLong(), itr.next().asLong());
      }
      return new Interval(value.asLong(), value.asLong());
    }
  }

  private static int intervalComparator(Interval i1, Interval i2) {
    return Long.compare(i1.start, i2.start);
  }

  /** Add one instance with its status into the pending buffer. */
  @JsonIgnore
  void add(long instanceId, WorkflowInstance.Status status) {
    if (!pendingInfo.containsKey(status)) {
      pendingInfo.put(status, new ArrayList<>());
    }
    pendingInfo.get(status).add(new Interval(instanceId, instanceId));
  }

  /** Merge the pending info into info to come up with new interval lists. */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  @JsonIgnore
  void refresh() {
    for (WorkflowInstance.Status status : WorkflowInstance.Status.values()) {
      if (!status.isTerminal()) {
        info.remove(status);
      }
      if (pendingInfo.containsKey(status) && !pendingInfo.get(status).isEmpty()) {
        List<Interval> toMerge = pendingInfo.get(status);
        if (status.isTerminal()) {
          toMerge.addAll(info.getOrDefault(status, Collections.emptyList()));
        }
        toMerge.sort(ForeachDetails::intervalComparator);

        List<Interval> merged = new ArrayList<>();
        Iterator<Interval> iterator = toMerge.iterator();
        merged.add(iterator.next());
        while (iterator.hasNext()) {
          Interval interval = iterator.next();
          Interval lastOne = merged.get(merged.size() - 1);
          if (lastOne.end + 1 < interval.start) {
            merged.add(interval);
          } else if (lastOne.end < interval.end) {
            lastOne.end = interval.end;
          }
        }
        info.put(status, merged);
      }
    }
    pendingInfo.clear();
  }

  /** Util helper method to return a flattened list of instance ids derived from intervals. */
  @JsonIgnore
  public Map<WorkflowInstance.Status, List<Long>> flatten(
      Predicate<WorkflowInstance.Status> condition) {
    return info.entrySet().stream()
        .filter(e -> condition.test(e.getKey()))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  List<Long> instanceIds = new ArrayList<>();
                  for (Interval interval : e.getValue()) {
                    for (long id = interval.start; id <= interval.end; ++id) {
                      instanceIds.add(id);
                    }
                  }
                  return instanceIds;
                }));
  }

  @JsonIgnore
  boolean isForeachIterationRestartable(long iterationId) {
    return info.entrySet().stream()
        .filter(e -> (e.getKey().isTerminal()))
        .anyMatch(
            e -> {
              for (Interval interval : e.getValue()) {
                if (iterationId >= interval.start && iterationId <= interval.end) {
                  return true;
                }
              }
              return false;
            });
  }

  @JsonIgnore
  void resetIterationDetail(
      long iterationId, WorkflowInstance.Status newStatus, WorkflowInstance.Status oldStatus) {
    Checks.checkTrue(
        info.get(oldStatus) != null,
        "Invalid: the restarted iteration [%s]'s status [%s] is missing in the foreach details",
        iterationId,
        oldStatus);
    Interval deleted = null;
    Iterator<Interval> iter = info.get(oldStatus).iterator();
    while (iter.hasNext()) {
      Interval interval = iter.next();
      if (iterationId >= interval.start && iterationId <= interval.end) {
        deleted = interval;
        iter.remove();
        break;
      }
    }
    Checks.checkTrue(
        deleted != null,
        "Invalid: the restarted iteration [%s] is missing in the foreach details",
        iterationId);

    if (deleted.start < iterationId) {
      info.get(oldStatus).add(new Interval(deleted.start, iterationId - 1));
    }
    if (iterationId < deleted.end) {
      info.get(oldStatus).add(new Interval(iterationId + 1, deleted.end));
    }
    if (info.get(oldStatus).isEmpty()) {
      info.remove(oldStatus);
    }
    add(iterationId, newStatus);
  }
}
