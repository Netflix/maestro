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
package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.api.TagPermitRequest;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Controller class for tag permit operations. */
@Tag(name = "/api/v3/tag-permits", description = "Maestro TagPermit APIs")
@RestController
@RequestMapping(
    value = "/api/v3/tag-permits",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class TagPermitController {
  private final TagPermitManager tagPermitManager;
  private final User.UserBuilder callerBuilder;

  /** Constructor. */
  @Autowired
  public TagPermitController(TagPermitManager tagPermitManager, User.UserBuilder callerBuilder) {
    this.tagPermitManager = tagPermitManager;
    this.callerBuilder = callerBuilder;
  }

  /**
   * Set or Update a tag permit with the given max_allowed value.
   *
   * @param tagPermitRequest tag permit request
   */
  @PostMapping(value = "")
  @Operation(summary = "Create or update a tag permit.")
  public ResponseEntity<?> upsertTagPermit(@Valid @RequestBody TagPermitRequest tagPermitRequest) {
    tagPermitManager.upsertTagPermit(
        tagPermitRequest.getTag(), tagPermitRequest.getMaxAllowed(), callerBuilder.build());
    return ResponseEntity.ok().build();
  }

  /**
   * Fetch an existing tag permit.
   *
   * @param tag tag name
   * @return the tag permit
   */
  @GetMapping(value = "/{tag}", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Fetches an existing tag permit.")
  public TagPermit getTagPermit(@Valid @NotNull @PathVariable("tag") String tag) {
    var tp = tagPermitManager.getTagPermit(tag);
    if (tp == null) {
      throw new MaestroNotFoundException("No tag permit found for tag " + tag);
    }
    return tp;
  }

  /**
   * Remove an existing tag permit.
   *
   * @param tag tag name
   */
  @DeleteMapping(value = "/{tag}", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Removes an existing tag permit.")
  public ResponseEntity<?> removeTagPermit(@Valid @NotNull @PathVariable("tag") String tag) {
    tagPermitManager.removeTagPermit(tag);
    return ResponseEntity.ok().build();
  }
}
