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
package com.netflix.maestro.server.handlers;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroDryRunException;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.utils.ObjectHelper;
import jakarta.validation.ValidationException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.method.ParameterErrors;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Global exception handler for all REST controllers. */
@Slf4j
@ControllerAdvice
public class MaestroRestExceptionHandler extends ResponseEntityExceptionHandler {
  private static final HttpHeaders EMPTY_HEADER = new HttpHeaders();
  private static final String EMPTY_SPACE = " ";

  @ExceptionHandler({MaestroRuntimeException.class})
  protected ResponseEntity<Object> handleMaestroRuntimeException(
      MaestroRuntimeException e, WebRequest request) {
    LOG.info("Handle [{}] with a message: {}", e.getClass().getSimpleName(), e.getMessage());
    return buildDetailedResponse(e, HttpStatus.valueOf(e.getHttpStatusCode()), request);
  }

  @ExceptionHandler(value = {MaestroDryRunException.class})
  protected ResponseEntity<Object> handleMaestroDryRunException(
      MaestroDryRunException e, WebRequest request) {
    LOG.info("Handle MaestroDryRunException with a message: {}", e.getMessage());
    return buildDetailedResponse(e, e.getDetails().getErrors(), HttpStatus.BAD_REQUEST, request);
  }

  @ExceptionHandler(value = {MaestroBadRequestException.class})
  protected ResponseEntity<Object> handleWorkflowBadRequestException(
      MaestroBadRequestException e, WebRequest request) {
    LOG.info("Handle WorkflowBadRequestException with a message: {}", e.getMessage());
    return buildDetailedResponse(
        e, e.getErrors(), HttpStatus.valueOf(e.getHttpStatusCode()), request);
  }

  @ExceptionHandler(value = {MaestroInvalidStatusException.class})
  protected ResponseEntity<Object> handleMaestroInvalidStatusException(
      MaestroInvalidStatusException e, WebRequest request) {
    LOG.info("Handle MaestroInvalidStatusException with a message: {}", e.getMessage());
    return buildDetailedResponse(
        e, e.getErrors(), HttpStatus.valueOf(e.getHttpStatusCode()), request);
  }

  @ExceptionHandler(value = {MaestroInternalError.class})
  protected ResponseEntity<Object> handleMaestroInternalErrorException(
      MaestroInternalError e, WebRequest request) {
    LOG.info("Handle MaestroInternalError with a message: {}", e.getMessage());
    return buildDetailedResponse(
        e, e.getDetails().getErrors(), HttpStatus.valueOf(e.getHttpStatusCode()), request);
  }

  @ExceptionHandler(
      value = {
        IllegalArgumentException.class,
        NullPointerException.class,
        ValidationException.class
      })
  protected ResponseEntity<Object> handlePreconditionsException(
      RuntimeException e, WebRequest request) {
    LOG.info("Handle Preconditions Exception with a message: {}", e.getMessage());
    return buildDetailedResponse(e, HttpStatus.BAD_REQUEST, request);
  }

  @ExceptionHandler(value = {UnrecognizedPropertyException.class})
  protected ResponseEntity<Object> handleUnrecognizedPropertyException(
      UnrecognizedPropertyException e, WebRequest request) {
    LOG.info("Handle UnrecognizedProperty Exception with a message: {}", e.getMessage());
    return buildDetailedResponse(e, HttpStatus.BAD_REQUEST, request);
  }

  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(
      MethodArgumentNotValidException e,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    LOG.info("Handle MethodArgumentNotValidException with a message: {}", e.getMessage());
    List<String> errors =
        e.getBindingResult().getFieldErrors().stream()
            .map(error -> error.getField() + EMPTY_SPACE + error.getDefaultMessage())
            .toList();
    return buildDetailedResponse(e, errors, headers, request);
  }

  @Override
  protected ResponseEntity<Object> handleHandlerMethodValidationException(
      HandlerMethodValidationException e,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    LOG.info("Handle HandlerMethodValidationException with a message: {}", e.getMessage());
    List<String> errors =
        e.getBeanResults().stream()
            .map(ParameterErrors::getFieldErrors)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .map(error -> error.getField() + EMPTY_SPACE + error.getDefaultMessage())
            .toList();
    return buildDetailedResponse(e, errors, headers, request);
  }

  @Override
  protected ResponseEntity<Object> handleHttpMessageNotReadable(
      HttpMessageNotReadableException e,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    LOG.info("Handle HttpMessageNotReadableException with a message: {}", e.getMessage());
    return buildDetailedResponse(e, Collections.emptyList(), headers, request);
  }

  private ResponseEntity<Object> buildDetailedResponse(
      Exception e,
      List<String> errors,
      HttpStatus status,
      HttpHeaders headers,
      WebRequest request) {
    Details.DetailsBuilder details =
        Details.builder()
            .status(MaestroRuntimeException.Code.create(status.value()))
            .message(e.getMessage());
    if (!ObjectHelper.isCollectionEmptyOrNull(errors)) {
      details.errors(errors);
    }
    return handleExceptionInternal(e, details.build(), headers, status, request);
  }

  private ResponseEntity<Object> buildDetailedResponse(
      Exception e, List<String> errors, HttpHeaders headers, WebRequest request) {
    return buildDetailedResponse(e, errors, HttpStatus.BAD_REQUEST, headers, request);
  }

  private ResponseEntity<Object> buildDetailedResponse(
      Exception e, HttpStatus status, WebRequest request) {
    return buildDetailedResponse(e, Collections.emptyList(), status, EMPTY_HEADER, request);
  }

  private ResponseEntity<Object> buildDetailedResponse(
      Exception e, List<String> errors, HttpStatus status, WebRequest request) {
    return buildDetailedResponse(e, errors, status, EMPTY_HEADER, request);
  }
}
