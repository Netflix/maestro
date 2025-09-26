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
package com.netflix.maestro.exceptions;

/** Maestro Core exception, includes optional status code. */
public class MaestroRuntimeException extends RuntimeException {
  private static final long serialVersionUID = -5554668492523995454L;

  /** Enum for return status code. */
  public enum Code {
    /** Bad Request. */
    BAD_REQUEST(400),

    /** 401 - Unauthorized. */
    UNAUTHORIZED(401),

    /** 403 - Forbidden. */
    FORBIDDEN(403),

    /** Resource not found. */
    NOT_FOUND(404),

    /** Resource timeout. */
    REQUEST_TIMEOUT(408),

    /** Conflict. */
    CONFLICT(409),

    /** Resource Gone. */
    GONE(410),

    /** Precondition Failed. */
    PRECONDITION_FAILED(412),

    /** Unprocessable Entity. */
    UNPROCESSABLE_ENTITY(422),

    /** Internal Error. */
    INTERNAL_ERROR(500),

    /** Bad Gateway. */
    BAD_GATEWAY(502);

    private final int statusCode;

    /**
     * Constructor with status code.
     *
     * @param statusCode status code of the exception
     */
    Code(int statusCode) {
      this.statusCode = statusCode;
    }

    /**
     * Get status code.
     *
     * @return status code of the exception
     */
    public int getStatusCode() {
      return statusCode;
    }

    /** Static method to convert status code to Code enum object. */
    public static Code create(int statusCode) {
      if (statusCode == BAD_REQUEST.getStatusCode()) {
        return BAD_REQUEST;
      } else if (statusCode == UNAUTHORIZED.getStatusCode()) {
        return UNAUTHORIZED;
      } else if (statusCode == FORBIDDEN.getStatusCode()) {
        return FORBIDDEN;
      } else if (statusCode == NOT_FOUND.getStatusCode()) {
        return NOT_FOUND;
      } else if (statusCode == REQUEST_TIMEOUT.getStatusCode()) {
        return REQUEST_TIMEOUT;
      } else if (statusCode == CONFLICT.getStatusCode()) {
        return CONFLICT;
      } else if (statusCode == GONE.getStatusCode()) {
        return GONE;
      } else if (statusCode == INTERNAL_ERROR.getStatusCode()) {
        return INTERNAL_ERROR;
      } else if (statusCode == BAD_GATEWAY.getStatusCode()) {
        return BAD_GATEWAY;
      } else if (statusCode == UNPROCESSABLE_ENTITY.getStatusCode()) {
        return UNPROCESSABLE_ENTITY;
      } else if (statusCode == PRECONDITION_FAILED.getStatusCode()) {
        return PRECONDITION_FAILED;
      } else {
        throw new MaestroInvalidStatusException("Invalid status code: " + statusCode);
      }
    }
  }

  /** Code enum. */
  private final Code code;

  /**
   * Constructor with message and throwable (defaults to INTERNAL_ERROR).
   *
   * @param msg message to include
   * @param t throwable exception
   */
  public MaestroRuntimeException(String msg, Throwable t) {
    this(Code.INTERNAL_ERROR, msg, t);
  }

  /**
   * Full Constructor.
   *
   * @param code status code of the exception
   * @param msg message to include
   * @param t throwable exception
   */
  public MaestroRuntimeException(Code code, String msg, Throwable t) {
    super(code + " - " + msg, t);
    this.code = code;
  }

  /**
   * Constructor with throwable and code.
   *
   * @param code status code of the exception
   * @param t throwable exception
   */
  public MaestroRuntimeException(Code code, Throwable t) {
    super(code.name(), t);
    this.code = code;
  }

  /**
   * Constructor with code and message.
   *
   * @param code status code of the exception
   * @param message message to include
   */
  public MaestroRuntimeException(Code code, String message) {
    super(message);
    this.code = code;
  }

  /**
   * Return HTTP Status code.
   *
   * @return HTTP response status code
   */
  public int getHttpStatusCode() {
    return this.code.getStatusCode();
  }
}
