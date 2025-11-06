# Maestro HTTP Module

The Maestro Http module includes maestro http/https step runtime support to launch a job making an HTTP/HTTPS call.

## Overview

This module enables workflows to make HTTP/HTTPS calls as step executions and save the response in the http artifact and output params.

## Architecture

- **HttpRuntimeExecutor**: Interface for executing HTTP requests
- **JdkHttpRuntimeExecutor**: Default implementation using JDK 11+ HttpClient
- **HttpStepRuntime**: Step runtime that implements HTTP request execution and state management

## Step Parameters

HTTP steps accept the following parameters (see `default-http-step-params.yaml`):

- **http** (required): Map containing request configuration
  - **url** (required): URL for the HTTP request
  - **method** (provided): HTTP method (default: GET)
  - **headers** (provided): String Map of HTTP headers
  - **body** (optional): Request body in String format
- **state_expr** (provided): SEL expression to determine step state based on response

## Output Parameters

HTTP steps produce the following output parameters:

- **status_code**: HTTP response status code (e.g., 200, 404, 500)
- **response_body**: Response body as String format

## Usage Example

See `maestro-server/src/test/resources/samples/yaml/sample-http-wf.yaml` for a complete example.

## Security Considerations

**WARNING**: This module is currently in development and has known security limitations (e.g. SSRF).
**Do not use in production environments accessible to untrusted users** until these issues are addressed.

## Pending Features

- Add URL validation for SSRF vulnerability
- Add response size limits
- Add response content-type handling
- Add metrics
