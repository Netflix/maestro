# Maestro HTTP Module

The Maestro Http module includes maestro http/https step runtime support to launch a job making an HTTP/HTTPS call.

## Overview

This module enables workflows to make HTTP/HTTPS calls as step executions and save the response in the http artifact and output params.

## Architecture

- **HttpRuntimeExecutor**: Interface for executing HTTP requests
- **JdkHttpRuntimeExecutor**: Default implementation using JDK 11+ HttpClient
- **UrlValidator**: Validates URLs against allow-list to prevent SSRF attacks
- **SizeBoundedBodyHandler**: HTTP response body handler that enforces size limits to prevent OOM and DoS attacks
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

## Security

### SSRF Protection

This module implements comprehensive Server-Side Request Forgery (SSRF) protection using an allow-list approach:

- **Allow-list based validation**: Only URLs with hostnames in the configured allow-list are permitted
- **Scheme validation**: Only HTTP and HTTPS protocols are allowed
- **Case-insensitive matching**: Hostnames are normalized to lowercase for consistent validation

### Response Size Limiting

To prevent Out-of-Memory (OOM) errors and Denial-of-Service (DoS) attacks, the module enforces a configurable maximum response size:

- **Content-Length header validation**: Rejects responses if Content-Length exceeds the limit
- **Stream-based enforcement**: The response body is limited during streaming

### Configuration

Configure HTTP security settings in your `application.yml`:

```yaml
stepruntime:
  http:
    connection-timeout: 30000      # Connection timeout in milliseconds
    send-timeout: 30000            # Send timeout in milliseconds
    max-response-size: 1048576     # Maximum response size in bytes (1 MB)
    allow-list: []                 # List of allowed hostnames
```

**Important**: By default, the allow-list is empty, which blocks all HTTP requests.
You must configure allowed hostnames before HTTP steps can execute.

## Pending Features

- Add response content-type handling
- Add metrics
