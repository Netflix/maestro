# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Maestro is Netflix's general-purpose workflow orchestrator (workflow-as-a-service). This is a
multi-module Gradle project (Java 21, Spring Boot 3, PostgreSQL). All modules share the
`com.netflix.maestro` package namespace and the `com.netflix.maestro` Gradle group.

## Build & Run

```bash
./gradlew build                  # compile + test + lint all modules
./gradlew bootRun                # run maestro-server on :8080 (in-memory/testcontainer Postgres by default)
./gradlew bootRun --args='--spring.profiles.active=aws'   # run with AWS module (needs LocalStack)
docker compose -f maestro-aws/docker-compose.yml up -d    # LocalStack (SQS/SNS) + supporting infra
```

Two deployable Spring Boot services exist:
- **maestro-server** (`MaestroApp`, port 8080) — the main orchestrator; bundles every module. It is
  the only module *not* built as a `java-library` (it is the application).
- **maestro-extensions** (port 8081) — a separate event-driven service consuming `maestro-event` SQS
  (subscribed to maestro-server's SNS topic), e.g. foreach flattening. Run with
  `./gradlew :maestro-extensions:bootRun`.

The REST API is versioned under `/api/v3/...`. Requests that mutate require a `user:` header.
See `README.md` for end-to-end curl examples (create/start/query/delete a sample workflow).

## Testing

```bash
./gradlew test                                              # all tests
./gradlew :maestro-engine:test                              # one module
./gradlew :maestro-engine:test --tests "*WorkflowRunnerTest"   # single test class
```

Tests run on JUnit Platform but most are **JUnit 4** (`junit:4.13.2`) executed via the vintage
engine. DB-backed tests use **Testcontainers** against PostgreSQL — the default JDBC URL is
`jdbc:tc:postgresql:17:///maestro_local`, so Docker must be running. There is no separate DB
to provision locally.

## Linting & Formatting

```bash
./gradlew spotlessApply    # auto-format (google-java-format) — run before committing
./gradlew spotlessCheck checkstyleMain pmdMain
```

- Formatting is **google-java-format** via Spotless (also strips unused imports). CI/build fails on
  unformatted code.
- Checkstyle runs on `src/main/java` only (not tests); PMD likewise skips test sources; SpotBugs is
  configured with `ignoreFailures = true`.
- `netflix-sel` is exempt from checkstyle/pmd/spotbugs.
- Lombok is used project-wide; the generated logger field is named `LOG` (not `log`) —
  use `LOG` in `@Slf4j` classes.

## Architecture

Modules are layered; arrows are compile dependencies (`settings.gradle` lists them all):

```
maestro-common   shared domain models, DTOs, params, exceptions, validations (no maestro deps)
netflix-sel      sandboxed expression language (SEL) for evaluating workflow params (standalone)
maestro-database DB access layer (HikariCP + Flyway)
maestro-queue    Postgres-backed internal job-event queue + worker
maestro-flow     internal actor-based flow engine
maestro-engine   bridges Maestro domain ↔ flow engine; step runtime, DAOs, handlers, processors
maestro-dsl / -signal / -timetrigger     workflow DSL and trigger subsystems
maestro-aws / -kubernetes / -http        pluggable integration/step-runtime modules
maestro-server   REST controllers + Spring Boot bootstrap (depends on everything)
```

Key flow of control:

- **Flow engine (`maestro-flow`)** is an actor model. `FlowExecutor` (singleton, thread-safe) claims
  ownership of flow groups and drives a hierarchy of actors: `GroupActor` → `FlowActor` →
  `TaskActor` (see `flow/actor/`). This is the rewritten high-throughput engine.
- **`MaestroTask` (`maestro-engine/tasks`)** is the proxy bridging the generic flow engine to Maestro
  domain logic. It translates internal flow data into Maestro models, invokes the appropriate step
  runtime, and handles at-least-once step-status publishing and persistence. Step runtimes stay
  independent of the flow engine through this boundary.
- **Job events** are written to the Postgres queue (`maestro-queue`), polled by `MaestroQueueWorker`,
  and dispatched to processors in `maestro-engine/processors` (start/terminate/update/notification/
  delete/instance-action). Queue ids and per-queue worker counts are configured under `maestro.queue`
  in `application.yml`.
- **Action handlers** (`maestro-engine/handlers`) implement workflow/step lifecycle operations
  invoked by the controllers (`WorkflowActionHandler`, `StepInstanceActionHandler`, `WorkflowRunner`).

### Database migrations

Flyway migrations live **per-module** under `src/main/resources/db/migration/postgres/` (e.g.
`maestro-engine`, `maestro-flow`, `maestro-queue`, `maestro-signal`, `maestro-extensions`). They are
merged at runtime. Migration files are named `V<yyyyMMddHHmm>__description.sql`.

### Configuration

`maestro-server/src/main/resources/application.yml` is the canonical config (and `application-aws.yml`
for the `aws` profile). It controls queue worker counts, SEL sandbox limits (`maestro.sel.*`: stack/
loop/array/memory limits + timeout), engine DB/pool settings, step-runtime callback delays, tag
permits, and trigger types. Notifier/alerting/redis default to `noop`/disabled.

### SEL (netflix-sel)

A sandboxed expression language used to evaluate dynamic workflow parameters. It implements a subset
of the Java Language Specification with security permission controls and runtime guards (loop/array/
memory limits enforced via the `maestro.sel.*` config). Only a curated set of classes/functions is
callable. See `netflix-sel/README.md` and `netflix-sel/docs/lang-guide/`.

## Dependencies

Versioned dependency aliases are centralized in `dependencies.gradle` (referenced as `lombokDep`,
`jacksonDatabindDep`, etc.). Gradle **dependency locking is enabled** for all configurations — after
changing dependencies, regenerate locks (e.g. `./gradlew dependencies --write-locks` /
`--update-locks`) and commit the updated `gradle.lockfile`s.
