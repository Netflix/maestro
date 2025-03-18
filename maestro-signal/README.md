Maestro Signal
===================================
This module includes the implementation of Maestro signal functions. 
It supports both signal triggers, signal dependencies, and output signals.
The workflow can be triggered by a specific set of signals with join key support.
Workflow steps can wait for signal dependencies to be ready, where the condition supports comparison operators.
After the execution, a step can also emit an output signal to trigger another workflow execution or unblock a step.

# Get started

## Create a sample workflow
- `curl --header "user: tester" -X POST 'http://127.0.0.1:8080/api/v3/workflows' -H "Content-Type: application/json" -d @maestro-server/src/test/resources/samples/sample-signal-trigger-wf.json`

## Trigger the sample workflow over signals
- `curl -X POST 'http://127.0.0.1:8080/api/v3/signals' -H "Content-Type: application/json" -d '{"name": "signal_a", "params": {"foo": "bat", "watermark": 123456, "updated_by": "tester"}}'`
- `curl -X POST 'http://127.0.0.1:8080/api/v3/signals' -H "Content-Type: application/json" -d '{"name": "signal_b", "params": {"bar": "baz", "watermark": 123456, "posted_by": "tester"}}'`

## Get the signal instances
- `curl -X GET 'http://127.0.0.1:8080/api/v3/signals/signal_a/instances/1'`
- `curl -X GET 'http://127.0.0.1:8080/api/v3/signals/signal_b/instances/latest'`


Then a workflow instance will be started, which we can get its information over the below API.

## Get the sample workflow instance
- `curl -X GET 'http://127.0.0.1:8080/api/v3/workflows/sample-signal-trigger-wf/instances/1/runs/1'`

Then once job.1 started, it will be paused in the waiting for signal status, which we can get its information over the below API.

## Get the sample workflow step attempt
- `curl -X GET 'http://127.0.0.1:8080/api/v3/workflows/sample-signal-trigger-wf/instances/1/runs/1/steps/job.1/attempts/1'`

We then can send the signals to unblock the step over the below API.

## Unblock the step
- `curl -X POST 'http://127.0.0.1:8080/api/v3/signals' -H "Content-Type: application/json" -d '{"name": "db/test/table1", "params": {"vtts_utc_dateint": "20500101", "vtts_utc_hour": "00"}}'`
- `curl -X POST 'http://127.0.0.1:8080/api/v3/signals' -H "Content-Type: application/json" -d '{"name": "db/test/table2", "params": {"vtts_utc_dateint": "20500101", "vtts_utc_hour": "00"}}'`

Once the job.1 is unblocked and executed, it will then send two output signals to Maestro, which can be consumed by other workflows.
For example, it can trigger another round of execution of itself to achieve forever running execution mode.

We can see the output signal information in the step info over the below API.

## Get the sample workflow step attempt
- `curl -X GET 'http://127.0.0.1:8080/api/v3/workflows/sample-signal-trigger-wf/instances/1/runs/1/steps/job.1/attempts/1'`
