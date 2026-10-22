# CLI reference

All output is English. Structured output uses `--json`. Credentials are never
included. A nonzero exit means failure, an incomplete workflow, or incomplete
verification; resource readiness alone is not end-to-end success.

## Configuration

`init -f tilink.yaml` interactively collects a named connection and discovers
available AWS resources where credentials allow it. Non-interactive users may
write the YAML directly. Unknown fields and inline secrets are rejected.

```yaml
version: 1
name: orders-dm
kind: dm
provider: aws
aws:
  profile: customer
  region: ap-northeast-1
  account_id: "123456789012"
  vpc_id: vpc-example
  subnet_ids:
    - subnet-example-a
    - subnet-example-b
tidb:
  project_id: "project-id"
  cluster_id: "cluster-id"
  public_key_env: TIDB_PUBLIC_KEY
  private_key_env: TIDB_PRIVATE_KEY
  principal_arn: arn:aws:iam::123456789999:root
  az_ids:
    - apne1-az1
target:
  type: rds
  host: orders.example.ap-northeast-1.rds.amazonaws.com
  port: 3306
  security_group_id: sg-database
maintenance: true
probe: true
```

Connection kinds are `app`, `dm`, and `ticdc`. Target types are `mysql`, `rds`,
`aurora`, `kafka`, and `msk`; application connections use the discovered TiDB
endpoint. AWS and TiDB resources must be in compatible regions and AZs.

Optional TiDB fields supply an existing `service_name`, `private_link_id`,
`endpoint_id`, `host`, `port`, or explicit `dataflow_base_url` when a console
step or API capability requires them. `base_url` defaults to the shared v1beta
API. Never select an API host solely from an Essential/Premium label.

For Kafka, configure `target.brokers` with stable `id`, `host`, `port`, and
`listener_port`, plus a bootstrap `target.port`. For MSK, supply
`target.msk_cluster_arn`. Existing SASL/SCRAM authentication, secrets, and ACLs
are prerequisites. Generated broker configuration is published by the operator.

Verification credentials use `verification.username_env` and
`verification.password_env`; optional fields include `database`, `server_name`,
and `ca_file`. Passwords are resolved only when verification is invoked.

## State and generated content

`--state-dir` overrides the default `$XDG_STATE_HOME/tilink`, or
`~/.local/state/tilink` when XDG_STATE_HOME is unset. State is local, owner-only,
and revisioned. Back it up before moving computers. Cloud tags cannot reconstruct
the complete history of changes to existing customer resources.

`show [name]` displays saved configuration, generated files, resource IDs/ARNs,
account/region, ownership, dependencies, observed time, and proposed cleanup.
Without a name it shows every saved connection. It works without cloud credentials.
`--refresh` updates the view from read-only cloud calls and reports unknown or
drifted state instead of pretending the cached data is current.

## Plan, apply, and manual actions

`plan -f FILE` is read-only and reports planned resource and configuration changes.
`apply -f FILE` writes an operation journal before every mutation and resumes from
recorded state. API uncertainty is reconciled before retrying a create. An
unsupported API or a required Kafka configuration change produces `waiting_action`
with the exact next step. Complete the step, update the indicated configuration
fields, and run apply again. Never edit managed resource IDs in the state file.

## Inspection and verification

`status NAME` refreshes resource readiness. `doctor NAME` reports actionable
configuration and network findings. `verify NAME` runs the deployed VPC probe
and available TiDB-side checks. Reports include the probe location and uncovered
checks. Missing credentials or a missing remote test API produce partial
verification. `reconcile NAME` invokes the maintenance function and reports the
actual outcome; it does not start a second concurrent target updater.

## Destroy

`destroy NAME` displays the exact refreshed cleanup plan. Enter `y` at the first
prompt, then enter the complete connection ID at the second prompt. A terminal is
required. There is no force or yes flag. EOF, cancellation, wrong input, or state
changes cause no deletion under the rejected confirmation.

The tool removes all resources it created for the connection, including auxiliary
IAM, logging, scheduling, maintenance, probe, and API-endpoint resources. It
preserves imported resources and only reverses its exact unchanged additions to
existing resources. It does not silently disable a shared MSK configuration.

If cleanup is incomplete, the command exits unsuccessfully, lists every remaining
resource and blocker, and preserves state for retry or manual cleanup. Inspect
with `show --refresh`; retries require two new confirmations. Successfully
destroyed connections retain a sanitized cleanup report for audit.
