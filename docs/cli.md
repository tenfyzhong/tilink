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
`target.msk_cluster_arn`; configure the actual bootstrap `target.host` and
`target.port` before verification. Existing SASL/SCRAM authentication, secrets, and ACLs
are prerequisites. Generated broker configuration is published by the operator.

Verification credentials use `verification.username_env` and
`verification.password_env`; optional fields include `database`, `server_name`,
and `ca_file`. Passwords are resolved only when verification is invoked.
Kafka supports `verification.sasl_mechanism` values `PLAIN`, `SCRAM-SHA-256`,
`SCRAM-SHA-512`, or `NONE`. TLS remains enabled, including server certificate
validation. Use a CA bundle when the server certificate uses a private authority.
API keys default to `TIDB_PUBLIC_KEY` and `TIDB_PRIVATE_KEY` when references are
omitted.

## State and generated content

`--state-dir` overrides the default `$XDG_STATE_HOME/tilink`, or
`~/.local/state/tilink` when XDG_STATE_HOME is unset. State is local, owner-only,
and revisioned. Back it up before moving computers. Cloud tags cannot reconstruct
the complete history of changes to existing customer resources.
A random connection ID is saved before the first cloud mutation and remains
stable across retries. A new read-only plan has no reserved connection ID.
Starting with a different state directory cannot claim resources from a previous
connection merely because its name, account, and region match.

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

AWS can retain Lambda-managed network interfaces after a function disappears.
tilink observes those interfaces and keeps execution roles until AWS releases
them. A bounded wait can end with incomplete cleanup; retry destroy after AWS
finishes. These service-managed interfaces are never claimed or force-deleted
by tilink.

## Resuming console handoffs

For outbound connections without a verified dataflow endpoint, first allow apply
to create the AWS endpoint service. Read its `service_name` from `show`, create
the TiDB-side private connection in the console, then fill `tidb.private_link_id`,
`tidb.endpoint_id` (the AWS `vpce-*` consumer), and `tidb.service_name`. Rerun apply.
The supplied TiDB connection is recorded as reused with operator-supplied,
unverified evidence; AWS verifies the intended consumer and service during
acceptance. Destroy preserves the console-created TiDB connection. Remove that
connection in the console before retrying a provider-service cleanup blocked by it.
For an operator-supplied connection, tilink checks the exact AWS consumer endpoint
before releasing that dependency. Proven consumer absence permits AWS cleanup;
the TiDB record remains preserved with its unverified remote status.

For self-hosted Kafka, also provide the TiDB-reachable domain in `tidb.host` when
automatic domain attachment is unavailable. `show` contains a generated listener
fragment for each broker. Merge each fragment into the broker's existing listener
configuration, keeping its security protocol mapping and inter-broker settings.
Publish/restart using your normal Kafka procedure, set
`target.advertised_listeners_published: true`, then rerun apply and verify. The
acknowledgement records completion of the manual step; it is not network evidence.

MSK cluster-policy changes and TiDB managed connections use the documented console
handoff. The tool validates the existing AWS cluster prerequisites and inventories
the supplied connection as reused. It does not replace a shared cluster policy.

An initially omitted `dataflow_base_url` can be added after AWS-only provisioning,
before the first TiDB resource or attempted operation is recorded. After that,
continue with the chosen
API/console path. Replacing a provider, API origin, account, target, or existing
connection identity is rejected; use a new connection name for a replacement.

Application `tidb.endpoint_id`, when supplied, is the TiDB registration ID, not an
AWS endpoint ID. Outbound `tidb.endpoint_id` is the AWS consumer endpoint ID.

## Verification limits

Automatic DM precheck creation is disabled because the documented DELETE operation
cancels a precheck and retains an audit record. Run the precheck from TiDB Cloud;
the CLI reports the missing TiDB-side evidence as partial verification.
TiCDC sink testing runs through the explicitly configured dataflow API when its
credentials, private link, and supported test operation are available.

With maintenance enabled, initial target registration and later DNS updates run
inside the VPC. With maintenance disabled, the CLI must resolve target hostnames
to private IPv4 addresses, and future address changes require operator action.
No NAT gateway is created. Maintenance needs a working ELB API interface endpoint,
which is created or reused according to `aws.elb_endpoint_id`.

An interrupted untaggable TiDB registration can leave uncertain ownership if the
process exits after sending create but before recording its result. The journal
and inventory remain available in `show`; destruction refuses to guess ownership.
Resolve the registration in the TiDB console, retain the audit state, and use the
recorded IDs for any required manual cleanup. The tool never silently treats an
unresolved operation as successfully destroyed.
