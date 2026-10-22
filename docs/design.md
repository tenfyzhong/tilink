# Design and acceptance contract

This document precedes implementation. Changes to behavior must update this
contract and its reusable tests together.

## Scope

The first version manages AWS networking for applications, DM sources, and TiCDC
sinks. It supports Essential and Premium through one TiDB Cloud client. Product
names do not select different API implementations. The client distinguishes
authorization failures, missing resources, and unverified operations.

The tool manages existing database and Kafka connectivity, not database creation,
data replication jobs, Kafka topics, ACLs, or broker rolling restarts. Self-hosted
Kafka listener configuration is generated for the operator to deploy.

## Components and contracts

- `internal/model`: versioned configuration, resources, plans, findings, and state.
- `internal/state`: atomic owner-only JSON storage, revision checks, and locks.
- `internal/engine`: resumable apply, read-only refresh, and guarded destruction.
- `internal/tidb`: shared authenticated API transport and typed operations.
- `internal/aws`: SDK adapters, resource discovery, planning, apply, and cleanup.
- `internal/worker`: reconciliation and network probes, independent of the CLI.
- `internal/cli`: interactive input, English output, JSON, and command dispatch.

The Go module is `github.com/tenfyzhong/tilink`. The common model defines `Config`,
`Resource`, `Step`, `Plan`, `State`, `Finding`, and `Verification`. Configuration
uses snake_case YAML/JSON fields. Resource properties contain non-secret metadata
only. A resource has a stable logical key, cloud ID, provider, kind, ownership
(`created`, `reused`, or `modified`), dependencies, status, operation ID, and
properties. A step has a key, provider, resource kind, action, dependencies,
properties, and optional manual instructions.

An engine provider exposes `Plan(ctx, Config) ([]Step, error)`,
`Apply(ctx, Config, Step, []Resource) (Resource, error)`,
`Refresh(ctx, Config, Resource) (Resource, error)`, and
`Delete(ctx, Config, Resource) error`. Apply must recover an ambiguous prior
operation before creating again. Delete must prove resource ownership and
account/region identity. Reused resources never reach Delete. Provider failures
must not include credentials or raw sensitive request/response bodies.

The engine persists an operation before calling a provider, then immediately
persists returned resource IDs. Stable connection and operation IDs are included
in AWS tags wherever supported. Untaggable children are anchored to recorded,
owned parent resources. Unknown ownership is a blocker, not permission to adopt.
Only resources with a known cleanup operation may be created automatically.

Configuration changes must not orphan resources. Unsupported replacement or
immutable changes are rejected with an explanation. Incremental configuration
changes on external resources record exact rule IDs or policy statement Sids;
cleanup never restores an entire old policy snapshot over customer changes.

## TiDB API evidence

The common reference is [TiDB Cloud API v1beta](https://docs.pingcap.com/tidbcloud/api/v1beta/)
and its [OpenAPI specification](https://docs-download.pingcap.com/api/tidbcloud-oas.json).
The base endpoint is `https://api.tidbcloud.com/api/v1beta`. API keys use HTTP
Digest authentication. Credentials must not be forwarded to another origin.

Under `/projects/{project_id}/clusters/{cluster_id}` the reference documents:

| Operation | Endpoint | Cleanup |
| --- | --- | --- |
| Read endpoint service | `GET /private_endpoint_service` | Read only |
| Create endpoint service | `POST /private_endpoint_service` | No delete operation in this reference; do not auto-create |
| List endpoint registrations | `GET /private_endpoints` | Read only |
| Register AWS endpoint | `POST /private_endpoints` with `endpoint_name` | `DELETE /private_endpoints/{endpoint_id}` |

The delete parameter is TiDB's internal endpoint ID, not the AWS `vpce-*` ID.
Endpoint service responses include service name, DNS name, port, AZ IDs, provider,
and status. No tier-based rejection is inferred from this legacy specification.

The reference omits outbound PrivateLink, migration precheck, and changefeed test
operations. Additional verified contracts are available in the official CLI:

- [PrivateLink](https://github.com/tidbcloud/tidbcloud-cli/blob/main/pkg/tidbcloud/v1beta1/serverless/privatelink.swagger.json)
- [DM prechecks](https://github.com/tidbcloud/tidbcloud-cli/blob/main/pkg/tidbcloud/v1beta1/serverless/dm.swagger.json)
- [Changefeed tests](https://github.com/tidbcloud/tidbcloud-cli/blob/main/pkg/tidbcloud/v1beta1/serverless/cdc.swagger.json)

These operations remain in the same client, with an explicitly configured
dataflow API endpoint. Unsupported or unverified operations produce a manual
action with exact fields to return. A 403 is not an unsupported capability, and
a 404 alone does not distinguish an unavailable operation from a missing object.
Do not create a service merely to probe API support.

## AWS topology

Application connections create an interface endpoint, explicit security groups,
and a TiDB registration. Private DNS is enabled only after registration is ready.
Discovery checks the service's AZ IDs against selected subnet AZ IDs, not AZ names
from different accounts. The TiDB service must already exist.

Database and self-hosted Kafka connections use internal NLBs with security
groups, TCP listeners, IP target groups, endpoint services, specific allowed
principals, and explicit acceptance of the intended consumer endpoint. Database
SG access references the NLB security group. No blanket public access is added.
Cross-zone forwarding is enabled for single-writer targets. Database protocol TLS
is passed through, preserving the expected server name and certificate checks.

Kafka has a bootstrap target group and a separate listener/target group for every
broker. Generated advertised listeners must use TiDB-reachable hostnames and
ports. Bootstrap success alone does not validate every broker.

MSK Provisioned uses managed multi-VPC connectivity and SASL/SCRAM. Validate its
region, AZ IDs, supported broker/version, SCRAM configuration, and policy. It
does not use the self-hosted NLB reconciler. Existing customer authentication and
ACLs are prerequisites. Mutations without safe granular rollback become explicit
manual actions instead of untracked updates to shared MSK settings.

## Lambda maintenance and probes

Go worker binaries are built for Linux and packaged into the CLI release. The
SDK deploys functions, least-privilege roles, log groups (14-day retention),
EventBridge rules, permissions, and VPC attachments. Every resource is inventoried.
Reconciliation runs every minute with reserved concurrency one and a bounded
invocation timeout. Manual reconcile invokes the same function.

For database writer changes, populate the inactive target group, verify new
targets and stable DNS, switch the listener, then drain old targets. Never place
old and new writer addresses together in the active forwarding group. DNS errors,
empty or non-private results, and unhealthy replacements preserve existing
forwarding and produce a finding. Kafka updates preserve broker identity; topology
changes require a configuration update. There is no zero-downtime promise.

Functions run in selected VPC subnets. Reuse a working ELB API interface endpoint
or explicitly plan its creation with a dedicated security group. Do not create
a NAT gateway. Runtime reconciliation uses DNS and ELBv2 APIs. AWS service-managed
ENIs and service-linked roles are not falsely claimed as tilink-owned resources.

An on-demand VPC probe checks DNS, TCP, TLS, and, when credentials are supplied,
MySQL `SELECT 1` or Kafka broker metadata. Credentials are supplied for that
invocation and are never stored in function configuration or logs. An AWS-side
probe cannot prove the TiDB-originated DM/TiCDC path. TiDB prechecks and sink tests
are separate evidence and do not create replication tasks.

## Destruction

`show`, `apply`, and `destroy` share the same inventory. Destruction refreshes
cloud state, builds the exact reverse dependency plan, and displays blockers.
It asks for `y/N`, then the complete connection ID. Both answers bind to the
same state revision and resource fingerprint. A changed plan invalidates them.
No `--yes` or `--force` bypass exists; non-interactive destruction is rejected.

Stop schedules and new probes before removing dependent resources. Wait for
in-flight work within a deadline. Delete consumer connections before provider
services, listeners before target groups, functions before roles/security groups,
and API endpoints after maintenance stops. Reused resources remain. Modified
external resources only lose the exact unchanged increments owned by tilink.

Continue independent deletions after a failure, but do not delete dependencies
of failed children. Record each result atomically. Missing resources count as
already deleted. Remaining resources, shared dependencies, ownership conflicts,
and missing API capabilities are incomplete cleanup, never success. Retain a
sanitized report after complete destruction. A retry refreshes the remaining
inventory and requires both confirmations again.

## Acceptance

Write reusable Go tests before production code. Cover strict configuration,
secret rejection, atomic state/revision conflicts, operation recovery, dependency
ordering, shared client requests, Digest authentication, transient errors, every
confirmation cancellation, stale plans, partial cleanup, and preserving external
resources. Use SDK interfaces, HTTP test servers, and protocol fixtures.

Run race tests, vet, release builds, and CLI integration tests. Live acceptance
is opt-in and requires explicit isolated account/instance identifiers. Record
application access, DM/TiCDC remote validation, database failover, every Kafka
broker, and post-destroy resource inventory. Mock tests do not establish live
cloud compatibility. Unperformed live checks must remain clearly documented.
