# Validation and live acceptance

The implementation is validated with reusable Go tests and builds. No customer
cloud resources were created or deleted during development. Local test success
does not establish compatibility with a particular TiDB Cloud environment or
prove an end-to-end PrivateLink connection.

## Local checks

```sh
make test
make vet
make build
./bin/tilink --help
git diff --check
```

The suite covers strict configuration and examples; owner-only atomic state;
operation journals and ambiguous-create recovery; dependency ordering; two
confirmation prompts and stale-plan rejection; owned and reused cleanup; actual
AWS SDK request serialization; shared TiDB Digest requests; missing API/manual
handoffs; Lambda deployment and cleanup; target failover; private socket dialing;
MySQL protocol/TLS and Kafka metadata fixtures; and CLI/provider composition.

## Isolated live acceptance

Select an explicit test AWS profile, account, region, VPC, and subnets, together
with TiDB project/cluster IDs. Use existing test databases or brokers. Supply
credentials through environment references, and retain the state directory.

| Scenario | Required evidence |
| --- | --- |
| Application | Interface endpoint and TiDB registration ready; VPC probe and actual application query succeed with TLS |
| DM | Source probe succeeds; TiDB-side migration precheck confirms connection, privileges, and binlog requirements |
| TiCDC MySQL | VPC probe and TiDB-originated sink test both succeed |
| Database failover | Writer DNS changes; VPC worker validates standby targets, switches traffic, and drains old targets without mixing writers |
| Self-hosted Kafka | Generated listener settings published; every metadata broker is reachable from the TiDB path; sink test succeeds |
| MSK | Existing supported SCRAM/multi-VPC configuration and policy validated; console connection and every broker tested |
| Retry | Interrupted create resumes without duplicates or false ownership; manual handoff resumes from saved state |
| Destroy | Rejecting either prompt changes nothing; accepted cleanup removes all tool-created resources and preserves reused objects |

After destroy, independently inspect the selected AWS account and TiDB project.
Compare cloud IDs with `show --json`, including security-group rules, target
groups, health listeners, endpoint services, interface endpoints, Lambda functions,
IAM roles/policies, log groups, schedules, permissions, and targets. No remaining
owned resource may be reported as successful cleanup. Console-created connections
are reused objects and may need console removal before provider-service cleanup.

Lambda-managed network interfaces can take 20 minutes or longer to disappear.
Retain execution roles while AWS releases them; retry the incomplete cleanup
afterward. This behavior follows the
[AWS Lambda VPC lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html).

The live scenarios above remain unperformed until an isolated environment is
provided and the observed results are recorded. Alibaba Cloud is a future provider
and has no implementation or acceptance claim in this version.
