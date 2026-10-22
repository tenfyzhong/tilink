# tilink

tilink provisions, inspects, verifies, and removes private network connections
between AWS workloads and TiDB Cloud Essential or Premium. It uses the AWS SDK
and a shared TiDB Cloud API client. It does not create databases, migrations, or
changefeeds.

## Workflows

| Connection | Network |
| --- | --- |
| Application | AWS application VPC to TiDB Cloud |
| DM source | TiDB Cloud to RDS MySQL or Aurora MySQL |
| TiCDC sink | TiDB Cloud to MySQL, RDS, Aurora, self-hosted Kafka, or MSK Provisioned |

AWS is the first provider. Alibaba Cloud is a future provider, not a promise of
cross-cloud PrivateLink connectivity. Missing TiDB API operations produce explicit,
resumable manual actions. Resource readiness is separate from network verification.

## Build and test

Use Go 1.27 or later. AWS CLI and Terraform are not required.

```sh
make build
make test
./bin/tilink --help
```

The CLI supports macOS and Linux. The build packages a Linux ARM64 Lambda worker
into the CLI; `go install` alone does not build that embedded worker. Tests use fake SDK clients,
HTTP servers, and local protocol fixtures. Live acceptance requires an isolated AWS
account, existing TiDB instances, and explicitly selected network resources.

## Usage

```sh
tilink init -f tilink.yaml
tilink plan -f tilink.yaml
tilink apply -f tilink.yaml
tilink show
tilink show my-connection --refresh --json
tilink doctor my-connection
tilink verify my-connection
tilink reconcile my-connection
tilink destroy my-connection
```

AWS authentication uses the standard SDK credential chain, including profiles,
SSO, and assumed roles. TiDB API and database credentials are environment variable
references, never literal values in configuration. See [CLI reference](docs/cli.md)
for configuration, manual steps, exit behavior, and deletion.

Start from the [application](examples/app.yaml), [RDS DM](examples/rds-dm.yaml),
or [Kafka TiCDC](examples/kafka-ticdc.yaml) example. Replace all placeholder IDs
with resources from the selected account. The outbound examples deliberately
leave the dataflow API endpoint unset: follow the reported console handoff or
configure a verified endpoint for your TiDB Cloud environment.

`show` lists generated configuration and every recorded resource, including
auxiliary infrastructure. `destroy` requires two interactive confirmations and
deletes resources created by tilink. It preserves reused resources and reports
every unresolved deletion. Keep the state directory: it records ownership and
the exact configuration changes made to existing resources.

Read the [design and acceptance contract](docs/design.md) before contributing.
All repository documentation, comments, and examples must be written in English.

See [validation and live acceptance](docs/validation.md) for the test boundary and
the remaining checks that require an isolated cloud environment.
