// Package app composes cloud providers into the CLI workflows.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	cloudaws "github.com/tenfyzhong/tilink/internal/aws"
	"github.com/tenfyzhong/tilink/internal/bundle"
	"github.com/tenfyzhong/tilink/internal/cli"
	"github.com/tenfyzhong/tilink/internal/engine"
	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/state"
	"github.com/tenfyzhong/tilink/internal/tidb"
	"github.com/tenfyzhong/tilink/internal/worker"
)

type Runtime struct {
	ConsumerPresent func(context.Context, model.Config, model.Resource, string) (bool, error)
	AWS             engine.Provider
	TiDB            engine.Provider
	Client          *tidb.Client
	Invoke          func(context.Context, model.Config, model.Resource, worker.Event) (worker.Result, error)
}
type Options struct {
	Build      func(context.Context, model.Config) (Runtime, error)
	LookupEnv  func(string) (string, bool)
	HTTPClient *http.Client
}
type Service struct {
	Store   *state.Store
	options Options
}

func New(dir string, options Options) *Service {
	if options.LookupEnv == nil {
		options.LookupEnv = os.LookupEnv
	}
	return &Service{Store: state.New(dir), options: options}
}
func Open(dir string) (cli.Service, cli.StateStore, error) {
	s := New(dir, Options{})
	return s, s.Store, nil
}
func (s *Service) build(ctx context.Context, c model.Config) (Runtime, error) {
	if s.options.Build != nil {
		return s.options.Build(ctx, c)
	}
	var out Runtime
	publicName, privateName := c.TiDB.PublicKeyEnv, c.TiDB.PrivateKeyEnv
	if publicName == "" {
		publicName = "TIDB_PUBLIC_KEY"
	}
	if privateName == "" {
		privateName = "TIDB_PRIVATE_KEY"
	}
	public, _ := s.options.LookupEnv(publicName)
	private, _ := s.options.LookupEnv(privateName)
	client, err := tidb.NewClient(tidb.Options{BaseURL: c.TiDB.BaseURL, DataflowBaseURL: c.TiDB.DataflowBaseURL, PublicKey: public, PrivateKey: private, HTTPClient: s.options.HTTPClient, MaxRetries: 2})
	if err != nil {
		return out, err
	}
	var zip []byte
	if c.Maintenance || c.Probe {
		zip, err = bundle.Worker()
		if err != nil {
			return out, err
		}
	}
	aws, err := cloudaws.New(ctx, c, func(p *cloudaws.Provider) { p.WorkerZip = zip })
	if err != nil {
		return out, err
	}
	return Runtime{AWS: aws, TiDB: tidb.NewProvider(client), Client: client, Invoke: aws.Deployment.InvokeOwned, ConsumerPresent: aws.ConsumerPresent}, nil
}
func (s *Service) prepare(ctx context.Context, c model.Config) (model.Config, Runtime, string, error) {
	if err := c.Validate(); err != nil {
		return c, Runtime{}, "", err
	}
	r, err := s.build(ctx, c)
	if err != nil {
		return c, r, "", err
	}
	if c.Kind == "app" && r.Client != nil {
		service, err := r.Client.GetEndpointService(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID)
		if err != nil {
			var api *tidb.APIError
			if !errors.As(err, &api) || !api.NotFound() {
				return c, r, "", err
			}
		}
		if service.Name == "" && c.TiDB.ServiceName == "" {
			return c, r, "Create the TiDB private endpoint service in the console, then set tidb.service_name, tidb.host, tidb.port and tidb.az_ids and run apply again. Service creation has no verified deletion API.", nil
		}
		if service.Name != "" {
			if c.TiDB.ServiceName != "" && c.TiDB.ServiceName != service.Name {
				return c, r, "", errors.New("configured TiDB endpoint service conflicts with discovery")
			}
			c.TiDB.ServiceName = service.Name
			if service.DNSName != "" {
				c.TiDB.Host = service.DNSName
			}
			if service.Port != 0 {
				c.TiDB.Port = service.Port
			}
			if len(service.AZIDs) > 0 {
				c.TiDB.AZIDs = service.AZIDs
			}
		}
	}
	if c.Kind != "app" && c.Target.Type != "msk" && c.TiDB.DataflowBaseURL != "" && r.Client != nil {
		info, err := r.Client.AvailabilityZones(ctx, c.TiDB.ClusterID)
		if err != nil {
			return c, r, "", err
		}
		if info.AccountID != "" {
			principal := "arn:aws:iam::" + info.AccountID + ":root"
			if c.TiDB.PrincipalARN != "" && c.TiDB.PrincipalARN != principal {
				return c, r, "", errors.New("configured TiDB principal conflicts with discovered account")
			}
			c.TiDB.PrincipalARN = principal
		}
		if len(info.AZIDs) > 0 {
			c.TiDB.AZIDs = info.AZIDs
		}
	}
	if c.Kind != "app" && c.Target.Type != "msk" && c.TiDB.PrincipalARN == "" {
		return c, r, "Set tidb.dataflow_base_url for the verified PrivateLink API to discover the TiDB principal and AZ IDs, or obtain tidb.principal_arn and tidb.az_ids from TiDB Cloud and run apply again.", nil
	}
	return c, r, "", nil
}

type plannedProvider struct {
	engine.Provider
	extra       []model.Step
	replace     []model.Step
	replacement bool
}

func (p plannedProvider) Plan(ctx context.Context, c model.Config) ([]model.Step, error) {
	if p.replacement {
		return p.replace, nil
	}
	steps, err := p.Provider.Plan(ctx, c)
	return append(steps, p.extra...), err
}

type manualProvider struct{ message string }

func (p manualProvider) Plan(context.Context, model.Config) ([]model.Step, error) {
	return []model.Step{{Key: "workflow.action", Provider: "workflow", Kind: "manual", Action: "manual", Manual: p.message}}, nil
}
func (p manualProvider) Apply(context.Context, model.Config, model.Step, []model.Resource) (model.Resource, error) {
	return model.Resource{}, engine.ErrWaitingAction
}
func (p manualProvider) Refresh(_ context.Context, _ model.Config, r model.Resource) (model.Resource, error) {
	return r, nil
}
func (p manualProvider) Delete(context.Context, model.Config, model.Resource) error { return nil }
func (s *Service) engine(c model.Config, r Runtime, manual string) *engine.Engine {
	e := &engine.Engine{Store: s.Store, Providers: map[string]engine.Provider{}}
	if manual != "" {
		e.Providers["workflow"] = manualProvider{manual}
		return e
	}
	aws := plannedProvider{Provider: r.AWS}
	if c.Kind == "app" {
		aws.extra = []model.Step{{Key: "aws.private_dns", Provider: "aws", Kind: "private_dns", Action: "update", DependsOn: []string{"aws.endpoint", "tidb.registration"}, Properties: map[string]string{"endpoint_key": "aws.endpoint"}}}
	}
	if c.Kind != "app" && c.Target.Type != "msk" {
		aws.extra = []model.Step{{Key: "aws.endpoint_acceptance", Provider: "aws", Kind: "endpoint_acceptance", Action: "update", DependsOn: []string{"aws.service_permission", "tidb.private_link"}, Properties: map[string]string{"service_key": "aws.endpoint_service"}}}
	}
	e.Providers["aws"] = aws
	e.Providers["tidb"] = consumerCleanupProvider{Provider: r.TiDB, Store: s.Store, observe: r.ConsumerPresent}
	if c.Target.Type == "kafka" && !c.Target.AdvertisedListenersPublished {
		e.Providers["workflow"] = plannedProvider{Provider: manualProvider{}, replacement: true, replace: []model.Step{{Key: "workflow.kafka.publish", Provider: "workflow", Kind: "manual", Action: "manual", DependsOn: []string{"tidb.domains", "aws.endpoint_acceptance"}, Manual: "Run tilink show to obtain generated advertised.listeners for every broker. Publish them while preserving existing listener and security settings, set target.advertised_listeners_published: true, and run apply again. Verification still checks actual broker metadata."}}}
	}
	if c.Target.Type == "msk" {
		e.Providers["tidb"] = mskProvider{Client: r.Client}
	}
	return e
}
func (s *Service) Plan(ctx context.Context, c model.Config) (model.Plan, error) {
	c, r, manual, err := s.prepare(ctx, c)
	if err != nil {
		return model.Plan{}, err
	}
	return s.engine(c, r, manual).Plan(ctx, c)
}
func (s *Service) Apply(ctx context.Context, c model.Config) (*model.State, error) {
	c, r, manual, err := s.prepare(ctx, c)
	if err != nil {
		return nil, err
	}
	v, applyErr := s.engine(c, r, manual).Apply(ctx, c)
	if v != nil {
		if err := s.generate(v); err != nil {
			return v, err
		}
		if err := s.initialize(ctx, v, r); err != nil {
			return v, err
		}
	}
	return v, applyErr
}
func (s *Service) existing(ctx context.Context, name string) (*model.State, Runtime, *engine.Engine, error) {
	v, err := s.Store.Load(name)
	if err != nil {
		return nil, Runtime{}, nil, err
	}
	r, err := s.build(ctx, v.Config)
	if err != nil {
		return v, r, nil, err
	}
	return v, r, s.engine(v.Config, r, ""), nil
}
func (s *Service) Refresh(ctx context.Context, name string) (*model.State, error) {
	prior, _, e, err := s.existing(ctx, name)
	if err != nil {
		return nil, err
	}
	v, err := e.Refresh(ctx, name)
	if err == nil {
		for i := range v.Resources {
			if i < len(prior.Resources) && prior.Resources[i].Status != "deleted" {
				v.Resources[i].ObservedAt = time.Now().UTC()
			}
		}
		err = s.generate(v)
	}
	return v, err
}
func (s *Service) Destroy(ctx context.Context, name string, c engine.Confirmer) (*model.State, error) {
	_, _, e, err := s.existing(ctx, name)
	if err != nil {
		return nil, err
	}
	return e.Destroy(ctx, name, c)
}
func (s *Service) generate(v *model.State) error {
	if v.Config.Target.Type != "kafka" || v.Status == "destroyed" {
		return nil
	}
	var domains []string
	for _, r := range v.Resources {
		if r.Key == "tidb.domains" && r.Status != "deleted" {
			if err := json.Unmarshal([]byte(r.Properties["domains"]), &domains); err != nil {
				return errors.New("invalid saved Kafka private domains")
			}
		}
	}
	if len(domains) == 0 {
		return nil
	}
	if len(domains) != 1 && len(domains) != len(v.Config.Target.Brokers) {
		return errors.New("Kafka private domain count cannot be mapped to broker listener ports")
	}
	files := []model.GeneratedFile{}
	for i, b := range v.Config.Target.Brokers {
		domain := domains[0]
		if len(domains) > 1 {
			domain = domains[i]
		}
		files = append(files, model.GeneratedFile{Path: filepath.Join("generated", v.Config.Name, "broker-"+b.ID+".properties"), Content: fmt.Sprintf("# Publish this listener alongside the broker's existing listeners.\n# Preserve the broker's existing security protocol and inter-broker settings.\nadvertised.listeners=PRIVATE://%s:%d\n", domain, b.ListenerPort)})
	}
	finding := model.Finding{Code: "kafka_configuration", Severity: "warning", Message: "Generated Kafka advertised.listeners must be published by the operator; tilink does not restart brokers.", NextStep: "Run tilink show, publish each broker configuration while preserving existing listener settings, then run verify."}
	if v.Config.Target.AdvertisedListenersPublished {
		finding.Severity = "info"
		finding.Message = "The operator acknowledged publication of Kafka advertised.listeners; broker metadata still requires verification."
		finding.NextStep = "Run tilink verify to check every advertised broker endpoint."
	}
	nextFindings := replaceFindings(v.Findings, "kafka_configuration", []model.Finding{finding})
	before, _ := json.Marshal([]any{v.GeneratedFiles, v.Findings})
	after, _ := json.Marshal([]any{files, nextFindings})
	if string(before) == string(after) {
		return nil
	}
	v.GeneratedFiles = files
	v.Findings = nextFindings
	return s.Store.Save(v, v.Revision)
}
func replaceFindings(all []model.Finding, prefix string, add []model.Finding) []model.Finding {
	out := []model.Finding{}
	for _, f := range all {
		if !strings.HasPrefix(f.Code, prefix) {
			out = append(out, f)
		}
	}
	return append(out, add...)
}
func resourceID(v *model.State, key string) string {
	for _, r := range v.Resources {
		if r.Key == key && r.Status != "deleted" {
			return r.ID
		}
	}
	return ""
}
func (s *Service) Doctor(ctx context.Context, name string) ([]model.Finding, error) {
	v, err := s.Refresh(ctx, name)
	if err != nil {
		return nil, err
	}
	findings := append([]model.Finding{}, v.Findings...)
	for _, r := range v.Resources {
		switch strings.ToLower(r.Status) {
		case "ready", "active", "available":
		case "deleted":
			if v.Status != "destroyed" {
				findings = append(findings, model.Finding{Code: "resource_readiness", Severity: "error", ResourceKey: r.Key, Message: "Managed resource is missing", NextStep: "Run apply to reconcile the missing resource or destroy to finish cleanup."})
			}
		default:
			findings = append(findings, model.Finding{Code: "resource_readiness", Severity: "error", ResourceKey: r.Key, Message: "Resource is " + r.Status, NextStep: "Inspect authorization, acceptance, DNS, security groups, target health, and AZ IDs; then run apply again."})
		}
	}
	if v.Config.Kind != "app" {
		findings = append(findings, model.Finding{Code: "remote_verification", Severity: "warning", Message: "A customer VPC probe cannot prove TiDB Cloud outbound connectivity.", NextStep: "Run verify and complete all reported TiDB-side checks."})
	}
	return findings, nil
}
func (s *Service) Reconcile(ctx context.Context, name string) (*model.State, error) {
	unlock, err := s.Store.Lock(name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := s.allowWorkers(name); err != nil {
		return nil, err
	}
	v, r, _, err := s.existing(ctx, name)
	if err != nil {
		return v, err
	}
	function := recordedResource(v, "aws.reconcile.function")
	id := function.ID
	if id == "" || r.Invoke == nil {
		return v, errors.New("maintenance function is unavailable; enable maintenance and apply first")
	}
	result, invokeErr := r.Invoke(ctx, v.Config, function, worker.Event{})
	findings := workerFindings("maintenance", result)
	if invokeErr != nil {
		findings = append(findings, model.Finding{Code: "maintenance_invocation", Severity: "error", Message: "Maintenance invocation failed.", NextStep: "Inspect the Lambda status, permissions and CloudWatch logs, then retry reconcile."})
	}
	v.Findings = replaceFindings(v.Findings, "maintenance", findings)
	if err := s.Store.SaveLocked(v, v.Revision); err != nil {
		return v, err
	}
	if invokeErr != nil {
		return v, invokeErr
	}
	if result.Status != "pass" {
		return v, fmt.Errorf("maintenance is %s", result.Status)
	}
	return v, nil
}
func workerFindings(prefix string, result worker.Result) []model.Finding {
	out := []model.Finding{}
	for _, c := range result.Checks {
		severity := "info"
		if c.Status == "fail" {
			severity = "error"
		} else if c.Status == "partial" {
			severity = "warning"
		}
		out = append(out, model.Finding{Code: prefix + "_" + c.Name, Severity: severity, Message: c.Detail})
	}
	return out
}
func (s *Service) Verify(ctx context.Context, name string) (model.Verification, error) {
	report := model.Verification{Status: "partial", Location: "aws_vpc_probe", CheckedAt: time.Now().UTC()}
	unlock, err := s.Store.Lock(name)
	if err != nil {
		return report, err
	}
	defer unlock()
	if err := s.allowWorkers(name); err != nil {
		return report, err
	}
	v, r, _, err := s.existing(ctx, name)
	if err != nil {
		return report, err
	}
	c := v.Config
	host, port := c.Target.Host, c.Target.Port
	if c.Kind == "app" {
		host, port = c.TiDB.Host, c.TiDB.Port
		if port == 0 {
			port = 4000
		}
	}
	protocol := "mysql"
	if c.Target.Type == "kafka" || c.Target.Type == "msk" {
		protocol = "kafka"
	}
	username, _ := s.options.LookupEnv(c.Verification.UsernameEnv)
	password, _ := s.options.LookupEnv(c.Verification.PasswordEnv)
	req := worker.ProbeRequest{Protocol: protocol, Host: host, Port: strconv.Itoa(port), TLS: true, ServerName: c.Verification.ServerName, Username: username, Password: password, SASLMechanism: c.Verification.SASLMechanism, Database: c.Verification.Database}
	if c.Verification.CAFile != "" {
		req.CAPEM, err = os.ReadFile(c.Verification.CAFile)
		if err != nil {
			return report, errors.New("read verification CA file failed")
		}
	}
	probeID := resourceID(v, "aws.probe.function")
	probePass := false
	if probeID == "" || r.Invoke == nil {
		report.Uncovered = append(report.Uncovered, "VPC probe is unavailable; enable probe and apply first.")
	} else if host == "" {
		report.Uncovered = append(report.Uncovered, "Probe hostname is not configured.")
	} else {
		result, err := r.Invoke(ctx, c, recordedResource(v, "aws.probe.function"), worker.Event{Action: "probe", Probe: &req})
		if err != nil {
			return report, err
		}
		report.Findings = append(report.Findings, workerFindings("probe", result)...)
		probePass = result.Status == "pass"
		if result.Status == "fail" {
			report.Status = "failed"
		} else if !probePass {
			report.Uncovered = append(report.Uncovered, "VPC protocol authentication or metadata verification is incomplete.")
		}
	}
	remotePass := c.Kind == "app"
	if c.Kind == "dm" {
		report.Uncovered = append(report.Uncovered, "Run the DM migration precheck from TiDB Cloud and review its source, privileges, binlog, and connectivity results. Automatic precheck creation is disabled because cancellation retains a remote audit record.")
	}
	if c.Kind == "ticdc" {
		link := resourceID(v, "tidb.private_link")
		if link == "" {
			link = c.TiDB.PrivateLinkID
		}
		if port < 1 || port > 65535 {
			report.Uncovered = append(report.Uncovered, "Set target.host and target.port to the MSK SASL/SCRAM bootstrap endpoint before verification; no zero-port sink test was sent.")
		} else if r.Client == nil || c.TiDB.DataflowBaseURL == "" || link == "" {
			report.Uncovered = append(report.Uncovered, "TiDB-originated sink test requires a verified dataflow API and private link ID.")
		} else {
			sink := tidb.Sink{Type: "MYSQL", MySQL: &tidb.MySQLSink{Network: tidb.SinkNetwork{NetworkType: "PRIVATE_LINK", PrivateLink: &tidb.SinkPrivateLink{ID: link, Port: port}}, Authentication: tidb.MySQLAuthentication{Username: username, Password: password, EnableTLS: true}}}
			canTest := true
			if protocol == "kafka" {
				auth := kafkaAuth(c.Verification.SASLMechanism)
				if auth == "" {
					canTest = false
					report.Uncovered = append(report.Uncovered, "Set a supported Kafka SASL mechanism before TiDB sink testing.")
				}
				sink = tidb.Sink{Type: "KAFKA", Kafka: &tidb.KafkaSink{Network: tidb.KafkaNetwork{NetworkType: "PRIVATE_LINK", PrivateLink: &tidb.SinkPrivateLink{ID: link, Port: port}}, Authentication: tidb.KafkaAuthentication{AuthType: auth, Username: username, Password: password, EnableTLS: true, ServerName: c.Verification.ServerName}}}
			} else if username == "" {
				canTest = false
				report.Uncovered = append(report.Uncovered, "MySQL sink test requires verification credential environment references.")
			}
			if canTest {
				if err := r.Client.TestSink(ctx, c.TiDB.ClusterID, sink); err != nil {
					return report, err
				}
				remotePass = true
				report.Findings = append(report.Findings, model.Finding{Code: "tidb_sink_test", Severity: "info", Message: "TiDB Cloud independently validated the PrivateLink sink."})
			}
		}
	}
	if c.Kind == "app" {
		report.Uncovered = append(report.Uncovered, "The application process must validate its own path; the probe only represents its configured subnet and security groups.")
	}
	if probePass && remotePass && report.Status != "failed" {
		report.Status = "passed"
	}
	return report, nil
}
func kafkaAuth(mechanism string) string {
	switch strings.ToUpper(mechanism) {
	case "", "NONE", "DISABLE":
		return "DISABLE"
	case "PLAIN", "SASL_PLAIN":
		return "SASL_PLAIN"
	case "SCRAM-SHA-256", "SASL_SCRAM_SHA_256":
		return "SASL_SCRAM_SHA_256"
	case "SCRAM-SHA-512", "SASL_SCRAM_SHA_512":
		return "SASL_SCRAM_SHA_512"
	}
	return ""
}

var _ cli.Service = (*Service)(nil)

// mskProvider imports an operator-created managed connection without claiming its ownership.
type mskProvider struct{ Client *tidb.Client }

func (p mskProvider) Plan(_ context.Context, c model.Config) ([]model.Step, error) {
	step := model.Step{Key: "tidb.private_link", Provider: "tidb", Kind: "msk_private_link", Action: "reuse"}
	if c.TiDB.PrivateLinkID == "" || c.TiDB.EndpointID == "" {
		step.Manual = "Complete the MSK managed PrivateLink connection in TiDB Cloud, then supply tidb.private_link_id and tidb.endpoint_id and run apply again. The existing MSK multi-VPC SASL/SCRAM and cluster policy prerequisites are inspected by AWS planning."
	}
	return []model.Step{step}, nil
}
func (p mskProvider) Apply(ctx context.Context, c model.Config, s model.Step, _ []model.Resource) (model.Resource, error) {
	r := model.Resource{Key: s.Key, ID: c.TiDB.PrivateLinkID, Provider: "tidb", Kind: s.Kind, Ownership: model.Reused, Status: "ready", OperationID: s.OperationID, Properties: map[string]string{"evidence": "operator_supplied", "aws_endpoint_id": c.TiDB.EndpointID, "msk_cluster_arn": c.Target.MSKClusterARN}, ObservedAt: time.Now().UTC()}
	return p.Refresh(ctx, c, r)
}
func (p mskProvider) Refresh(ctx context.Context, c model.Config, r model.Resource) (model.Resource, error) {
	if p.Client != nil && c.TiDB.DataflowBaseURL != "" {
		link, err := p.Client.GetPrivateLink(ctx, c.TiDB.ClusterID, r.ID)
		if err != nil {
			return r, err
		}
		if link.ClusterID != "" && link.ClusterID != c.TiDB.ClusterID {
			return r, errors.New("MSK private link cluster identity conflicts")
		}
		if link.ID != "" && link.ID != r.ID {
			return r, errors.New("MSK private link identity conflicts")
		}
		if link.AWSEndpointService != nil && link.AWSEndpointService.EndpointID != "" && link.AWSEndpointService.EndpointID != c.TiDB.EndpointID {
			return r, errors.New("MSK consumer endpoint identity conflicts")
		}
		r.Status = strings.ToLower(link.State)
		r.Properties["evidence"] = "tidb_api"
		if r.Status != "active" {
			return r, errors.New("MSK managed private link is not active")
		}
	}
	r.ObservedAt = time.Now().UTC()
	return r, nil
}
func (p mskProvider) Delete(context.Context, model.Config, model.Resource) error {
	return errors.New("imported MSK managed connection must be preserved")
}

func recordedResource(v *model.State, key string) model.Resource {
	for _, r := range v.Resources {
		if r.Key == key && r.Status != "deleted" {
			return r
		}
	}
	return model.Resource{}
}

func (s *Service) allowWorkers(name string) error {
	v, err := s.Store.Load(name)
	if err != nil {
		return err
	}
	switch v.Status {
	case "destroying", "destroyed", "cleanup_incomplete":
		return errors.New("worker invocation is disabled during or after cleanup")
	}
	return nil
}

func (s *Service) initialize(ctx context.Context, v *model.State, r Runtime) error {
	if !v.Config.Maintenance || v.Config.Kind == "app" || v.Config.Target.Type == "msk" {
		return nil
	}
	pending := false
	for _, resource := range v.Resources {
		if resource.Kind == "target_group" && resource.Properties["initialization"] == "deferred" && resource.Properties["initialized"] != "true" {
			pending = true
		}
	}
	function := recordedResource(v, "aws.reconcile.function")
	if !pending || function.ID == "" {
		return nil
	}
	if r.Invoke == nil {
		return errors.New("target initialization requires the deployed VPC maintenance function")
	}
	unlock, err := s.Store.Lock(v.Config.Name)
	if err != nil {
		return err
	}
	defer unlock()
	original := v.Status
	v.Status = "initializing"
	v.Findings = replaceFindings(v.Findings, "target_initialization", []model.Finding{{Code: "target_initialization", Severity: "warning", Message: "NLB targets are waiting for initialization from the VPC maintenance function.", NextStep: "Resolve maintenance errors and run apply again."}})
	if err := s.Store.SaveLocked(v, v.Revision); err != nil {
		return err
	}
	result, invokeErr := r.Invoke(ctx, v.Config, function, worker.Event{})
	if invokeErr != nil || result.Status != "pass" {
		v.Status = "incomplete"
		v.Findings = replaceFindings(v.Findings, "maintenance", workerFindings("maintenance", result))
		if err := s.Store.SaveLocked(v, v.Revision); err != nil {
			return err
		}
		if invokeErr != nil {
			return invokeErr
		}
		return fmt.Errorf("target initialization is %s; inspect findings and run apply again", result.Status)
	}
	for i := range v.Resources {
		if v.Resources[i].Kind == "target_group" && v.Resources[i].Properties["initialization"] == "deferred" {
			v.Resources[i].Properties["initialized"] = "true"
		}
	}
	v.Status = original
	v.Findings = replaceFindings(v.Findings, "target_initialization", nil)
	v.Findings = replaceFindings(v.Findings, "maintenance", workerFindings("maintenance", result))
	return s.Store.SaveLocked(v, v.Revision)
}

// consumerCleanupProvider preserves external TiDB records while observing their AWS dependency.
type consumerCleanupProvider struct {
	engine.Provider
	Store   *state.Store
	observe func(context.Context, model.Config, model.Resource, string) (bool, error)
}

func (p consumerCleanupProvider) Refresh(ctx context.Context, c model.Config, r model.Resource) (model.Resource, error) {
	next, err := p.Provider.Refresh(ctx, c, r)
	if err != nil {
		return next, err
	}
	if next.Key != "tidb.private_link" || next.Ownership != model.Reused || next.Properties["operator_supplied"] != "true" || p.observe == nil {
		return next, nil
	}
	endpoint := next.Properties["aws_endpoint_id"]
	if endpoint == "" {
		return next, errors.New("operator-managed connection is missing its AWS consumer endpoint identity")
	}
	saved, err := p.Store.Load(c.Name)
	if err != nil {
		return next, err
	}
	service := model.Resource{}
	for _, resource := range saved.Resources {
		if resource.Key == "aws.endpoint_service" {
			service = resource
			break
		}
	}
	if service.ID == "" {
		return next, errors.New("cannot inspect the operator-managed consumer without its recorded AWS endpoint service")
	}
	present, err := p.observe(ctx, c, service, endpoint)
	if err != nil {
		return next, fmt.Errorf("inspect operator-managed AWS consumer: %w", err)
	}
	dependencies := next.DependsOn
	if baseline := next.Properties["cleanup_aws_dependencies"]; baseline != "" {
		if err := json.Unmarshal([]byte(baseline), &dependencies); err != nil {
			return next, errors.New("invalid saved AWS cleanup dependencies")
		}
	} else {
		baseline, _ := json.Marshal(dependencies)
		next.Properties["cleanup_aws_dependencies"] = string(baseline)
	}
	if present {
		next.DependsOn = dependencies
		next.Properties["aws_consumer_observation"] = "present"
	} else {
		next.DependsOn = nil
		for _, dep := range dependencies {
			if !strings.HasPrefix(dep, "aws.") {
				next.DependsOn = append(next.DependsOn, dep)
			}
		}
		next.Properties["aws_consumer_observation"] = "absent"
	}
	return next, nil
}
