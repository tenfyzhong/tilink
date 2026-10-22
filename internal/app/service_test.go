package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	cloudaws "github.com/tenfyzhong/tilink/internal/aws"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tenfyzhong/tilink/internal/engine"
	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/tidb"
	"github.com/tenfyzhong/tilink/internal/worker"
)

type fakeProvider struct {
	steps     []model.Step
	config    model.Config
	refreshFn func(model.Resource) model.Resource
	deleteFn  func(model.Resource) error
}

func (p *fakeProvider) Plan(_ context.Context, c model.Config) ([]model.Step, error) {
	p.config = c
	return p.steps, nil
}
func (p *fakeProvider) Apply(_ context.Context, c model.Config, s model.Step, _ []model.Resource) (model.Resource, error) {
	return model.Resource{Key: s.Key, ID: s.Key, Kind: s.Kind, Provider: s.Provider, Status: "ready", Ownership: model.Created, OperationID: s.OperationID, Properties: s.Properties}, nil
}
func (p *fakeProvider) Refresh(_ context.Context, _ model.Config, r model.Resource) (model.Resource, error) {
	if p.refreshFn != nil {
		return p.refreshFn(r), nil
	}
	return r, nil
}
func (p *fakeProvider) Delete(_ context.Context, _ model.Config, r model.Resource) error {
	if p.deleteFn != nil {
		return p.deleteFn(r)
	}
	return nil
}
func config() model.Config {
	return model.Config{Version: 1, Name: "orders", Kind: "app", Provider: "aws", AWS: model.AWSConfig{Region: "us-east-1", AccountID: "123456789012", VPCID: "vpc-a", SubnetIDs: []string{"subnet-a"}}, TiDB: model.TiDBConfig{ProjectID: "project", ClusterID: "cluster"}}
}
func TestOpenWorksWithoutCredentialsOrBundle(t *testing.T) {
	s, store, err := Open(t.TempDir())
	if err != nil || s == nil || store == nil {
		t.Fatalf("offline open failed: %v", err)
	}
	states, err := store.List()
	if err != nil || len(states) != 0 {
		t.Fatal(err)
	}
}
func TestPlanEnrichesEndpointAndOrdersPeerCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "private_endpoint_service") {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{"private_endpoint_service": tidb.EndpointService{Name: "com.amazonaws.vpce.service", DNSName: "private.tidb.example", Port: 4000, AZIDs: []string{"use1-az1"}}})
	}))
	defer server.Close()
	client, _ := tidb.NewClient(tidb.Options{BaseURL: server.URL, PublicKey: "public", PrivateKey: "private"})
	aws := &fakeProvider{steps: []model.Step{{Key: "aws.endpoint", Provider: "aws", Kind: "interface_endpoint", Action: "create"}}}
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: aws, TiDB: tidb.NewProvider(client), Client: client}, nil
	}})
	p, err := svc.Plan(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if aws.config.TiDB.ServiceName != "com.amazonaws.vpce.service" || aws.config.TiDB.Host != "private.tidb.example" {
		t.Fatal("endpoint metadata was not propagated")
	}
	if len(p.Steps) != 3 || p.Steps[2].Key != "aws.private_dns" || p.Steps[2].Properties["endpoint_key"] != "aws.endpoint" {
		t.Fatalf("missing peer completion: %+v", p.Steps)
	}
}
func TestDMVerificationDoesNotCreatePersistentPrecheck(t *testing.T) {
	c := config()
	c.Kind = "dm"
	c.Target = model.TargetConfig{Type: "mysql", Host: "mysql.internal", Port: 3306}
	invoked := false
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: &fakeProvider{}, TiDB: &fakeProvider{}, Invoke: func(_ context.Context, _ model.Config, _ model.Resource, e worker.Event) (worker.Result, error) {
			invoked = true
			return worker.Result{Status: "pass", Vantage: "aws_vpc_probe", UpdatedAt: time.Now()}, nil
		}}, nil
	}})
	s := &model.State{Version: 1, ConnectionID: "id", Config: c, Status: "ready", Resources: []model.Resource{{Key: "aws.probe.function", ID: "arn:probe", Status: "ready"}}}
	if err := svc.Store.Save(s, 0); err != nil {
		t.Fatal(err)
	}
	v, err := svc.Verify(context.Background(), c.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !invoked || v.Status != "partial" || len(v.Uncovered) == 0 {
		t.Fatalf("DM verification overclaimed: %+v", v)
	}
}
func TestReconcileInvokesStoredEventAndPersistsFailure(t *testing.T) {
	c := config()
	c.Kind = "dm"
	c.Target = model.TargetConfig{Type: "mysql", Host: "mysql.internal", Port: 3306}
	c.Maintenance = true
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: &fakeProvider{}, TiDB: &fakeProvider{}, Invoke: func(_ context.Context, _ model.Config, _ model.Resource, e worker.Event) (worker.Result, error) {
			if e.Action != "" || len(e.Bindings) != 0 {
				t.Fatal("must load saved Lambda event")
			}
			return worker.Result{Status: "fail", Checks: []worker.Check{{Name: "dns", Status: "fail", Detail: "DNS resolution failed"}}}, nil
		}}, nil
	}})
	s := &model.State{Version: 1, ConnectionID: "id", Config: c, Status: "ready", Resources: []model.Resource{{Key: "aws.reconcile.function", ID: "arn:maintenance", Status: "ready"}}}
	if err := svc.Store.Save(s, 0); err != nil {
		t.Fatal(err)
	}
	result, err := svc.Reconcile(context.Background(), c.Name)
	if err == nil {
		t.Fatal("failed maintenance reported success")
	}
	if result.Status != "ready" || len(result.Findings) == 0 {
		t.Fatal("status/evidence lost")
	}
	saved, _ := svc.Store.Load(c.Name)
	if len(saved.Findings) == 0 {
		t.Fatal("failure not saved")
	}
}

var _ engine.Provider = (*fakeProvider)(nil)

func TestTiCDCTestUsesPrivateLinkAndKeepsCredentialsTransient(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "changefeeds:test") {
			t.Errorf("unexpected API call %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Sink tidb.Sink `json:"sink"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Sink.Type != "KAFKA" || request.Sink.Kafka.Network.PrivateLink.ID != "link-123" || request.Sink.Kafka.Network.NetworkType != "PRIVATE_LINK" || request.Sink.Kafka.Authentication.AuthType != "SASL_SCRAM_SHA_512" || request.Sink.Kafka.Authentication.Password != "transient-password" {
			t.Errorf("invalid sink %+v", request.Sink)
		}
	}))
	defer server.Close()
	client, _ := tidb.NewClient(tidb.Options{BaseURL: server.URL, DataflowBaseURL: server.URL, PublicKey: "public", PrivateKey: "private"})
	c := config()
	c.Kind = "ticdc"
	c.Target = model.TargetConfig{Type: "kafka", Host: "bootstrap.internal", Port: 9092, Brokers: []model.Broker{{ID: "1", Host: "broker.internal", Port: 9092, ListenerPort: 19092}}}
	c.TiDB.DataflowBaseURL = server.URL
	c.Verification = model.VerificationConfig{UsernameEnv: "KAFKA_USER", PasswordEnv: "KAFKA_PASSWORD", SASLMechanism: "SCRAM-SHA-512"}
	svc := New(t.TempDir(), Options{LookupEnv: func(name string) (string, bool) {
		if name == "KAFKA_USER" {
			return "user", true
		}
		if name == "KAFKA_PASSWORD" {
			return "transient-password", true
		}
		return "", false
	}, Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: &fakeProvider{}, TiDB: &fakeProvider{}, Client: client, Invoke: func(_ context.Context, _ model.Config, _ model.Resource, e worker.Event) (worker.Result, error) {
			if e.Probe == nil || e.Probe.Password != "transient-password" || e.Probe.Protocol != "kafka" {
				t.Fatal("probe credentials missing")
			}
			return worker.Result{Status: "pass"}, nil
		}}, nil
	}})
	s := &model.State{Version: 1, ConnectionID: "id", Config: c, Status: "ready", Resources: []model.Resource{{Key: "aws.probe.function", ID: "probe"}, {Key: "tidb.private_link", ID: "link-123"}}}
	if err := svc.Store.Save(s, 0); err != nil {
		t.Fatal(err)
	}
	v, err := svc.Verify(context.Background(), c.Name)
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "passed" || calls != 1 {
		t.Fatalf("remote verification missing: %+v calls=%d", v, calls)
	}
	saved, _ := svc.Store.Load(c.Name)
	data, _ := json.Marshal(saved)
	if strings.Contains(string(data), "transient-password") {
		t.Fatal("credentials persisted")
	}
}
func TestKafkaGeneratedFilesPersistForShow(t *testing.T) {
	c := config()
	c.Kind = "ticdc"
	c.Target = model.TargetConfig{Type: "kafka", Host: "bootstrap.internal", Port: 9092, Brokers: []model.Broker{{ID: "1", Host: "broker1.internal", Port: 9092, ListenerPort: 19092}, {ID: "2", Host: "broker2.internal", Port: 9092, ListenerPort: 29092}}}
	svc := New(t.TempDir(), Options{})
	s := &model.State{Version: 1, ConnectionID: "id", Config: c, Status: "ready", Resources: []model.Resource{{Key: "tidb.domains", Properties: map[string]string{"domains": "[\"private.kafka.example\"]"}, Status: "active"}}}
	if err := svc.Store.Save(s, 0); err != nil {
		t.Fatal(err)
	}
	if err := svc.generate(s); err != nil {
		t.Fatal(err)
	}
	saved, _ := svc.Store.Load(c.Name)
	if len(saved.GeneratedFiles) != 2 || !strings.Contains(saved.GeneratedFiles[1].Content, "private.kafka.example:29092") {
		t.Fatalf("missing Kafka config %+v", saved.GeneratedFiles)
	}
	rev := saved.Revision
	if err := svc.generate(saved); err != nil {
		t.Fatal(err)
	}
	if saved.Revision != rev {
		t.Fatal("unchanged generated content rewrote state")
	}
}
func TestMSKSuppliedIDsHaveNoDanglingAWSDependencies(t *testing.T) {
	c := config()
	c.Kind = "ticdc"
	c.Target = model.TargetConfig{Type: "msk", MSKClusterARN: "arn:aws:kafka:us-east-1:123456789012:cluster/test/id"}
	c.TiDB.PrivateLinkID = "managed-link"
	c.TiDB.EndpointID = "vpce-managed"
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: &fakeProvider{}, TiDB: &fakeProvider{}}, nil
	}})
	p, err := svc.Plan(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Steps) != 1 || p.Steps[0].Key != "tidb.private_link" || p.Steps[0].Manual != "" {
		t.Fatalf("MSK handoff still blocked: %+v", p)
	}
	v, err := svc.Apply(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Resources) != 1 || v.Resources[0].Ownership != model.Reused || v.Resources[0].Properties["evidence"] != "operator_supplied" {
		t.Fatalf("MSK ownership missing: %+v", v)
	}
}

func TestRealSDKAndTiDBPlanComposeOutboundWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, ":getAvailabilityZones") {
			fmt.Fprint(w, `{"accountId":"999999999999","azIds":["use1-az1"]}`)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "text/xml")
		switch r.Form.Get("Action") {
		case "GetCallerIdentity":
			fmt.Fprint(w, `<GetCallerIdentityResponse><GetCallerIdentityResult><Account>123456789012</Account><Arn>arn:aws:iam::123456789012:role/test</Arn><UserId>test</UserId></GetCallerIdentityResult></GetCallerIdentityResponse>`)
		case "DescribeSubnets":
			fmt.Fprint(w, `<DescribeSubnetsResponse><subnetSet><item><subnetId>subnet-a</subnetId><vpcId>vpc-a</vpcId><availabilityZoneId>use1-az1</availabilityZoneId></item></subnetSet></DescribeSubnetsResponse>`)
		default:
			t.Errorf("unexpected AWS action %s", r.Form.Get("Action"))
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	cfg := sdk.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	aws := &cloudaws.Provider{EC2: ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.BaseEndpoint = sdk.String(server.URL) }), STS: sts.NewFromConfig(cfg, func(o *sts.Options) { o.BaseEndpoint = sdk.String(server.URL) }), Deployment: &cloudaws.Deployment{}}
	client, _ := tidb.NewClient(tidb.Options{BaseURL: server.URL, DataflowBaseURL: server.URL, PublicKey: "public", PrivateKey: "private"})
	c := config()
	c.Kind = "dm"
	c.Target = model.TargetConfig{Type: "mysql", Host: "10.0.0.10", Port: 3306, SecurityGroupID: "sg-db"}
	c.TiDB.DataflowBaseURL = server.URL
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: aws, TiDB: tidb.NewProvider(client), Client: client}, nil
	}})
	p, err := svc.Plan(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range p.Steps {
		if s.Key == "aws.endpoint_acceptance" {
			found = true
			if s.Properties["service_key"] != "aws.endpoint_service" {
				t.Fatal("missing AWS dispatch property")
			}
		}
	}
	if !found {
		t.Fatal("AWS acceptance completion missing")
	}
}
func TestMSKMissingBootstrapDoesNotSendZeroPortSinkTest(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(400) }))
	defer server.Close()
	client, _ := tidb.NewClient(tidb.Options{BaseURL: server.URL, DataflowBaseURL: server.URL, PublicKey: "public", PrivateKey: "private"})
	c := config()
	c.Kind = "ticdc"
	c.Target = model.TargetConfig{Type: "msk", MSKClusterARN: "arn:msk"}
	c.TiDB.PrivateLinkID = "managed"
	c.TiDB.EndpointID = "vpce-managed"
	c.TiDB.DataflowBaseURL = server.URL
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: &fakeProvider{}, TiDB: &fakeProvider{}, Client: client}, nil
	}})
	s := &model.State{Version: 1, Config: c, Status: "ready"}
	if err := svc.Store.Save(s, 0); err != nil {
		t.Fatal(err)
	}
	v, err := svc.Verify(context.Background(), c.Name)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || v.Status != "partial" {
		t.Fatalf("invalid verification %+v calls=%d", v, calls)
	}
}
func TestWorkerInvocationsHoldConnectionLockAndRejectCleanup(t *testing.T) {
	for _, action := range []string{"verify", "reconcile"} {
		for _, status := range []string{"ready", "destroying", "destroyed", "cleanup_incomplete"} {
			t.Run(action+"/"+status, func(t *testing.T) {
				var svc *Service
				invoked := false
				c := config()
				c.Kind = "dm"
				c.Target = model.TargetConfig{Type: "mysql", Host: "mysql.internal", Port: 3306}
				svc = New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
					return Runtime{AWS: &fakeProvider{}, TiDB: &fakeProvider{}, Invoke: func(context.Context, model.Config, model.Resource, worker.Event) (worker.Result, error) {
						invoked = true
						unlock, err := svc.Store.Lock(c.Name)
						if err == nil {
							unlock()
							t.Error("worker invocation did not hold connection lock")
						}
						return worker.Result{Status: "pass"}, nil
					}}, nil
				}})
				s := &model.State{Version: 1, Config: c, Status: status, Resources: []model.Resource{{Key: "aws.probe.function", ID: "probe"}, {Key: "aws.reconcile.function", ID: "reconcile"}}}
				if err := svc.Store.Save(s, 0); err != nil {
					t.Fatal(err)
				}
				if action == "verify" {
					_, _ = svc.Verify(context.Background(), c.Name)
				} else {
					_, _ = svc.Reconcile(context.Background(), c.Name)
				}
				if invoked != (status == "ready") {
					t.Fatalf("status %s invoked=%v", status, invoked)
				}
			})
		}
	}
}
func TestRefreshReportsCurrentObservationAndDoctorMissingResources(t *testing.T) {
	c := config()
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
		return Runtime{AWS: &fakeProvider{refreshFn: func(r model.Resource) model.Resource { r.Status = "deleted"; return r }}, TiDB: &fakeProvider{}}, nil
	}})
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &model.State{Version: 1, Config: c, Status: "ready", Resources: []model.Resource{{Key: "aws.endpoint", Provider: "aws", Status: "active", ObservedAt: old}}}
	if err := svc.Store.Save(s, 0); err != nil {
		t.Fatal(err)
	}
	v, err := svc.Refresh(context.Background(), c.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Resources[0].ObservedAt.After(old) {
		t.Fatal("refreshed observation kept stale timestamp")
	}
	findings, err := svc.Doctor(context.Background(), c.Name)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range findings {
		if f.Code == "resource_readiness" && f.Severity == "error" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing resource treated as healthy")
	}
}
func TestKafkaPublicationRequiresExplicitAcknowledgement(t *testing.T) {
	c := config()
	c.Kind = "ticdc"
	c.Target = model.TargetConfig{Type: "kafka", Host: "bootstrap.internal", Port: 9092, Brokers: []model.Broker{{ID: "1", Host: "broker.internal", Port: 9092, ListenerPort: 19092}}}
	c.TiDB.PrincipalARN = "arn:aws:iam::999999999999:root"
	aws := &fakeProvider{steps: []model.Step{{Key: "aws.service_permission", Provider: "aws", Kind: "service_permission"}}}
	td := &fakeProvider{steps: []model.Step{{Key: "tidb.private_link", Provider: "tidb", Kind: "private_link", DependsOn: []string{"aws.service_permission"}}, {Key: "tidb.domains", Provider: "tidb", Kind: "private_link_domains", DependsOn: []string{"tidb.private_link"}}}}
	svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) { return Runtime{AWS: aws, TiDB: td}, nil }})
	p, err := svc.Plan(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range p.Steps {
		if s.Key == "workflow.kafka.publish" && s.Manual != "" {
			found = true
		}
	}
	if !found {
		t.Fatal("Kafka publish acknowledgement absent")
	}
	c.Target.AdvertisedListenersPublished = true
	p, err = svc.Plan(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range p.Steps {
		if s.Key == "workflow.kafka.publish" {
			t.Fatal("acknowledged publication still blocked")
		}
	}
}
func TestDeferredTargetInitializationMustPassBeforeReady(t *testing.T) {
	for _, status := range []string{"pass", "partial", "fail"} {
		t.Run(status, func(t *testing.T) {
			c := config()
			c.Kind = "dm"
			c.Maintenance = true
			svc := New(t.TempDir(), Options{})
			v := &model.State{Version: 1, Config: c, Status: "ready", Resources: []model.Resource{{Key: "aws.tg.primary", Kind: "target_group", Properties: map[string]string{"initialization": "deferred"}}, {Key: "aws.reconcile.function", ID: "function"}}}
			if err := svc.Store.Save(v, 0); err != nil {
				t.Fatal(err)
			}
			invoked := false
			runtime := Runtime{Invoke: func(context.Context, model.Config, model.Resource, worker.Event) (worker.Result, error) {
				invoked = true
				current, _ := svc.Store.Load(c.Name)
				if current.Status == "ready" {
					t.Error("initialization falsely ready during invocation")
				}
				return worker.Result{Status: status}, nil
			}}
			err := svc.initialize(context.Background(), v, runtime)
			if !invoked {
				t.Fatal("initialization not invoked")
			}
			if status == "pass" {
				if err != nil || v.Status != "ready" || v.Resources[0].Properties["initialized"] != "true" {
					t.Fatalf("initialization failed %+v %v", v, err)
				}
			} else if err == nil || v.Status == "ready" {
				t.Fatalf("initialization overclaimed %+v %v", v, err)
			}
		})
	}
}
func TestKafkaPublicationAcknowledgementUpdatesSavedInstructions(t *testing.T) {
	c := config()
	c.Kind = "ticdc"
	c.Target = model.TargetConfig{Type: "kafka", Host: "bootstrap.internal", Port: 9092, Brokers: []model.Broker{{ID: "1", Host: "broker.internal", Port: 9092, ListenerPort: 19092}}}
	svc := New(t.TempDir(), Options{})
	v := &model.State{Version: 1, Config: c, Resources: []model.Resource{{Key: "tidb.domains", Properties: map[string]string{"domains": "[\"private.kafka.example\"]"}}}}
	if err := svc.Store.Save(v, 0); err != nil {
		t.Fatal(err)
	}
	if err := svc.generate(v); err != nil {
		t.Fatal(err)
	}
	v.Config.Target.AdvertisedListenersPublished = true
	if err := svc.Store.Save(v, v.Revision); err != nil {
		t.Fatal(err)
	}
	if err := svc.generate(v); err != nil {
		t.Fatal(err)
	}
	if len(v.Findings) != 1 || v.Findings[0].Severity != "info" {
		t.Fatalf("stale publication instructions %+v", v.Findings)
	}
}

type cleanupConfirm struct {
	calls   int
	id      string
	afterID func()
}

func (c *cleanupConfirm) ConfirmPlan(engine.DestroyPlan) (bool, error) { c.calls++; return true, nil }
func (c *cleanupConfirm) ConfirmID(string) (string, error) {
	c.calls++
	if c.afterID != nil {
		c.afterID()
	}
	return c.id, nil
}
func TestManualConsumerRemovalAllowsOnlyOwnedCleanup(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		present, observationError, reappears bool
		success                              bool
	}{{"active", true, false, false, false}, {"console removal", false, false, false, true}, {"read failure", false, true, false, false}, {"reappears after confirmation", false, false, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			c := config()
			c.Kind = "dm"
			c.Target = model.TargetConfig{Type: "mysql", Host: "10.0.0.1", Port: 3306}
			c.TiDB.PrincipalARN = "arn:aws:iam::999999999999:root"
			present := tc.present
			deleted := []string{}
			aws := &fakeProvider{deleteFn: func(r model.Resource) error { deleted = append(deleted, r.Key); return nil }}
			svc := New(t.TempDir(), Options{Build: func(context.Context, model.Config) (Runtime, error) {
				return Runtime{AWS: aws, TiDB: &fakeProvider{}, ConsumerPresent: func(_ context.Context, _ model.Config, service model.Resource, endpoint string) (bool, error) {
					if service.ID != "vpce-svc-owned" || endpoint != "vpce-consumer" {
						t.Error("wrong consumer identity")
					}
					if tc.observationError {
						return false, errors.New("permission denied")
					}
					return present, nil
				}}, nil
			}})
			v := &model.State{Version: 1, ConnectionID: "conn", Config: c, Status: "ready", Resources: []model.Resource{{Key: "aws.endpoint_service", ID: "vpce-svc-owned", Provider: "aws", Kind: "endpoint_service", Ownership: model.Created, Status: "ready"}, {Key: "aws.service_permission", ID: "permission", Provider: "aws", Kind: "service_permission", Ownership: model.Modified, Status: "ready", DependsOn: []string{"aws.endpoint_service"}}, {Key: "tidb.private_link", ID: "operator-private-link", Provider: "tidb", Kind: "private_link", Ownership: model.Reused, Status: "ready", DependsOn: []string{"aws.service_permission"}, Properties: map[string]string{"operator_supplied": "true", "aws_endpoint_id": "vpce-consumer", "verification": "operator_supplied_unverified"}}, {Key: "tidb.domains", ID: "operator-domains", Provider: "tidb", Kind: "private_link_domains", Ownership: model.Reused, Status: "ready", DependsOn: []string{"tidb.private_link"}, Properties: map[string]string{"operator_supplied": "true"}}}}
			if err := svc.Store.Save(v, 0); err != nil {
				t.Fatal(err)
			}
			confirm := &cleanupConfirm{id: "conn"}
			if tc.reappears {
				confirm.afterID = func() { present = true }
			}
			result, err := svc.Destroy(context.Background(), c.Name, confirm)
			if tc.success {
				if err != nil {
					t.Fatal(err)
				}
				if result.Status != "destroyed" || len(deleted) != 2 || confirm.calls != 2 {
					t.Fatalf("cleanup incomplete %+v deleted=%v confirmations=%d", result, deleted, confirm.calls)
				}
				if _, err := svc.Refresh(context.Background(), c.Name); err != nil {
					t.Fatalf("destroyed connection cannot be refreshed: %v", err)
				}
				for _, r := range result.Resources {
					if r.Provider == "tidb" && (r.Ownership != model.Reused || r.Status == "deleted") {
						t.Fatal("external TiDB record was deleted or falsely reported deleted")
					}
					if r.Key == "tidb.private_link" && (r.Properties["aws_consumer_observation"] != "absent" || r.Properties["verification"] != "operator_supplied_unverified") {
						t.Fatal("missing observation boundary")
					}
				}
			} else {
				if err == nil || len(deleted) != 0 {
					t.Fatalf("unsafe cleanup err=%v deleted=%v", err, deleted)
				}
				if !tc.reappears && confirm.calls != 0 {
					t.Fatal("asked confirmation before resolving blocker")
				}
			}
		})
	}
}
