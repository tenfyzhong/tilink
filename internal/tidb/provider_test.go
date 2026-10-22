package tidb

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tenfyzhong/tilink/internal/model"
)

func providerConfig() model.Config {
	return model.Config{Kind: "app", TiDB: model.TiDBConfig{ProjectID: "project", ClusterID: "cluster"}}
}
func TestProviderPlanRequiresExistingServiceAndExplicitDataflow(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	steps, err := p.Plan(context.Background(), providerConfig())
	if err != nil || len(steps) != 1 || steps[0].Manual == "" {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	c := providerConfig()
	c.Kind = "dm"
	client, _ := NewClient(Options{PublicKey: "public", PrivateKey: "secret"})
	steps, err = NewProvider(client).Plan(context.Background(), c)
	if err != nil || steps[0].Manual == "" {
		t.Fatalf("missing dataflow must be manual: %+v %v", steps, err)
	}
}
func TestProviderRegistrationExistingIsReused(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("unexpected mutation")
		}
		fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-one","status":"ACTIVE","cluster_id":"cluster"}]}`)
	}))
	defer s.Close()
	c := providerConfig()
	p := NewProvider(clientFor(t, s))
	step := model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation", DependsOn: []string{"aws.endpoint"}}
	r, err := p.Apply(context.Background(), c, step, []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}})
	if err != nil || r.Ownership != model.Reused || r.ID != "42" {
		t.Fatalf("resource=%+v err=%v", r, err)
	}
}
func TestProviderPrivateLinkPersistsNonreadyIdentityAndChecksReuse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/plc-owned") {
			fmt.Fprint(w, `{"privateLinkConnectionId":"plc-owned","clusterId":"cluster","displayName":"tilink-operation","type":"AWS_ENDPOINT_SERVICE","state":"CREATING","awsEndpointService":{"name":"service"}}`)
			return
		}
		fmt.Fprint(w, `{"privateLinkConnections":[{"privateLinkConnectionId":"plc-owned","clusterId":"cluster","displayName":"tilink-operation","type":"AWS_ENDPOINT_SERVICE","state":"CREATING","awsEndpointService":{"name":"service"}}]}`)
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	c.Kind = "dm"
	step := model.Step{Key: "tidb.private_link", Kind: "private_link", OperationID: "operation"}
	resources := []model.Resource{{Key: "aws.endpoint_service", Properties: map[string]string{"service_name": "service"}}}
	r, err := p.Apply(context.Background(), c, step, resources)
	if err == nil || r.ID != "plc-owned" || r.Ownership != model.Created {
		t.Fatalf("lost nonready resource: %+v %v", r, err)
	}
	c.TiDB.PrivateLinkID = "plc-owned"
	c.TiDB.ServiceName = "other-service"
	if _, err := p.Apply(context.Background(), c, step, resources); err == nil {
		t.Fatal("reused private link with incorrect service")
	}
}
func TestProviderDeleteWaitsUntilGoneAndRejectsOwnershipDrift(t *testing.T) {
	var deletes int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			deletes++
			fmt.Fprint(w, `{}`)
			return
		}
		fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","clusterId":"cluster","displayName":"someone-else","type":"AWS_ENDPOINT_SERVICE","state":"ACTIVE","awsEndpointService":{"name":"service"}}`)
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	r := model.Resource{Kind: "private_link", ID: "plc-one", Ownership: model.Created, OperationID: "operation", Properties: map[string]string{"cluster_id": "cluster", "project_id": "project", "display_name": "tilink-operation", "service_name": "service"}}
	if err := p.Delete(context.Background(), c, r); err == nil || deletes != 0 {
		t.Fatalf("ownership drift deleted resource: %d %v", deletes, err)
	}
	r.Ownership = model.Reused
	if err := p.Delete(context.Background(), c, r); err == nil {
		t.Fatal("reused resource deletion accepted")
	}
}

func TestProviderPendingAcceptanceUnlocksAWSAndDeleteAwaitsAbsence(t *testing.T) {
	var deleted bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			deleted = true
			fmt.Fprint(w, `{}`)
			return
		}
		if deleted {
			w.WriteHeader(404)
			return
		}
		fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","clusterId":"cluster","displayName":"tilink-operation","type":"AWS_ENDPOINT_SERVICE","state":"PENDING_ACCEPTANCE","awsEndpointService":{"name":"service","endpointId":"vpce-consumer"}}`)
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	c.Kind = "dm"
	c.TiDB.PrivateLinkID = "plc-one"
	step := model.Step{Key: "tidb.private_link", Kind: "private_link", OperationID: "operation"}
	r, err := p.Apply(context.Background(), c, step, []model.Resource{{Key: "aws.endpoint_service", Properties: map[string]string{"service_name": "service"}}})
	if err != nil || r.Status != "pending_acceptance" || r.Properties["aws_endpoint_id"] != "vpce-consumer" || r.Ownership != model.Reused {
		t.Fatalf("resource=%+v err=%v", r, err)
	}
	r.Ownership = model.Created
	if err := p.Delete(context.Background(), c, r); err != nil || !deleted {
		t.Fatalf("deletion not observed: %v", err)
	}
}

func TestProviderKafkaDomainsRecoveryAndCleanup(t *testing.T) {
	var attached, detached bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ":attachDomains") {
			attached = true
			fmt.Fprint(w, `{"attachDomainId":"domain-one","type":"TIDBCLOUD_MANAGED","uniqueName":"tilink-domain-operation","domains":[{"name":"sink.plc.tidbcloud.com","state":"ACTIVE"}]}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, ":detachDomains") {
			detached = true
			fmt.Fprint(w, `{}`)
			return
		}
		if attached && !detached {
			fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","attachedDomains":[{"attachDomainId":"domain-one","type":"TIDBCLOUD_MANAGED","uniqueName":"tilink-domain-operation","domains":[{"name":"sink.plc.tidbcloud.com","state":"ACTIVE"}]}]}`)
			return
		}
		fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","attachedDomains":[]}`)
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	c.Kind = "ticdc"
	c.Target.Type = "kafka"
	step := model.Step{Key: "tidb.domains", Kind: "private_link_domains", OperationID: "domain-operation", DependsOn: []string{"tidb.private_link"}}
	resources := []model.Resource{{Key: "tidb.private_link", ID: "plc-one"}}
	r, err := p.Apply(context.Background(), c, step, resources)
	if err != nil || r.ID != "domain-one" || !strings.Contains(r.Properties["domains"], "sink.plc.tidbcloud.com") {
		t.Fatalf("domains=%+v err=%v", r, err)
	}
	recovered, err := p.Apply(context.Background(), c, step, resources)
	if err != nil || recovered.ID != r.ID {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	if err := p.Delete(context.Background(), c, r); err != nil || !detached {
		t.Fatalf("detach failed: %v", err)
	}
}

func TestProviderRejectsMismatchedSuppliedRegistrationID(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
	}))
	defer s.Close()
	c := providerConfig()
	c.TiDB.EndpointID = "99"
	_, err := NewProvider(clientFor(t, s)).Apply(context.Background(), c, model.Step{Kind: "endpoint_registration"}, []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}})
	if err == nil {
		t.Fatal("silently ignored explicit registration ID")
	}
}

func TestProviderDeleteAcknowledgementDoesNotMeanDeleted(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			fmt.Fprint(w, `{}`)
			return
		}
		fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","clusterId":"cluster","displayName":"tilink-operation","type":"AWS_ENDPOINT_SERVICE","state":"DELETING","awsEndpointService":{"name":"service"}}`)
	}))
	defer s.Close()
	c := providerConfig()
	r := model.Resource{Kind: "private_link", ID: "plc-one", Ownership: model.Created, OperationID: "operation", Properties: map[string]string{"cluster_id": "cluster", "project_id": "project", "display_name": "tilink-operation", "service_name": "service"}}
	if err := NewProvider(clientFor(t, s)).Delete(context.Background(), c, r); err == nil {
		t.Fatal("asynchronous delete acknowledgement reported as cleanup complete")
	}
}

func TestRegistrationCreateUsesOneDiscoveryAndPreservesLostResponse(t *testing.T) {
	var lists, posts int
	var exists bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			lists++
			if exists {
				fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
			} else {
				fmt.Fprint(w, `{"endpoints":[]}`)
			}
			return
		}
		if r.Method == "POST" {
			posts++
			exists = true
			h, ok := w.(http.Hijacker)
			if !ok {
				t.Error("hijack unavailable")
				return
			}
			conn, _, err := h.Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if r.Method == "DELETE" {
			exists = false
			fmt.Fprint(w, `{}`)
		}
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	step := model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation"}
	dependencies := []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}}
	pending, err := p.Apply(context.Background(), c, step, dependencies)
	if err == nil || pending.ID == "" || pending.Properties["registration_create_pending"] != "true" || lists != 1 {
		t.Fatalf("pending=%+v err=%v lists=%d", pending, err, lists)
	}
	recovered, err := p.Apply(context.Background(), c, step, append(dependencies, pending))
	if err != nil || recovered.ID != "42" || recovered.Ownership != model.Created || posts != 1 {
		t.Fatalf("recovery=%+v err=%v posts=%d", recovered, err, posts)
	}
	if err := p.Delete(context.Background(), c, recovered); err != nil || exists {
		t.Fatalf("created registration leaked: %v", err)
	}
}

func TestRegistrationPriorUncertainOutcomeNeverAdoptsOrDeletes(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("mutated ambiguous registration")
		}
		fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
	}))
	defer s.Close()
	step := model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation", Properties: map[string]string{"prior_operation_status": "uncertain"}}
	r, err := NewProvider(clientFor(t, s)).Apply(context.Background(), providerConfig(), step, []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}})
	if err == nil || r.Ownership == model.Created || r.Ownership == model.Reused {
		t.Fatalf("silently adopted uncertain registration: %+v %v", r, err)
	}
}

func TestOperatorSuppliedPrivateLinkHandoffRequiresNoUnverifiedCalls(t *testing.T) {
	c := providerConfig()
	c.Kind = "ticdc"
	c.Target.Type = "kafka"
	c.TiDB.PrivateLinkID = "plc-manual"
	c.TiDB.EndpointID = "vpce-manual"
	c.TiDB.ServiceName = "service"
	c.TiDB.Host = "sink.plc.tidbcloud.com"
	client, _ := NewClient(Options{PublicKey: "public", PrivateKey: "secret"})
	p := NewProvider(client)
	steps, err := p.Plan(context.Background(), c)
	if err != nil || len(steps) != 2 || steps[0].Manual != "" || steps[1].Manual != "" {
		t.Fatalf("manual handoff cannot resume: %+v %v", steps, err)
	}
	r, err := p.Apply(context.Background(), c, model.Step{Key: "tidb.private_link", Kind: "private_link"}, []model.Resource{{Key: "aws.endpoint_service", Properties: map[string]string{"service_name": "service"}}})
	if err != nil || r.Ownership != model.Reused || r.Properties["operator_supplied"] != "true" || r.Status != "ready" {
		t.Fatalf("handoff=%+v err=%v", r, err)
	}
	if _, err := p.Refresh(context.Background(), c, r); err != nil {
		t.Fatal(err)
	}
	domains, err := p.Apply(context.Background(), c, model.Step{Key: "tidb.domains", Kind: "private_link_domains"}, []model.Resource{r})
	if err != nil || domains.Ownership != model.Reused || !strings.Contains(domains.Properties["domains"], c.TiDB.Host) {
		t.Fatalf("domain handoff=%+v err=%v", domains, err)
	}
}

func TestRegistrationConflictDoesNotClaimExternalResource(t *testing.T) {
	var lists int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			lists++
			fmt.Fprint(w, `{"endpoints":[]}`)
			return
		}
		if r.Method != "POST" {
			t.Error("unexpected mutation")
		}
		w.WriteHeader(409)
		fmt.Fprint(w, `{"message":"already exists"}`)
	}))
	defer s.Close()
	r, err := NewProvider(clientFor(t, s)).Apply(context.Background(), providerConfig(), model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation"}, []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}})
	if err == nil || r.ID != "" || r.Properties["registration_create_pending"] != "" || lists != 1 {
		t.Fatalf("external conflict claimed: %+v %v lists=%d", r, err, lists)
	}
}

func TestDestroyPendingRegistrationResolvesInternalID(t *testing.T) {
	var exists = true
	var deleted string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if exists {
				fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
			} else {
				fmt.Fprint(w, `{"endpoints":[]}`)
			}
			return
		}
		if r.Method == "DELETE" {
			deleted = r.URL.Path
			exists = false
			fmt.Fprint(w, `{}`)
			return
		}
		t.Error("unexpected request")
	}))
	defer s.Close()
	r := model.Resource{Key: "tidb.registration", Kind: "endpoint_registration", ID: "vpce-one", Ownership: model.Created, OperationID: "operation", Properties: map[string]string{"project_id": "project", "cluster_id": "cluster", "aws_endpoint_id": "vpce-one", "registration_create_pending": "true"}}
	if err := NewProvider(clientFor(t, s)).Delete(context.Background(), providerConfig(), r); err != nil || !strings.HasSuffix(deleted, "/42") {
		t.Fatalf("pending registration leaked or wrong delete ID: %s %v", deleted, err)
	}
}

func TestRegistrationInterruptedPendingOperationNeverAdopts(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
	}))
	defer s.Close()
	step := model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation", Properties: map[string]string{"prior_operation_status": "pending"}}
	if _, err := NewProvider(clientFor(t, s)).Apply(context.Background(), providerConfig(), step, []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}}); err == nil {
		t.Fatal("silently adopted interrupted registration operation")
	}
}

func TestRegistrationPreparationCheckpointsAbsenceWithoutMutation(t *testing.T) {
	var calls int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" {
			t.Error("preflight mutated cloud state")
		}
		fmt.Fprint(w, `{"endpoints":[]}`)
	}))
	defer s.Close()
	step := model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation"}
	prepared, err := NewProvider(clientFor(t, s)).PrepareStep(context.Background(), providerConfig(), step, []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}})
	if err != nil || calls != 1 || prepared.Properties["registration_disposition"] != "absent" || prepared.Properties["aws_endpoint_id"] != "vpce-one" {
		t.Fatalf("checkpoint=%+v err=%v calls=%d", prepared, err, calls)
	}
}
func TestRegistrationPreparationPreservesUncertainSnapshot(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("uncertain snapshot was replaced by fresh discovery")
	}))
	defer s.Close()
	step := model.Step{Kind: "endpoint_registration", Properties: map[string]string{"prior_operation_status": "uncertain", "registration_disposition": "absent", "aws_endpoint_id": "vpce-one"}}
	prepared, err := NewProvider(clientFor(t, s)).PrepareStep(context.Background(), providerConfig(), step, nil)
	if err != nil || prepared.Properties["registration_disposition"] != "absent" {
		t.Fatalf("snapshot lost: %+v %v", prepared, err)
	}
}
func TestDefiniteAPIErrorsDistinguishUnknownMutations(t *testing.T) {
	if (&APIError{StatusCode: 403}).MutationUncertain() {
		t.Fatal("permission rejection treated as possibly created")
	}
	if !(&APIError{StatusCode: 503}).MutationUncertain() {
		t.Fatal("server error treated as definitely not created")
	}
}

func TestRegistrationSnapshotRejectsConcurrentExternalCreation(t *testing.T) {
	var lists int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("mutated concurrent external registration")
		}
		lists++
		if lists == 1 {
			fmt.Fprint(w, `{"endpoints":[]}`)
		} else {
			fmt.Fprint(w, `{"endpoints":[{"id":"external","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
		}
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	dependencies := []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}}
	ctx := context.Background()
	step, err := p.PrepareStep(ctx, c, model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation"}, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.Apply(ctx, c, step, dependencies)
	if err == nil || r.Ownership == model.Created || r.Ownership == model.Reused {
		t.Fatalf("concurrent registration silently adopted: %+v %v", r, err)
	}
}
func TestRegistrationSnapshotCanResumeProvenReuse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("mutated reused registration")
		}
		fmt.Fprint(w, `{"endpoints":[{"id":"external","endpoint_name":"vpce-one","status":"ACTIVE"}]}`)
	}))
	defer s.Close()
	p := NewProvider(clientFor(t, s))
	c := providerConfig()
	dependencies := []model.Resource{{Key: "aws.endpoint", ID: "vpce-one"}}
	ctx := context.Background()
	step, err := p.PrepareStep(ctx, c, model.Step{Key: "tidb.registration", Kind: "endpoint_registration", OperationID: "operation"}, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	step.Properties["prior_operation_status"] = "pending"
	r, err := p.Apply(ctx, c, step, dependencies)
	if err != nil || r.Ownership != model.Reused || r.ID != "external" {
		t.Fatalf("proven reuse could not resume: %+v %v", r, err)
	}
}
