package engine

import (
	"context"
	"errors"
	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/state"
	"testing"
)

type fake struct {
	steps     []model.Step
	calls     []string
	fail      string
	applyFail bool
	partial   bool
	pending   bool
}

func (f *fake) Plan(context.Context, model.Config) ([]model.Step, error) { return f.steps, nil }
func (f *fake) Apply(_ context.Context, _ model.Config, s model.Step, _ []model.Resource) (model.Resource, error) {
	f.calls = append(f.calls, "apply:"+s.Key)
	if f.applyFail {
		if f.partial {
			return model.Resource{Key: s.Key, ID: "partial", Provider: "test", Ownership: model.Created, Status: "pending"}, errors.New("waiting")
		}
		return model.Resource{}, errors.New("ambiguous")
	}
	return model.Resource{Key: s.Key, ID: s.OperationID, Provider: "test", Ownership: model.Created, DependsOn: s.DependsOn, Status: "ready"}, nil
}
func (f *fake) Refresh(_ context.Context, _ model.Config, r model.Resource) (model.Resource, error) {
	if f.pending {
		r.Status = "pending"
	} else {
		r.Status = "ready"
	}
	return r, nil
}
func (f *fake) Delete(_ context.Context, _ model.Config, r model.Resource) error {
	f.calls = append(f.calls, "delete:"+r.Key)
	if r.Key == f.fail {
		return errors.New("blocked")
	}
	return nil
}

type confirm struct {
	yes  bool
	id   string
	hook func()
}

func (c confirm) ConfirmPlan(DestroyPlan) (bool, error) { return c.yes, nil }
func (c confirm) ConfirmID(string) (string, error) {
	if c.hook != nil {
		c.hook()
	}
	return c.id, nil
}
func config() model.Config {
	return model.Config{Version: 1, Name: "demo", Kind: "app", Provider: "aws", AWS: model.AWSConfig{Region: "us-east-1", AccountID: "123456789012", VPCID: "vpc-a", SubnetIDs: []string{"a"}}, TiDB: model.TiDBConfig{ProjectID: "p", ClusterID: "c"}}
}
func TestApplyResumesStableOperation(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "a", Provider: "test"}}, applyFail: true}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, err := e.Apply(context.Background(), config())
	if err == nil {
		t.Fatal("expected failure")
	}
	id := v.Operations[0].ID
	f.applyFail = false
	v, err = e.Apply(context.Background(), config())
	if err != nil || v.Resources[0].ID != id {
		t.Fatal(v, err)
	}
	_, err = e.Apply(context.Background(), config())
	if err != nil || len(f.calls) != 2 {
		t.Fatal(f.calls, err)
	}
}

func TestSeparateStateDirectoriesCannotShareOwnership(t *testing.T) {
	ctx := context.Background()
	first := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": &fake{steps: []model.Step{{Key: "owned", Provider: "test"}}}}}
	second := Engine{Store: state.New(t.TempDir()), Providers: first.Providers}
	a, err := first.Apply(ctx, config())
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.Apply(ctx, config())
	if err != nil {
		t.Fatal(err)
	}
	if a.ConnectionID == "" || a.ConnectionID == b.ConnectionID {
		t.Fatal("independent state directories share cloud deletion ownership")
	}
	if a.Operations[0].ID == b.Operations[0].ID {
		t.Fatal("independent connections share operation recovery identity")
	}
	plan, err := first.Plan(ctx, config())
	if err != nil || plan.ConnectionID != a.ConnectionID {
		t.Fatalf("existing plan lost saved connection identity: %v %v", plan, err)
	}
	resumed, err := first.Apply(ctx, config())
	if err != nil || resumed.ConnectionID != a.ConnectionID {
		t.Fatalf("resume changed connection identity: %v %v", resumed, err)
	}
}

func TestNewReadOnlyPlanDoesNotReserveOwnership(t *testing.T) {
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{}}
	plan, err := e.Plan(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if plan.ConnectionID != "" {
		t.Fatal("new read-only plan pretends to own a persisted connection")
	}
	states, err := e.Store.List()
	if err != nil || len(states) != 0 {
		t.Fatalf("read-only plan created state: %v %v", states, err)
	}
}
func TestDestroyTwoConfirmationsAndPartialCleanup(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "parent", Provider: "test"}, {Key: "child", Provider: "test", DependsOn: []string{"parent"}}, {Key: "other", Provider: "test"}}, fail: "child"}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, _ := e.Apply(context.Background(), config())
	if _, err := e.Destroy(context.Background(), "demo", confirm{yes: true, id: "wrong"}); !errors.Is(err, ErrCancelled) {
		t.Fatal(err)
	}
	if len(f.calls) != 3 {
		t.Fatal("deleted on wrong confirmation")
	}
	_, err := e.Destroy(context.Background(), "demo", confirm{yes: true, id: v.ConnectionID})
	if err == nil {
		t.Fatal("cleanup falsely complete")
	}
	for _, c := range f.calls {
		if c == "delete:parent" {
			t.Fatal("deleted failed child's dependency")
		}
	}
}
func TestImmutableUpdateRejected(t *testing.T) {
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{}}
	c := config()
	_, _ = e.Apply(context.Background(), c)
	c.AWS.VPCID = "different"
	if _, err := e.Apply(context.Background(), c); err == nil {
		t.Fatal("immutable update accepted")
	}
}

func TestStaleConfirmationDoesNotDelete(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "a", Provider: "test"}}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, _ := e.Apply(context.Background(), config())
	_, err := e.Destroy(context.Background(), "demo", confirm{yes: true, id: v.ConnectionID, hook: func() {
		latest, _ := e.Store.Load("demo")
		if err := e.Store.Save(latest, latest.Revision); err != nil {
			t.Fatal(err)
		}
	}})
	if !errors.Is(err, ErrStalePlan) || len(f.calls) != 1 {
		t.Fatal(f.calls, err)
	}
}
func TestPreservedChildProtectsManagedParent(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "parent", Provider: "test"}, {Key: "child", Provider: "test", DependsOn: []string{"parent"}}}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, _ := e.Apply(context.Background(), config())
	v.Resources[1].Ownership = model.Reused
	if err := e.Store.Save(v, v.Revision); err != nil {
		t.Fatal(err)
	}
	_, err := e.Destroy(context.Background(), "demo", confirm{yes: true, id: v.ConnectionID})
	if err == nil {
		t.Fatal("shared dependency falsely deleted")
	}
	for _, call := range f.calls {
		if call == "delete:parent" {
			t.Fatal("deleted dependency of preserved resource")
		}
	}
}
func TestManualActionResumesWithoutRecreating(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "a", Provider: "test"}, {Key: "manual", Provider: "test", Manual: "Return endpoint_id"}}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, err := e.Apply(context.Background(), config())
	if !errors.Is(err, ErrWaitingAction) || v.Status != "waiting_action" {
		t.Fatal(v, err)
	}
	f.steps = f.steps[:1]
	c := config()
	c.TiDB.EndpointID = "e"
	v, err = e.Apply(context.Background(), c)
	if err != nil || len(f.calls) != 1 || v.Status != "ready" {
		t.Fatal(v, err, f.calls)
	}
}
func TestUncertainOperationBlocksDestroy(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "a", Provider: "test"}}, applyFail: true}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, _ := e.Apply(context.Background(), config())
	v, err := e.Destroy(context.Background(), "demo", confirm{yes: true, id: v.ConnectionID})
	if err == nil || v.Status == "destroyed" {
		t.Fatal("uncertain operation lost")
	}
}
func TestExistingManualValueCannotBeReplaced(t *testing.T) {
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{}}
	c := config()
	c.TiDB.ServiceName = "service-a"
	_, err := e.Apply(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	c.TiDB.ServiceName = "service-b"
	if _, err = e.Apply(context.Background(), c); err == nil {
		t.Fatal("replaced existing service")
	}
}

func TestPartialResourcePersistedAndPendingNotReady(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "a", Provider: "test"}}, applyFail: true, partial: true, pending: true}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, err := e.Apply(context.Background(), config())
	if err == nil || len(v.Resources) != 1 {
		t.Fatal(v, err)
	}
	f.applyFail = false
	v, err = e.Apply(context.Background(), config())
	if err == nil || v.Status == "ready" || len(f.calls) != 2 {
		t.Fatal(v, err, f.calls)
	}
	f.pending = false
	v, err = e.Apply(context.Background(), config())
	if err != nil || v.Status != "ready" || len(f.calls) != 2 {
		t.Fatal(v, err, f.calls)
	}
}

type acceptanceProvider struct {
	fake
	accepted bool
}

func (f *acceptanceProvider) Apply(ctx context.Context, c model.Config, s model.Step, r []model.Resource) (model.Resource, error) {
	result, err := f.fake.Apply(ctx, c, s, r)
	if s.Key == "endpoint" {
		result.Status = "pending_acceptance"
	}
	if s.Key == "registration" {
		f.accepted = true
	}
	return result, err
}
func (f *acceptanceProvider) Refresh(_ context.Context, _ model.Config, r model.Resource) (model.Resource, error) {
	if r.Key == "endpoint" && !f.accepted {
		r.Status = "pending_acceptance"
	} else {
		r.Status = "ready"
	}
	return r, nil
}
func TestAcceptanceDependenciesCompleteBeforeFinalReadiness(t *testing.T) {
	f := &acceptanceProvider{fake: fake{steps: []model.Step{{Key: "endpoint", Provider: "test"}, {Key: "registration", Provider: "test", DependsOn: []string{"endpoint"}}}}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, err := e.Apply(context.Background(), config())
	if err != nil || v.Status != "ready" || !f.accepted {
		t.Fatal(v, err)
	}
}
func TestEveryConfirmationCancellationPreservesResources(t *testing.T) {
	for _, c := range []confirm{{yes: false}, {yes: true, id: ""}, {yes: true, id: "incorrect"}} {
		f := &fake{steps: []model.Step{{Key: "a", Provider: "test"}}}
		e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
		_, err := e.Apply(context.Background(), config())
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.Destroy(context.Background(), "demo", c)
		if !errors.Is(err, ErrCancelled) || len(f.calls) != 1 {
			t.Fatal(err, f.calls)
		}
	}
}
func TestPlanRejectsCyclesAndMissingDependencies(t *testing.T) {
	for _, steps := range [][]model.Step{{{Key: "a", Provider: "test", DependsOn: []string{"b"}}}, {{Key: "a", Provider: "test", DependsOn: []string{"b"}}, {Key: "b", Provider: "test", DependsOn: []string{"a"}}}} {
		e := Engine{Providers: map[string]Provider{"test": &fake{steps: steps}}}
		if _, err := e.Plan(context.Background(), config()); err == nil {
			t.Fatal("accepted invalid graph")
		}
	}
}

type provenanceProvider struct {
	fake
	prior   []string
	missing bool
}

func (f *provenanceProvider) Apply(ctx context.Context, c model.Config, s model.Step, r []model.Resource) (model.Resource, error) {
	f.prior = append(f.prior, s.Properties["prior_operation_status"])
	return f.fake.Apply(ctx, c, s, r)
}
func (f *provenanceProvider) Refresh(_ context.Context, _ model.Config, r model.Resource) (model.Resource, error) {
	if f.missing {
		r.Status = "deleted"
	} else {
		r.Status = "ready"
	}
	return r, nil
}
func TestRetrySuppliesPreviousOperationStatus(t *testing.T) {
	f := &provenanceProvider{fake: fake{steps: []model.Step{{Key: "a", Provider: "test"}}, applyFail: true}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	_, _ = e.Apply(context.Background(), config())
	f.applyFail = false
	if _, err := e.Apply(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if len(f.prior) != 2 || f.prior[0] != "" || f.prior[1] != "uncertain" {
		t.Fatal(f.prior)
	}
}
func TestObservedDeletedRequiredStepIsReapplied(t *testing.T) {
	f := &provenanceProvider{fake: fake{steps: []model.Step{{Key: "a", Provider: "test"}}}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	_, _ = e.Apply(context.Background(), config())
	f.missing = true
	_, _ = e.Apply(context.Background(), config())
	if len(f.calls) != 2 {
		t.Fatal("required resource not reapplied", f.calls)
	}
}
func TestDataflowOriginCanBeFilledOnlyBeforeResources(t *testing.T) {
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{}}
	c := config()
	_, err := e.Apply(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	c.TiDB.DataflowBaseURL = "https://data.example.com"
	if _, err = e.Apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	c.TiDB.DataflowBaseURL = "https://other.example.com"
	if _, err = e.Apply(context.Background(), c); err == nil {
		t.Fatal("moved API origin")
	}
}

type checkpointProvider struct {
	fake
	prepares int
	seen     []string
}

func (f *checkpointProvider) PrepareStep(_ context.Context, _ model.Config, s model.Step, _ []model.Resource) (model.Step, error) {
	f.prepares++
	s.Properties["disposition"] = "absent"
	return s, nil
}
func (f *checkpointProvider) Apply(ctx context.Context, c model.Config, s model.Step, r []model.Resource) (model.Resource, error) {
	f.seen = append(f.seen, s.Properties["disposition"])
	return f.fake.Apply(ctx, c, s, r)
}
func TestPreparationCheckpointPersistsAcrossUncertainRetry(t *testing.T) {
	f := &checkpointProvider{fake: fake{steps: []model.Step{{Key: "a", Provider: "test"}}, applyFail: true}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, _ := e.Apply(context.Background(), config())
	if v.Operations[0].Step.Properties["disposition"] != "absent" {
		t.Fatal(v)
	}
	f.applyFail = false
	_, err := e.Apply(context.Background(), config())
	if err != nil || f.prepares != 1 || len(f.seen) != 2 || f.seen[1] != "absent" {
		t.Fatal(err, f.prepares, f.seen)
	}
}
func TestFinalRefreshCannotReportMissingRequiredResourceReady(t *testing.T) {
	f := &provenanceProvider{fake: fake{steps: []model.Step{{Key: "a", Provider: "test"}}}, missing: true}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	v, err := e.Apply(context.Background(), config())
	if err == nil || v.Status == "ready" {
		t.Fatal("missing required resource reported ready")
	}
}
func TestInitialDataflowURLAfterAWSOnlyWaitingState(t *testing.T) {
	f := &fake{steps: []model.Step{{Key: "aws", Provider: "test"}, {Key: "manual", Provider: "test", Manual: "Configure dataflow_base_url"}}}
	e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{"test": f}}
	c := config()
	v, err := e.Apply(context.Background(), c)
	if !errors.Is(err, ErrWaitingAction) {
		t.Fatal(v, err)
	}
	f.steps = f.steps[:1]
	c.TiDB.DataflowBaseURL = "https://data.example.com"
	if v, err = e.Apply(context.Background(), c); err != nil || v.Status != "ready" {
		t.Fatal(v, err)
	}
}
func TestInitialDataflowURLRejectedAfterTiDBHistory(t *testing.T) {
	for _, kind := range []string{"resource", "operation"} {
		t.Run(kind, func(t *testing.T) {
			e := Engine{Store: state.New(t.TempDir()), Providers: map[string]Provider{}}
			c := config()
			v, err := e.Apply(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "resource" {
				v.Resources = append(v.Resources, model.Resource{Key: "tidb.r", Provider: "tidb", ID: "r", Ownership: model.Created})
			} else {
				v.Operations = append(v.Operations, model.Operation{ID: "op", Step: model.Step{Key: "tidb.op", Provider: "tidb"}, Status: "uncertain"})
			}
			if err = e.Store.Save(v, v.Revision); err != nil {
				t.Fatal(err)
			}
			c.TiDB.DataflowBaseURL = "https://data.example.com"
			if _, err = e.Apply(context.Background(), c); err == nil {
				t.Fatal("moved TiDB origin after remote operation")
			}
		})
	}
}
