package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tenfyzhong/tilink/internal/engine"
	"github.com/tenfyzhong/tilink/internal/model"
)

type memoryStore struct{ states []model.State }

func (s memoryStore) List() ([]model.State, error) { return s.states, nil }
func (s memoryStore) Load(name string) (*model.State, error) {
	for _, v := range s.states {
		if v.Config.Name == name {
			return &v, nil
		}
	}
	return nil, os.ErrNotExist
}

func TestShowOfflineIncludesConfigurationFilesAndOwnership(t *testing.T) {
	s := model.State{ConnectionID: "conn-123", Config: model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "ap-northeast-1"}}, UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Resources: []model.Resource{{Key: "nlb", ID: "arn:aws:lb", Ownership: model.Created}}, GeneratedFiles: []model.GeneratedFile{{Path: "kafka.properties", Content: "advertised.listeners=PRIVATE://broker:9094"}}}
	var out bytes.Buffer
	cmd := New(Options{Store: memoryStore{[]model.State{s}}, Out: &out, Err: &out})
	cmd.SetArgs([]string{"show", "orders", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var got []model.State
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v: %s", err, out.String())
	}
	if len(got) != 1 || got[0].GeneratedFiles[0].Path != "kafka.properties" || got[0].Resources[0].Ownership != model.Created {
		t.Fatalf("missing inventory: %s", out.String())
	}
}
func TestDestroyRejectsNonTerminalBeforeOpeningServices(t *testing.T) {
	opened := false
	cmd := New(Options{In: strings.NewReader("y\nconn-123\n"), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, IsTerminal: func() bool { return false }, Open: func(string) (Service, StateStore, error) { opened = true; return nil, nil, nil }})
	cmd.SetArgs([]string{"destroy", "orders"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("expected terminal error, got %v", err)
	}
	if opened {
		t.Fatal("opened cloud services before terminal validation")
	}
}
func TestInitWritesValidatedPrivateConfigurationAndDoesNotOverwrite(t *testing.T) {
	file := filepath.Join(t.TempDir(), "tilink.yaml")
	input := "orders\ndm\ncustomer\nap-northeast-1\n123456789012\nvpc-123\nsubnet-a,subnet-b\nproject\ncluster\nrds\norders.rds.amazonaws.com\n3306\nsg-db\narn:aws:iam::123:root\n"
	var out bytes.Buffer
	cmd := New(Options{In: strings.NewReader(input), Out: &out, Err: &out, IsTerminal: func() bool { return true }})
	cmd.SetArgs([]string{"init", "-f", file})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := model.DecodeConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "orders" || len(c.AWS.SubnetIDs) != 2 || c.TiDB.PrivateKeyEnv != "TIDB_PRIVATE_KEY" {
		t.Fatalf("bad config: %+v", c)
	}
	info, _ := os.Stat(file)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("unsafe mode: %v", info.Mode())
	}
	cmd = New(Options{In: strings.NewReader(input), Out: &out, Err: &out, IsTerminal: func() bool { return true }})
	cmd.SetArgs([]string{"init", "-f", file})
	if err := cmd.Execute(); err == nil {
		t.Fatal("overwrote existing file")
	}
}

type fakeService struct {
	planFn       func(context.Context, model.Config) (model.Plan, error)
	destroyFn    func(context.Context, string, engine.Confirmer) (*model.State, error)
	refreshFn    func(context.Context, string) (*model.State, error)
	applyState   *model.State
	verification model.Verification
}

func (f fakeService) Plan(ctx context.Context, c model.Config) (model.Plan, error) {
	if f.planFn != nil {
		return f.planFn(ctx, c)
	}
	return model.Plan{}, nil
}
func (f fakeService) Apply(context.Context, model.Config) (*model.State, error) {
	return f.applyState, nil
}
func (f fakeService) Refresh(ctx context.Context, n string) (*model.State, error) {
	if f.refreshFn != nil {
		return f.refreshFn(ctx, n)
	}
	return &model.State{}, nil
}
func (f fakeService) Destroy(ctx context.Context, n string, c engine.Confirmer) (*model.State, error) {
	return f.destroyFn(ctx, n, c)
}
func (f fakeService) Doctor(context.Context, string) ([]model.Finding, error) { return nil, nil }
func (f fakeService) Verify(context.Context, string) (model.Verification, error) {
	return f.verification, nil
}
func (f fakeService) Reconcile(context.Context, string) (*model.State, error) {
	return &model.State{}, nil
}

func TestDestroyRequiresBothIndependentConfirmations(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		wantDelete  bool
	}{{"decline", "n\n", false}, {"first EOF", "", false}, {"second EOF", "y\n", false}, {"wrong ID", "y\nwrong\n", false}, {"full confirmation", "y\nconn-123\n", true}} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := false
			service := fakeService{destroyFn: func(ctx context.Context, name string, c engine.Confirmer) (*model.State, error) {
				yes, err := c.ConfirmPlan(engine.DestroyPlan{ConnectionID: "conn-123"})
				if err != nil {
					return nil, err
				}
				if !yes {
					return nil, errors.New("cancelled")
				}
				id, err := c.ConfirmID("conn-123")
				if err != nil {
					return nil, err
				}
				if id != "conn-123" {
					return nil, errors.New("incorrect connection ID")
				}
				deleted = true
				return &model.State{Status: "destroyed"}, nil
			}}
			var out, prompts bytes.Buffer
			cmd := New(Options{Engine: service, Store: memoryStore{}, In: strings.NewReader(tc.input), Out: &out, Err: &prompts, IsTerminal: func() bool { return true }})
			cmd.SetArgs([]string{"destroy", "orders"})
			err := cmd.Execute()
			if deleted != tc.wantDelete {
				t.Fatalf("deleted=%v wanted %v", deleted, tc.wantDelete)
			}
			if tc.wantDelete && err != nil {
				t.Fatal(err)
			}
			if !tc.wantDelete && err == nil {
				t.Fatal("refused confirmation reported success")
			}
		})
	}
}
func TestShowRefreshUsesCloudObservations(t *testing.T) {
	refreshed := false
	service := fakeService{refreshFn: func(context.Context, string) (*model.State, error) {
		refreshed = true
		return &model.State{Status: "drifted", Config: model.Config{Name: "orders"}}, nil
	}}
	var out bytes.Buffer
	cmd := New(Options{Engine: service, Store: memoryStore{[]model.State{{Config: model.Config{Name: "orders"}}}}, Out: &out})
	cmd.SetArgs([]string{"show", "--refresh", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !refreshed || !strings.Contains(out.String(), "drifted") {
		t.Fatalf("missing cloud observation %s", out.String())
	}
}
func TestPartialVerificationReturnsFailureWithStructuredEvidence(t *testing.T) {
	var out bytes.Buffer
	cmd := New(Options{Engine: fakeService{verification: model.Verification{Status: "partial", Uncovered: []string{"TiDB Cloud outbound path"}}}, Store: memoryStore{}, Out: &out})
	cmd.SetArgs([]string{"verify", "orders", "--json"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("partial verification reported success")
	}
	var got model.Verification
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Uncovered) != 1 {
		t.Fatal("missing uncovered evidence")
	}
}
func TestDestroyHasNoConfirmationBypassFlags(t *testing.T) {
	for _, flag := range []string{"--yes", "--force", "--json"} {
		cmd := New(Options{In: strings.NewReader("y\nconn-123\n"), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, IsTerminal: func() bool { return true }})
		cmd.SetArgs([]string{"destroy", "orders", flag})
		if err := cmd.Execute(); err == nil {
			t.Fatalf("accepted bypass %s", flag)
		}
	}
}
func TestShowHumanIncludesResourceCleanupAndGeneratedContent(t *testing.T) {
	var out bytes.Buffer
	s := model.State{Config: model.Config{Name: "orders"}, Resources: []model.Resource{{Key: "database", ID: "db-1", Ownership: model.Reused}, {Key: "rule", ID: "rule-1", Ownership: model.Modified}}, GeneratedFiles: []model.GeneratedFile{{Path: "broker.properties", Content: "listeners=PRIVATE://:9094"}}}
	cmd := New(Options{Store: memoryStore{[]model.State{s}}, Out: &out})
	cmd.SetArgs([]string{"show"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"saved state", "preserve external resource", "revert exact unchanged", "broker.properties", "listeners=PRIVATE://:9094"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
}
func TestInitRejectsNoninteractiveAndJSONWithoutCreatingFile(t *testing.T) {
	for _, jsonFlag := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "tilink.yaml")
		cmd := New(Options{In: strings.NewReader(""), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, IsTerminal: func() bool { return jsonFlag }})
		args := []string{"init", "-f", path}
		if jsonFlag {
			args = append(args, "--json")
		}
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil {
			t.Fatal("accepted noninteractive init")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("wrote rejected config")
		}
	}
}

func TestInitCollectsKafkaBrokerMappings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kafka.yaml")
	input := "events\nticdc\n\nap-northeast-1\n123456789012\nvpc-a\nsubnet-a,subnet-b\nproject\ncluster\nkafka\nbootstrap.internal\n9092\nsg-kafka\narn:aws:iam::123:root\n2\n1\nbroker1.internal\n9092\n19092\n2\nbroker2.internal\n9092\n29092\n"
	cmd := New(Options{In: strings.NewReader(input), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, IsTerminal: func() bool { return true }})
	cmd.SetArgs([]string{"init", "-f", path})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := model.DecodeConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Target.Brokers) != 2 || c.Target.Brokers[1].ListenerPort != 29092 {
		t.Fatalf("missing broker mappings: %+v", c.Target.Brokers)
	}
}
func TestInitCollectsApplicationAccessSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	input := "app\napp\n\nap-northeast-1\n123456789012\nvpc-a\nsubnet-a\nproject\ncluster\nsg-app\n10.0.0.0/24\n"
	cmd := New(Options{In: strings.NewReader(input), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, IsTerminal: func() bool { return true }})
	cmd.SetArgs([]string{"init", "-f", path})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	c, err := model.DecodeConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.AWS.SecurityGroupIDs) != 1 || len(c.AWS.ClientCIDRs) != 1 {
		t.Fatal("application access sources missing")
	}
}
func TestShowHumanDisplaysStatePath(t *testing.T) {
	var out bytes.Buffer
	cmd := New(Options{Store: memoryStore{[]model.State{{Config: model.Config{Name: "orders"}}}}, StateDir: "/state/tilink", Out: &out})
	cmd.SetArgs([]string{"show"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "/state/tilink/orders.json") {
		t.Fatal("state file path missing")
	}
}
