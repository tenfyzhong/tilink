package aws

import (
	"context"
	"github.com/tenfyzhong/tilink/internal/model"
	"testing"
)

func TestTopologyIncludesStandbyAndOwnership(t *testing.T) {
	c := model.Config{Name: "orders", Kind: "dm", AWS: model.AWSConfig{Region: "us-east-1", AccountID: "123", VPCID: "vpc-1", SubnetIDs: []string{"subnet-1"}}, TiDB: model.TiDBConfig{PrincipalARN: "arn:aws:iam::456:root"}, Target: model.TargetConfig{Type: "mysql", Host: "db.internal", Port: 3306, SecurityGroupID: "sg-db"}}
	steps, err := networkPlan(c)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, s := range steps {
		kinds[s.Kind]++
		if s.Provider != "aws" {
			t.Fatal(s)
		}
	}
	if kinds["target_group"] != 2 || kinds["listener"] != 2 || kinds["service_permission"] != 1 || kinds["security_group_rule"] != 1 {
		t.Fatal(kinds)
	}
}
func TestDeleteRefusesUnownedBeforeAPI(t *testing.T) {
	p := &Provider{}
	for _, r := range []model.Resource{{Kind: "nlb", Ownership: model.Reused}, {Kind: "nlb", Ownership: model.Created, Properties: map[string]string{"account_id": "other"}}} {
		if err := p.Delete(context.Background(), model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123"}}, r); err == nil {
			t.Fatal("unowned deletion accepted")
		}
	}
}
func TestResolveDependencyRequiresRecordedID(t *testing.T) {
	if _, err := resourceID(nil, "missing"); err == nil {
		t.Fatal("missing dependency accepted")
	}
}
func TestApplicationRequiresClientAccessScope(t *testing.T) {
	_, err := networkPlan(model.Config{Kind: "app", TiDB: model.TiDBConfig{ServiceName: "service"}})
	if err == nil {
		t.Fatal("unscoped endpoint accepted")
	}
}
func TestNamesRemainDistinctWhenConnectionNameIsLong(t *testing.T) {
	c := model.Config{Name: "this-is-a-very-long-connection-name"}
	if name(c, "aws.tg.primary") == name(c, "aws.tg.primary.standby") {
		t.Fatal("resource names collide")
	}
}
func TestKafkaHasStandbyForEveryBroker(t *testing.T) {
	c := model.Config{Kind: "ticdc", TiDB: model.TiDBConfig{PrincipalARN: "arn"}, Target: model.TargetConfig{Type: "kafka", Host: "bootstrap", Port: 9092, Brokers: []model.Broker{{ID: "1", Host: "one", Port: 9092, ListenerPort: 19092}}}}
	steps, e := networkPlan(c)
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, s := range steps {
		if s.Kind == "target_group" {
			n++
		}
	}
	if n != 4 {
		t.Fatal(n)
	}
}
func TestApplicationRejectsPublicClientCIDR(t *testing.T) {
	_, e := networkPlan(model.Config{Kind: "app", AWS: model.AWSConfig{ClientCIDRs: []string{"0.0.0.0/0"}}, TiDB: model.TiDBConfig{ServiceName: "service"}})
	if e == nil {
		t.Fatal("public access accepted")
	}
}
func TestWaitUntilAbsentPollsAndHonorsCancellation(t *testing.T) {
	calls := 0
	if e := waitUntilAbsent(context.Background(), 0, func() (bool, error) { calls++; return calls == 3, nil }); e != nil || calls != 3 {
		t.Fatal(calls, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := waitUntilAbsent(ctx, 0, func() (bool, error) { return false, nil }); e == nil {
		t.Fatal("cancellation ignored")
	}
}
func TestApplicationProbeUsesOnlyOwnedSelfRule(t *testing.T) {
	for _, probe := range []bool{false, true} {
		c := model.Config{Kind: "app", Probe: probe, AWS: model.AWSConfig{SecurityGroupIDs: []string{"sg-client"}}, TiDB: model.TiDBConfig{ServiceName: "service", Port: 4500}}
		steps, e := networkPlan(c)
		if e != nil {
			t.Fatal(e)
		}
		found := false
		for _, s := range steps {
			if s.Key == "aws.probe.ingress" {
				found = true
				if s.Kind != "security_group_rule" || s.Properties["group_key"] != "aws.network.sg" || s.Properties["source_key"] != "aws.network.sg" || s.Properties["port"] != "4500" || s.Properties["group_id"] != "" || s.Properties["cidrs"] != "" || s.Properties["sources"] != "" {
					t.Fatal(s)
				}
			}
			if s.Key == "aws.endpoint" && probe {
				has := false
				for _, d := range s.DependsOn {
					has = has || d == "aws.probe.ingress"
				}
				if !has {
					t.Fatal("endpoint does not depend on probe ingress")
				}
			}
		}
		if found != probe {
			t.Fatal("unexpected probe ingress", probe, found)
		}
	}
}
func TestHealthListenersBindStandbyAndAvoidConfiguredPorts(t *testing.T) {
	c := model.Config{Kind: "ticdc", Maintenance: true, TiDB: model.TiDBConfig{PrincipalARN: "arn"}, Target: model.TargetConfig{Type: "kafka", Host: "bootstrap", Port: 40000, Brokers: []model.Broker{{ID: "2", Host: "two", Port: 9092, ListenerPort: 40001}, {ID: "1", Host: "one", Port: 9092, ListenerPort: 19092}}}}
	steps, e := networkPlan(c)
	if e != nil {
		t.Fatal(e)
	}
	ports := map[string]string{}
	for _, s := range steps {
		if s.Kind == "target_group" && s.Properties["initialization"] != "deferred" {
			t.Fatal("managed TG initialized locally", s)
		}
		if s.Kind == "listener" && s.Properties["health_for"] != "" {
			if s.Properties["port"] == "40000" || s.Properties["port"] == "40001" || s.Properties["target_key"] != "aws.tg."+s.Key[len("aws.health_listener."):]+".standby" {
				t.Fatal(s)
			}
			ports[s.Key] = s.Properties["port"]
		}
	}
	if len(ports) != 3 {
		t.Fatal(ports)
	}
	c.Target.Brokers[0], c.Target.Brokers[1] = c.Target.Brokers[1], c.Target.Brokers[0]
	again, e := networkPlan(c)
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range again {
		if s.Properties["health_for"] != "" && ports[s.Key] != s.Properties["port"] {
			t.Fatal("unstable health port", s)
		}
	}
}
