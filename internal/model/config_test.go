package model

import (
	"strings"
	"testing"
)

func TestDecodeRejectsUnknownAndSecrets(t *testing.T) {
	for _, input := range []string{"version: 1\npassword: secret\n", "version: 1\nname: ../escape\n", "version: 1\nunknown: true\n"} {
		if _, err := DecodeConfig(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}
func TestDecodeValidConfig(t *testing.T) {
	input := "version: 1\nname: orders\nkind: dm\nprovider: aws\naws:\n  region: us-east-1\n  account_id: '123456789012'\n  vpc_id: vpc-a\n  subnet_ids: [subnet-a]\ntidb:\n  project_id: p\n  cluster_id: c\n  principal_arn: arn:aws:iam::123456789012:root\ntarget:\n  type: rds\n  host: db.example.com\n  port: 3306\n"
	c, err := DecodeConfig(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "orders" {
		t.Fatal(c)
	}
}
func TestValidateNetworkAndBrokerFields(t *testing.T) {
	base := Config{Version: 1, Name: "a", Kind: "ticdc", Provider: "aws", AWS: AWSConfig{Region: "us-east-1", AccountID: "123456789012", VPCID: "vpc-a", SubnetIDs: []string{"s"}}, TiDB: TiDBConfig{ProjectID: "p", ClusterID: "c"}, Target: TargetConfig{Type: "kafka", Host: "bootstrap.local", Port: 9092, Brokers: []Broker{{ID: "1", Host: "b.local", Port: 9092, ListenerPort: 19092}}}}
	for _, mutate := range []func(*Config){func(c *Config) { c.AWS.AccountID = "wrong" }, func(c *Config) { c.AWS.ClientCIDRs = []string{"0.0.0.0/0"} }, func(c *Config) { c.Target.Brokers = append(c.Target.Brokers, c.Target.Brokers[0]) }, func(c *Config) { c.Target.Brokers[0].Port = 65536 }} {
		data := base
		data.Target.Brokers = append([]Broker{}, base.Target.Brokers...)
		mutate(&data)
		if data.Validate() == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
}
func TestRejectUnsafeCredentialReferencesAndURLs(t *testing.T) {
	base := Config{Version: 1, Name: "a", Kind: "app", Provider: "aws", AWS: AWSConfig{Region: "us-east-1", AccountID: "123456789012", VPCID: "vpc-a", SubnetIDs: []string{"s"}}, TiDB: TiDBConfig{ProjectID: "p", ClusterID: "c"}}
	for _, mutate := range []func(*Config){func(c *Config) { c.Verification.TargetUsernameEnv = "inline user" }, func(c *Config) { c.Verification.TargetPasswordEnv = "password=secret" }, func(c *Config) { c.TiDB.BaseURL = "https://user:secret@example.com" }, func(c *Config) { c.TiDB.DataflowBaseURL = "https://example.com?secret=secret" }, func(c *Config) { c.TiDB.PrivateKeyEnv = "not an env" }} {
		c := base
		mutate(&c)
		if c.Validate() == nil {
			t.Fatal("accepted unsafe reference")
		}
	}
}
func TestKafkaPublicationAcknowledgementIsOneWay(t *testing.T) {
	old := Config{}
	next := old
	next.Target.AdvertisedListenersPublished = true
	if !CompatibleUpdate(old, next) {
		t.Fatal("publication cannot continue")
	}
	if CompatibleUpdate(next, old) {
		t.Fatal("publication acknowledgement reversed")
	}
}
