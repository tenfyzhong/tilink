package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
)

func TestPrivateDialerPinsCheckedIPAddress(t *testing.T) {
	dns := &fakeDNS{values: [][]string{{"10.0.0.2"}, {"8.8.8.8"}}}
	var addresses []string
	d := privateDialer{DNS: dns, Dial: func(_ context.Context, network, address string) (net.Conn, error) {
		addresses = append(addresses, address)
		a, b := net.Pipe()
		b.Close()
		return a, nil
	}}
	c, e := d.DialContext(context.Background(), "tcp", "db:3306")
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	if len(addresses) != 1 || addresses[0] != "10.0.0.2:3306" {
		t.Fatal(addresses)
	}
	if _, e = d.DialContext(context.Background(), "tcp", "db:3306"); e == nil || len(addresses) != 1 {
		t.Fatal("public rebound socket dialed", addresses, e)
	}
}
func TestPrivateDialerRejectsMixedAddressesBeforeDial(t *testing.T) {
	d := privateDialer{DNS: &fakeDNS{values: [][]string{{"10.0.0.2", "8.8.8.8"}}}, Dial: func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("socket opened")
		return nil, errors.New("unexpected")
	}}
	if _, e := d.DialContext(context.Background(), "tcp", "db:3306"); e == nil {
		t.Fatal("public peer accepted")
	}
}
func TestConfiguredEventRejectsTopologyAndActionOverrides(t *testing.T) {
	base := Event{Action: "reconcile", Region: "us-west-2", Bindings: []Binding{{Hostname: "db", Port: 3306, ListenerARN: "l", HealthListenerARN: "h", TargetGroupARNs: [2]string{"a", "b"}}}}
	raw, _ := json.Marshal(base)
	for _, incoming := range []Event{{Action: "probe", Probe: &ProbeRequest{Host: "db"}}, {Action: "reconcile", Region: "other"}, {Action: "reconcile", Bindings: []Binding{{Hostname: "other"}}}} {
		if _, e := configuredEvent(string(raw), incoming); e == nil {
			t.Fatal("override accepted", incoming)
		}
	}
	merged, e := configuredEvent(string(raw), Event{})
	if e != nil || len(merged.Bindings) != 1 || merged.Bindings[0].ListenerARN != "l" {
		t.Fatal(merged, e)
	}
}
func TestConfiguredProbeAcceptsCredentialsWithoutPersisting(t *testing.T) {
	out, e := configuredEvent(`{"action":"probe","region":"us-west-2"}`, Event{Action: "probe", Probe: &ProbeRequest{Protocol: "mysql", Host: "db", Port: "3306", Username: "user", Password: "secret"}})
	if e != nil || out.Probe == nil || out.Probe.Password != "secret" || out.Region != "us-west-2" {
		t.Fatal(out, e)
	}
}
func TestReconcileAttachesStandbyHealthListenerBeforeTargets(t *testing.T) {
	f := &fakeLB{active: "a", health: "a", groups: map[string][]Target{"a": {{"10.0.0.1", 3306}}}, healthy: true}
	r := Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f}
	out := r.Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, ListenerARN: "l", HealthListenerARN: "h", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "pass" || len(f.calls) < 2 || f.calls[0] != "health:b" || f.calls[1] != "replace:b" {
		t.Fatal(out, f.calls)
	}
}

type peerDNS map[string][]string

func (d peerDNS) LookupHost(_ context.Context, host string) ([]string, error) {
	if ips, ok := d[host]; ok {
		return ips, nil
	}
	return nil, errors.New("unknown peer")
}
func TestProbeRejectsPublicMetadataBeforeSendingCredentials(t *testing.T) {
	p := &fakeProtocols{brokers: []string{"public-broker:9092"}}
	out := (Probe{DNS: peerDNS{"bootstrap": {"10.0.0.2"}, "public-broker": {"8.8.8.8"}}, DialContext: func(context.Context, string, string) (net.Conn, error) { a, b := net.Pipe(); b.Close(); return a, nil }, Protocols: p}).Run(context.Background(), ProbeRequest{Protocol: "kafka", Host: "bootstrap", Port: "9092", Username: "user", Password: "secret", SASLMechanism: "SCRAM-SHA-512"})
	if out.Status != "fail" || len(p.calls) != 1 {
		t.Fatal("credentials sent to metadata peer", out, p.calls)
	}
}
func TestPrivateProtocolsRejectPublicLiteral(t *testing.T) {
	p := NetworkProtocols{DialContext: func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("public socket opened")
		return nil, errors.New("unexpected")
	}}
	if e := p.MySQL(context.Background(), ProbeRequest{Host: "8.8.8.8", Port: "3306", Username: "user", Password: "secret"}); e == nil {
		t.Fatal("public MySQL accepted")
	}
	if _, e := p.Kafka(context.Background(), ProbeRequest{Host: "8.8.8.8", Port: "9092"}); e == nil {
		t.Fatal("public Kafka accepted")
	}
}

func TestReconcileInitialEmptyGroupBootstrapsViaHealthListener(t *testing.T) {
	f := &fakeLB{active: "a", health: "b", groups: map[string][]Target{}, healthy: true}
	out := (Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f}).Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, ListenerARN: "l", HealthListenerARN: "h", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "pass" || f.active != "b" || len(f.groups["b"]) != 1 {
		t.Fatal(out, f.calls)
	}
}
func TestReconcileRejectsUnownedHealthForwarding(t *testing.T) {
	f := &fakeLB{active: "a", health: "external", groups: map[string][]Target{}, healthy: true}
	out := (Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f}).Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, ListenerARN: "l", HealthListenerARN: "h", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "fail" || len(f.calls) != 0 {
		t.Fatal(out, f.calls)
	}
}
