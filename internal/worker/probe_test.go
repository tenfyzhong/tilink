package worker

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

type fakeProtocols struct {
	calls   []string
	err     error
	brokers []string
}

func (f *fakeProtocols) MySQL(context.Context, ProbeRequest) error {
	f.calls = append(f.calls, "mysql")
	return f.err
}
func (f *fakeProtocols) Kafka(_ context.Context, r ProbeRequest) ([]string, error) {
	if r.Port == "1" {
		return nil, errors.New("unreachable")
	}
	f.calls = append(f.calls, "kafka")
	return f.brokers, f.err
}
func TestProbeTCPIsPartialAndNeverClaimsRemotePath(t *testing.T) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	host, port, _ := net.SplitHostPort(l.Addr().String())
	r := Probe{DialContext: localFixtureDial, DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}}
	out := r.Run(context.Background(), ProbeRequest{Protocol: "tcp", Host: host, Port: port})
	if out.Status != "partial" || out.Vantage != "aws_vpc_probe" {
		t.Fatal(out)
	}
}
func TestProbeSanitizesProtocolErrors(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	host, port, _ := net.SplitHostPort(l.Addr().String())
	p := &fakeProtocols{err: errors.New("password=secret")}
	out := (Probe{DialContext: localFixtureDial, DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, Protocols: p}).Run(context.Background(), ProbeRequest{Protocol: "mysql", Host: host, Port: port, Username: "user", Password: "secret"})
	if out.Status != "fail" || strings.Contains(out.Checks[len(out.Checks)-1].Detail, "secret") {
		t.Fatal(out)
	}
}
func TestProbeKafkaChecksEveryMetadataBroker(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	host, port, _ := net.SplitHostPort(l.Addr().String())
	p := &fakeProtocols{brokers: []string{l.Addr().String(), "127.0.0.1:1"}}
	out := (Probe{DialContext: localFixtureDial, DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, Protocols: p}).Run(context.Background(), ProbeRequest{Protocol: "kafka", Host: host, Port: port})
	if out.Status != "fail" || len(out.Checks) < 4 {
		t.Fatal(out)
	}
}
func TestProbeRejectsUnknownProtocolBeforeNetwork(t *testing.T) {
	out := (Probe{}).Run(context.Background(), ProbeRequest{Protocol: "other", Host: "foo", Port: "3306"})
	if out.Status != "fail" {
		t.Fatal(out)
	}
}

func TestProbeRejectsPublicDNS(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	host, port, _ := net.SplitHostPort(l.Addr().String())
	out := (Probe{DialContext: localFixtureDial, DNS: &fakeDNS{values: [][]string{{"8.8.8.8"}}}}).Run(context.Background(), ProbeRequest{Protocol: "tcp", Host: host, Port: port})
	if out.Status != "fail" || len(out.Checks) != 1 {
		t.Fatal(out)
	}
}
