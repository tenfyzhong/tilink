package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type fakeDNS struct {
	values [][]string
	err    error
	n      int
}

func (f *fakeDNS) LookupHost(context.Context, string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	i := f.n
	f.n++
	if i >= len(f.values) {
		i = len(f.values) - 1
	}
	return f.values[i], nil
}

type fakeLB struct {
	active  string
	health  string
	groups  map[string][]Target
	healthy bool
	calls   []string
	fail    string
}

func (f *fakeLB) Active(_ context.Context, listener string) (string, error) {
	if listener == "h" {
		if f.health == "" {
			return "b", nil
		}
		return f.health, nil
	}
	return f.active, nil
}
func (f *fakeLB) Targets(_ context.Context, g string) ([]Target, error) { return f.groups[g], nil }
func (f *fakeLB) Replace(_ context.Context, g string, v []Target) error {
	f.calls = append(f.calls, "replace:"+g)
	if f.fail == "replace" {
		return errors.New("secret")
	}
	f.groups[g] = v
	return nil
}
func (f *fakeLB) Healthy(context.Context, string, []Target) (bool, error) { return f.healthy, nil }
func (f *fakeLB) Switch(_ context.Context, l, g string) error {
	if l == "h" {
		f.calls = append(f.calls, "health:"+g)
		f.health = g
		return nil
	}
	f.calls = append(f.calls, "switch:"+g)
	if f.fail == "switch" {
		return errors.New("secret")
	}
	f.active = g
	return nil
}
func TestReconcileSwitchesOnlyAfterHealthyStablePrivateDNS(t *testing.T) {
	f := &fakeLB{active: "a", groups: map[string][]Target{"a": {{IP: "10.0.0.1", Port: 3306}}}, healthy: true}
	r := Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}, {"10.0.0.2"}}}, LB: f}
	out := r.Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, HealthListenerARN: "h", ListenerARN: "listener", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "pass" || !reflect.DeepEqual(f.calls, []string{"replace:b", "switch:b", "replace:a"}) {
		t.Fatalf("%+v %v", out, f.calls)
	}
}
func TestReconcilePreservesActiveOnUnsafeReplacement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  [][]string
		healthy bool
	}{{"empty", [][]string{{}}, true}, {"public", [][]string{{"8.8.8.8"}}, true}, {"dns changed", [][]string{{"10.0.0.2"}, {"10.0.0.3"}}, true}, {"unhealthy", [][]string{{"10.0.0.2"}, {"10.0.0.2"}}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeLB{active: "a", groups: map[string][]Target{"a": {{IP: "10.0.0.1", Port: 3306}}}, healthy: tc.healthy}
			r := Reconciler{DNS: &fakeDNS{values: tc.values}, LB: f}
			out := r.Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, HealthListenerARN: "h", ListenerARN: "listener", TargetGroupARNs: [2]string{"a", "b"}}})
			if out.Status != "fail" || f.active != "a" || len(f.groups["a"]) != 1 {
				t.Fatalf("%+v %+v", out, f)
			}
		})
	}
}
func TestReconcileDoesNotMutateAlreadyCurrent(t *testing.T) {
	f := &fakeLB{active: "a", groups: map[string][]Target{"a": {{IP: "10.0.0.2", Port: 3306}}}}
	r := Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f}
	out := r.Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, HealthListenerARN: "h", ListenerARN: "l", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "pass" || len(f.calls) != 0 {
		t.Fatal(out, f.calls)
	}
}

func TestReconcileRetriesOldTargetDrainAfterSuccessfulSwitch(t *testing.T) {
	f := &fakeLB{active: "b", groups: map[string][]Target{"a": {{IP: "10.0.0.1", Port: 3306}}, "b": {{IP: "10.0.0.2", Port: 3306}}}}
	r := Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f}
	out := r.Run(context.Background(), []Binding{{Hostname: "db", Port: 3306, HealthListenerARN: "h", ListenerARN: "l", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "pass" || len(f.groups["a"]) != 0 {
		t.Fatal("old writer targets were not drained on retry", out)
	}
}

func TestReconcileRejectsSharedTargetGroupsBeforeMutation(t *testing.T) {
	f := &fakeLB{active: "a", groups: map[string][]Target{"a": {{"10.0.0.1", 3306}}}, healthy: true}
	r := Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f}
	out := r.Run(context.Background(), []Binding{{Hostname: "one", Port: 3306, HealthListenerARN: "h", ListenerARN: "l1", TargetGroupARNs: [2]string{"a", "b"}}, {Hostname: "two", Port: 3306, HealthListenerARN: "h", ListenerARN: "l2", TargetGroupARNs: [2]string{"b", "c"}}})
	if out.Status != "fail" || len(f.calls) != 0 {
		t.Fatal(out, f.calls)
	}
}

func TestReconcileRejectsIPv6TargetsForIPv4NLB(t *testing.T) {
	if _, ok := privateTargets([]string{"fd00::1"}, 3306); ok {
		t.Fatal("IPv6 registered into IPv4 target group")
	}
}
