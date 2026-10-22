package worker

import (
	"context"
	"net"
	"net/netip"
	"reflect"
	"sort"
	"time"
)

type Event struct {
	Action         string        `json:"action"`
	Region         string        `json:"region,omitempty"`
	Bindings       []Binding     `json:"bindings,omitempty"`
	Probe          *ProbeRequest `json:"probe,omitempty"`
	TimeoutSeconds int           `json:"timeout_seconds,omitempty"`
}
type Binding struct {
	Hostname          string    `json:"hostname"`
	Port              int32     `json:"port"`
	HealthListenerARN string    `json:"health_listener_arn"`
	ListenerARN       string    `json:"listener_arn"`
	TargetGroupARNs   [2]string `json:"target_group_arns"`
}
type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}
type Result struct {
	Status    string    `json:"status"`
	Vantage   string    `json:"vantage"`
	Checks    []Check   `json:"checks"`
	UpdatedAt time.Time `json:"updated_at"`
}
type Target struct {
	IP   string
	Port int32
}
type DNS interface {
	LookupHost(context.Context, string) ([]string, error)
}
type LoadBalancer interface {
	Active(context.Context, string) (string, error)
	Targets(context.Context, string) ([]Target, error)
	Replace(context.Context, string, []Target) error
	Healthy(context.Context, string, []Target) (bool, error)
	Switch(context.Context, string, string) error
}
type Reconciler struct {
	DNS          DNS
	LB           LoadBalancer
	PollInterval time.Duration
}

func privateTargets(values []string, port int32) ([]Target, bool) {
	set := map[string]bool{}
	for _, v := range values {
		a, e := netip.ParseAddr(v)
		if e != nil || !a.IsPrivate() || !a.Is4() {
			return nil, false
		}
		set[a.String()] = true
	}
	if len(set) == 0 {
		return nil, false
	}
	out := []Target{}
	for v := range set {
		out = append(out, Target{v, port})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IP < out[j].IP })
	return out, true
}
func equalTargets(a, b []Target) bool {
	a = append([]Target(nil), a...)
	b = append([]Target(nil), b...)
	sort.Slice(a, func(i, j int) bool {
		if a[i].IP == a[j].IP {
			return a[i].Port < a[j].Port
		}
		return a[i].IP < a[j].IP
	})
	sort.Slice(b, func(i, j int) bool {
		if b[i].IP == b[j].IP {
			return b[i].Port < b[j].Port
		}
		return b[i].IP < b[j].IP
	})
	return reflect.DeepEqual(a, b)
}
func (r Reconciler) Run(ctx context.Context, bindings []Binding) Result {
	out := Result{Status: "pass", Vantage: "aws_vpc_reconciler", UpdatedAt: time.Now().UTC()}
	if len(bindings) == 0 {
		out.Status = "fail"
		out.Checks = append(out.Checks, Check{"bindings", "fail", "No bindings configured"})
		return out
	}
	seen := map[string]bool{}
	for _, b := range bindings {
		for _, id := range []string{b.ListenerARN, b.HealthListenerARN, b.TargetGroupARNs[0], b.TargetGroupARNs[1]} {
			if id == "" || seen[id] {
				out.Status = "fail"
				out.Checks = append(out.Checks, Check{"bindings", "fail", "Bindings must have distinct listeners and target groups"})
				return out
			}
			seen[id] = true
		}
	}
	for _, b := range bindings {
		if ctx.Err() != nil {
			out.Status = "fail"
			out.Checks = append(out.Checks, Check{"deadline", "fail", "Invocation cancelled"})
			return out
		}
		detail := r.reconcile(ctx, b)
		status := "pass"
		if detail != "" {
			status = "fail"
			out.Status = "fail"
		} else {
			detail = "Forwarding matches stable private DNS"
		}
		out.Checks = append(out.Checks, Check{b.Hostname, status, detail})
	}
	return out
}
func (r Reconciler) reconcile(ctx context.Context, b Binding) string {
	if b.Hostname == "" || b.Port < 1 || b.Port > 65535 || b.ListenerARN == "" || b.TargetGroupARNs[0] == "" || b.TargetGroupARNs[1] == "" || b.TargetGroupARNs[0] == b.TargetGroupARNs[1] {
		return "Invalid binding"
	}
	values, err := r.DNS.LookupHost(ctx, b.Hostname)
	if err != nil {
		return "DNS resolution failed"
	}
	desired, ok := privateTargets(values, b.Port)
	if !ok {
		return "DNS must resolve to a nonempty private address set"
	}
	active, err := r.LB.Active(ctx, b.ListenerARN)
	if err != nil {
		return "Cannot read listener"
	}
	standby := b.TargetGroupARNs[1]
	if active == standby {
		standby = b.TargetGroupARNs[0]
	} else if active != b.TargetGroupARNs[0] {
		return "Listener forwarding group is not owned by this binding"
	}
	healthGroup, e := r.LB.Active(ctx, b.HealthListenerARN)
	if e != nil || (healthGroup != b.TargetGroupARNs[0] && healthGroup != b.TargetGroupARNs[1]) {
		return "Health listener forwarding group is not owned by this binding"
	}
	if healthGroup != standby {
		if e = r.LB.Switch(ctx, b.HealthListenerARN, standby); e != nil {
			return "Cannot attach standby health listener; active forwarding preserved"
		}
	}
	current, err := r.LB.Targets(ctx, active)
	if err != nil {
		return "Cannot read active targets"
	}
	if equalTargets(current, desired) {
		old, e := r.LB.Targets(ctx, standby)
		if e != nil {
			return "Cannot inspect inactive targets"
		}
		if len(old) > 0 {
			if e = r.LB.Replace(ctx, standby, nil); e != nil {
				return "Old target drain needs retry"
			}
		}
		return ""
	}
	if err = r.LB.Replace(ctx, standby, desired); err != nil {
		return "Cannot prepare standby targets; active forwarding preserved"
	}
	for {
		healthy, e := r.LB.Healthy(ctx, standby, desired)
		if e != nil {
			return "Cannot check standby health; active forwarding preserved"
		}
		if healthy {
			break
		}
		if r.PollInterval <= 0 {
			return "Standby is unhealthy; active forwarding preserved"
		}
		select {
		case <-ctx.Done():
			return "Standby health deadline exceeded; active forwarding preserved"
		case <-time.After(r.PollInterval):
		}
	}
	values, err = r.DNS.LookupHost(ctx, b.Hostname)
	if err != nil {
		return "DNS recheck failed; active forwarding preserved"
	}
	again, ok := privateTargets(values, b.Port)
	if !ok || !equalTargets(again, desired) {
		return "DNS changed during reconciliation; active forwarding preserved"
	}
	if err = r.LB.Switch(ctx, b.ListenerARN, standby); err != nil {
		return "Listener switch failed; inspect listener before retry"
	}
	if err = r.LB.Replace(ctx, active, nil); err != nil {
		return "Listener switched; old target drain needs retry"
	}
	return ""
}

var _ DNS = (*net.Resolver)(nil)
