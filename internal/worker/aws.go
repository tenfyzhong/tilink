package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
)

type ELBAPI interface {
	DescribeListeners(context.Context, *elb.DescribeListenersInput, ...func(*elb.Options)) (*elb.DescribeListenersOutput, error)
	DescribeTargetHealth(context.Context, *elb.DescribeTargetHealthInput, ...func(*elb.Options)) (*elb.DescribeTargetHealthOutput, error)
	RegisterTargets(context.Context, *elb.RegisterTargetsInput, ...func(*elb.Options)) (*elb.RegisterTargetsOutput, error)
	DeregisterTargets(context.Context, *elb.DeregisterTargetsInput, ...func(*elb.Options)) (*elb.DeregisterTargetsOutput, error)
	ModifyListener(context.Context, *elb.ModifyListenerInput, ...func(*elb.Options)) (*elb.ModifyListenerOutput, error)
}
type ELBAdapter struct{ Client ELBAPI }

func (a ELBAdapter) Active(ctx context.Context, listener string) (string, error) {
	out, e := a.Client.DescribeListeners(ctx, &elb.DescribeListenersInput{ListenerArns: []string{listener}})
	if e != nil {
		return "", e
	}
	if len(out.Listeners) != 1 || len(out.Listeners[0].DefaultActions) != 1 {
		return "", errors.New("ambiguous listener")
	}
	action := out.Listeners[0].DefaultActions[0]
	if action.Type != types.ActionTypeEnumForward {
		return "", errors.New("not a forwarding listener")
	}
	if action.ForwardConfig != nil {
		if len(action.ForwardConfig.TargetGroups) != 1 {
			return "", errors.New("mixed target groups")
		}
		g := action.ForwardConfig.TargetGroups[0]
		if g.Weight != nil && *g.Weight == 0 {
			return "", errors.New("zero forwarding weight")
		}
		return aws.ToString(g.TargetGroupArn), nil
	}
	if action.TargetGroupArn == nil {
		return "", errors.New("missing target group")
	}
	return *action.TargetGroupArn, nil
}
func (a ELBAdapter) Targets(ctx context.Context, group string) ([]Target, error) {
	out, e := a.Client.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: aws.String(group)})
	if e != nil {
		return nil, e
	}
	v := []Target{}
	for _, t := range out.TargetHealthDescriptions {
		if t.Target != nil {
			v = append(v, Target{aws.ToString(t.Target.Id), aws.ToInt32(t.Target.Port)})
		}
	}
	return v, nil
}
func descriptions(v []Target) []types.TargetDescription {
	out := make([]types.TargetDescription, 0, len(v))
	for _, t := range v {
		out = append(out, types.TargetDescription{Id: aws.String(t.IP), Port: aws.Int32(t.Port)})
	}
	return out
}
func (a ELBAdapter) Replace(ctx context.Context, group string, desired []Target) error {
	existing, e := a.Targets(ctx, group)
	if e != nil {
		return e
	}
	remove := []Target{}
	add := []Target{}
	for _, old := range existing {
		found := false
		for _, n := range desired {
			if old == n {
				found = true
			}
		}
		if !found {
			remove = append(remove, old)
		}
	}
	for _, n := range desired {
		found := false
		for _, old := range existing {
			if old == n {
				found = true
			}
		}
		if !found {
			add = append(add, n)
		}
	}
	if len(remove) > 0 {
		if _, e = a.Client.DeregisterTargets(ctx, &elb.DeregisterTargetsInput{TargetGroupArn: aws.String(group), Targets: descriptions(remove)}); e != nil {
			return e
		}
	}
	if len(add) > 0 {
		_, e = a.Client.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: aws.String(group), Targets: descriptions(add)})
		return e
	}
	return nil
}
func (a ELBAdapter) Healthy(ctx context.Context, group string, desired []Target) (bool, error) {
	out, e := a.Client.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: aws.String(group), Targets: descriptions(desired)})
	if e != nil {
		return false, e
	}
	healthy := map[Target]bool{}
	for _, v := range out.TargetHealthDescriptions {
		if v.Target != nil && v.TargetHealth != nil && v.TargetHealth.State == types.TargetHealthStateEnumHealthy {
			healthy[Target{aws.ToString(v.Target.Id), aws.ToInt32(v.Target.Port)}] = true
		}
	}
	for _, v := range desired {
		if !healthy[v] {
			return false, nil
		}
	}
	return len(desired) > 0, nil
}
func (a ELBAdapter) Switch(ctx context.Context, listener, group string) error {
	_, e := a.Client.ModifyListener(ctx, &elb.ModifyListenerInput{ListenerArn: aws.String(listener), DefaultActions: []types.Action{{Type: types.ActionTypeEnumForward, TargetGroupArn: aws.String(group)}}})
	return e
}

func Handle(ctx context.Context, event Event) (Result, error) {
	fail := func(detail string) (Result, error) {
		return Result{Status: "fail", Vantage: "aws_vpc_worker", UpdatedAt: time.Now().UTC(), Checks: []Check{{"event", "fail", detail}}}, nil
	}
	var e error
	event, e = configuredEvent(os.Getenv("TILINK_EVENT"), event)
	if e != nil {
		return fail(e.Error())
	}
	seconds := event.TimeoutSeconds
	if seconds <= 0 || seconds > 240 {
		seconds = 45
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer cancel()
	switch event.Action {
	case "probe":
		if event.Probe == nil {
			return fail("Probe request missing")
		}
		return (Probe{DNS: net.DefaultResolver}).Run(ctx, *event.Probe), nil
	case "reconcile":
		cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(event.Region))
		if e != nil {
			return fail("AWS runtime configuration unavailable")
		}
		return (Reconciler{DNS: net.DefaultResolver, LB: ELBAdapter{Client: elb.NewFromConfig(cfg)}, PollInterval: 2 * time.Second}).Run(ctx, event.Bindings), nil
	default:
		return fail("Unknown worker action")
	}
}

func configuredEvent(raw string, input Event) (Event, error) {
	if raw == "" {
		return input, nil
	}
	var configured Event
	if json.Unmarshal([]byte(raw), &configured) != nil || configured.Action == "" {
		return Event{}, errors.New("Invalid default worker configuration")
	}
	if configured.Probe != nil && (configured.Probe.Username != "" || configured.Probe.Password != "") {
		return Event{}, errors.New("Credentials must be supplied in invocation input")
	}
	if (input.Action != "" && input.Action != configured.Action) || (input.Region != "" && input.Region != configured.Region) || (input.TimeoutSeconds != 0 && input.TimeoutSeconds != configured.TimeoutSeconds) || (len(input.Bindings) > 0 && !reflect.DeepEqual(input.Bindings, configured.Bindings)) {
		return Event{}, errors.New("Invocation cannot override configured worker topology")
	}
	if configured.Action == "reconcile" && input.Probe != nil {
		return Event{}, errors.New("Invocation cannot override configured worker purpose")
	}
	if configured.Action == "probe" && input.Probe != nil {
		configured.Probe = input.Probe
	}
	return configured, nil
}
