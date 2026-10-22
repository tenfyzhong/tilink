package worker

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"testing"
)

type fakeELB struct {
	calls       []string
	targetState types.TargetHealthStateEnum
	listeners   []types.Listener
	targets     []types.TargetHealthDescription
}

func (f *fakeELB) DescribeListeners(context.Context, *elb.DescribeListenersInput, ...func(*elb.Options)) (*elb.DescribeListenersOutput, error) {
	return &elb.DescribeListenersOutput{Listeners: f.listeners}, nil
}
func (f *fakeELB) DescribeTargetHealth(context.Context, *elb.DescribeTargetHealthInput, ...func(*elb.Options)) (*elb.DescribeTargetHealthOutput, error) {
	return &elb.DescribeTargetHealthOutput{TargetHealthDescriptions: f.targets}, nil
}
func (f *fakeELB) RegisterTargets(context.Context, *elb.RegisterTargetsInput, ...func(*elb.Options)) (*elb.RegisterTargetsOutput, error) {
	f.calls = append(f.calls, "register")
	return &elb.RegisterTargetsOutput{}, nil
}
func (f *fakeELB) DeregisterTargets(context.Context, *elb.DeregisterTargetsInput, ...func(*elb.Options)) (*elb.DeregisterTargetsOutput, error) {
	f.calls = append(f.calls, "deregister")
	return &elb.DeregisterTargetsOutput{}, nil
}
func (f *fakeELB) ModifyListener(context.Context, *elb.ModifyListenerInput, ...func(*elb.Options)) (*elb.ModifyListenerOutput, error) {
	f.calls = append(f.calls, "switch")
	return &elb.ModifyListenerOutput{}, nil
}
func TestELBRejectsWeightedOrUnknownForwarding(t *testing.T) {
	f := &fakeELB{listeners: []types.Listener{{DefaultActions: []types.Action{{Type: types.ActionTypeEnumForward, ForwardConfig: &types.ForwardActionConfig{TargetGroups: []types.TargetGroupTuple{{TargetGroupArn: aws.String("a"), Weight: aws.Int32(1)}, {TargetGroupArn: aws.String("b"), Weight: aws.Int32(1)}}}}}}}}
	_, e := (ELBAdapter{Client: f}).Active(context.Background(), "listener")
	if e == nil {
		t.Fatal("accepted mixed forwarding")
	}
}
func TestELBHealthyRequiresEveryRequestedTarget(t *testing.T) {
	f := &fakeELB{targets: []types.TargetHealthDescription{{Target: &types.TargetDescription{Id: aws.String("10.0.0.1"), Port: aws.Int32(3306)}, TargetHealth: &types.TargetHealth{State: types.TargetHealthStateEnumHealthy}}}}
	ok, e := (ELBAdapter{Client: f}).Healthy(context.Background(), "a", []Target{{"10.0.0.1", 3306}, {"10.0.0.2", 3306}})
	if e != nil || ok {
		t.Fatal("missing target accepted", e)
	}
}
func TestELBReplaceRemovesObsoleteBeforeRegistering(t *testing.T) {
	f := &fakeELB{targets: []types.TargetHealthDescription{{Target: &types.TargetDescription{Id: aws.String("10.0.0.1"), Port: aws.Int32(3306)}}}}
	e := (ELBAdapter{Client: f}).Replace(context.Background(), "b", []Target{{"10.0.0.2", 3306}})
	if e != nil || len(f.calls) != 2 || f.calls[0] != "deregister" || f.calls[1] != "register" {
		t.Fatal(f.calls, e)
	}
}
func TestHandleRejectsUnknownAction(t *testing.T) {
	out, e := Handle(context.Background(), Event{Action: "delete"})
	if e != nil || out.Status != "fail" {
		t.Fatal(out, e)
	}
}

func TestHandleRejectsSecretsInPersistentDefault(t *testing.T) {
	t.Setenv("TILINK_EVENT", `{"action":"probe","probe":{"protocol":"mysql","host":"db","port":"3306","username":"user","password":"secret"}}`)
	out, e := Handle(context.Background(), Event{})
	if e != nil || out.Status != "fail" || out.Checks[0].Detail != "Credentials must be supplied in invocation input" {
		t.Fatal(out, e)
	}
}
