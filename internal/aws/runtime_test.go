package aws

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ect "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	et "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"strings"
	"testing"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lt "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/worker"
)

type invokingLambda struct {
	LambdaAPI
	input *lambda.InvokeInput
	out   *lambda.InvokeOutput
	err   error
}

func (f *invokingLambda) Invoke(_ context.Context, i *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	f.input = i
	return f.out, f.err
}
func TestRuntimeInvocationRejectsFunctionErrors(t *testing.T) {
	f := &invokingLambda{out: &lambda.InvokeOutput{FunctionError: sdk.String("Unhandled"), Payload: []byte(`{"password":"must not print"}`)}}
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = f
	_, err := d.invoke(context.Background(), "arn:probe", worker.Event{Action: "probe"})
	if err == nil || err.Error() != "worker execution failed" {
		t.Fatalf("unexpected %v", err)
	}
}
func TestRuntimeInvocationUsesPayloadOnly(t *testing.T) {
	b, _ := json.Marshal(worker.Result{Status: "pass", Vantage: "aws_vpc_probe"})
	f := &invokingLambda{out: &lambda.InvokeOutput{StatusCode: 200, Payload: b}}
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = f
	_, err := d.invoke(context.Background(), "arn:probe", worker.Event{Action: "probe"})
	if err != nil {
		t.Fatal(err)
	}
	if f.input.InvocationType != "RequestResponse" || f.input.LogType != "None" {
		t.Fatal("unsafe invocation")
	}
}
func TestDeletedFunctionDoesNotBlockRetry(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	f := &fakeLambda{getErr: &lt.ResourceNotFoundException{}}
	d.Lambda = f
	d.EC2 = &lambdaENIs{}
	r, err := d.Refresh(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", Kind: "lambda_function", ID: "arn:probe", Properties: map[string]string{"name": "tilink-demo-probe", "security_group_id": "sg-1", "vpc_id": "vpc-1"}})
	if err != nil || r.Status != "deleted" {
		t.Fatalf("%+v %v", r, err)
	}
}

type concurrencyLambda struct {
	LambdaAPI
	value int32
}

func (f *concurrencyLambda) GetFunction(_ context.Context, _ *lambda.GetFunctionInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error) {
	return &lambda.GetFunctionOutput{Configuration: &lt.FunctionConfiguration{FunctionArn: sdk.String("arn:aws:lambda:us-east-1:123:function:tilink-demo-probe")}, Tags: map[string]string{"tilink:connection": "demo", "tilink:key": "aws.probe.function"}}, nil
}
func (f *concurrencyLambda) GetFunctionConcurrency(_ context.Context, _ *lambda.GetFunctionConcurrencyInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionConcurrencyOutput, error) {
	return &lambda.GetFunctionConcurrencyOutput{ReservedConcurrentExecutions: sdk.Int32(f.value)}, nil
}
func TestConcurrencyDriftIsVisible(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = &concurrencyLambda{value: 7}
	r, err := d.Refresh(context.Background(), model.Config{Name: "demo", AWS: model.AWSConfig{Region: "us-east-1", AccountID: "123"}}, model.Resource{Key: "aws.probe.concurrency", Kind: "lambda_concurrency", Properties: map[string]string{"name": "tilink-demo-probe", "mode": "probe"}})
	if err == nil && r.Properties["concurrency"] != "7" {
		t.Fatal("concurrency drift hidden")
	}
}

type deletingEvents struct {
	EventsAPI
	calls []string
}

func (f *deletingEvents) ListTagsForResource(context.Context, *eventbridge.ListTagsForResourceInput, ...func(*eventbridge.Options)) (*eventbridge.ListTagsForResourceOutput, error) {
	return &eventbridge.ListTagsForResourceOutput{Tags: []et.Tag{{Key: sdk.String("tilink:connection"), Value: sdk.String("demo")}, {Key: sdk.String("tilink:key"), Value: sdk.String("aws.reconcile.rule")}}}, nil
}
func (f *deletingEvents) ListTargetsByRule(context.Context, *eventbridge.ListTargetsByRuleInput, ...func(*eventbridge.Options)) (*eventbridge.ListTargetsByRuleOutput, error) {
	return &eventbridge.ListTargetsByRuleOutput{Targets: []et.Target{{Id: sdk.String("tilink"), Arn: sdk.String("arn:aws:lambda:us-east-1:123:function:tilink-demo-reconcile")}}}, nil
}
func (f *deletingEvents) DisableRule(context.Context, *eventbridge.DisableRuleInput, ...func(*eventbridge.Options)) (*eventbridge.DisableRuleOutput, error) {
	f.calls = append(f.calls, "disable")
	return &eventbridge.DisableRuleOutput{}, nil
}
func (f *deletingEvents) RemoveTargets(_ context.Context, in *eventbridge.RemoveTargetsInput, _ ...func(*eventbridge.Options)) (*eventbridge.RemoveTargetsOutput, error) {
	f.calls = append(f.calls, "remove")
	if len(in.Ids) != 1 || in.Ids[0] != "tilink" {
		panic("removing unrelated targets")
	}
	return &eventbridge.RemoveTargetsOutput{}, nil
}
func TestScheduleStopsBeforeTargetRemoval(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	f := &deletingEvents{}
	d.Events = f
	err := d.Delete(context.Background(), model.Config{Name: "demo", AWS: model.AWSConfig{Region: "us-east-1", AccountID: "123"}}, model.Resource{Key: "aws.reconcile.target", Kind: "event_target", Ownership: model.Created, Properties: map[string]string{"name": "tilink-demo-reconcile", "mode": "reconcile"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.calls, ",") != "disable,remove" {
		t.Fatal(f.calls)
	}
}

func TestPendingFunctionBecomesReady(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = &fakeLambda{response: &lambda.GetFunctionOutput{Configuration: &lt.FunctionConfiguration{FunctionArn: sdk.String("arn:probe"), State: lt.StateActive}, Tags: map[string]string{"tilink:connection": "demo", "tilink:key": "aws.probe.function"}}}
	r, err := d.Refresh(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", Kind: "lambda_function", ID: "arn:probe", Status: "pending", Properties: map[string]string{"name": "tilink-demo-probe"}})
	if err != nil || r.Status != "ready" {
		t.Fatalf("%+v %v", r, err)
	}
}

type deletingLambda struct {
	LambdaAPI
	deleted   bool
	probes    int
	polls     int
	neverGone bool
	calls     []string
}

func (f *deletingLambda) GetFunction(_ context.Context, _ *lambda.GetFunctionInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error) {
	if f.deleted {
		f.probes++
		if f.probes >= f.polls && !f.neverGone {
			return nil, &lt.ResourceNotFoundException{}
		}
	}
	return &lambda.GetFunctionOutput{Configuration: &lt.FunctionConfiguration{FunctionArn: sdk.String("arn:probe"), State: lt.StateActive}, Tags: map[string]string{"tilink:connection": "demo", "tilink:key": "aws.probe.function", "tilink:operation": "op", "tilink:connection-id": "connection"}}, nil
}
func (f *deletingLambda) PutFunctionConcurrency(_ context.Context, in *lambda.PutFunctionConcurrencyInput, _ ...func(*lambda.Options)) (*lambda.PutFunctionConcurrencyOutput, error) {
	f.calls = append(f.calls, "stop")
	if sdk.ToInt32(in.ReservedConcurrentExecutions) != 0 {
		panic("not stopped")
	}
	return &lambda.PutFunctionConcurrencyOutput{}, nil
}
func (f *deletingLambda) DeleteFunction(context.Context, *lambda.DeleteFunctionInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionOutput, error) {
	f.deleted = true
	f.calls = append(f.calls, "delete")
	return &lambda.DeleteFunctionOutput{}, nil
}
func TestLambdaDeletionWaitsForAbsence(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	f := &deletingLambda{polls: 3}
	d.Lambda = f
	d.EC2 = &lambdaENIs{}
	d.Sleep = func(context.Context, time.Duration) error { return nil }
	err := d.Delete(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", ID: "arn:probe", Kind: "lambda_function", Ownership: model.Created, OperationID: "op", Properties: map[string]string{"name": "tilink-demo-probe", "mode": "probe", "connection_id": "connection", "security_group_id": "sg-1", "vpc_id": "vpc-1"}})
	if err != nil || f.probes < 3 {
		t.Fatalf("acknowledgement reported as deletion: probes=%d err=%v", f.probes, err)
	}
}
func TestLambdaDeletionWithoutAbsenceIsIncomplete(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	f := &deletingLambda{neverGone: true}
	d.Lambda = f
	d.EC2 = &lambdaENIs{}
	d.Sleep = func(context.Context, time.Duration) error { return nil }
	err := d.Delete(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", ID: "arn:probe", Kind: "lambda_function", Ownership: model.Created, OperationID: "op", Properties: map[string]string{"name": "tilink-demo-probe", "mode": "probe", "connection_id": "connection", "security_group_id": "sg-1", "vpc_id": "vpc-1"}})
	if err == nil {
		t.Fatal("deletion acknowledged but still present must not succeed")
	}
}
func TestInvokeOwnedRejectsOwnershipBeforeInvoke(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = &fakeLambda{response: &lambda.GetFunctionOutput{Configuration: &lt.FunctionConfiguration{FunctionArn: sdk.String("arn:probe"), State: lt.StateActive}, Tags: map[string]string{"tilink:connection": "other"}}}
	_, err := d.InvokeOwned(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", ID: "arn:probe", Kind: "lambda_function", Ownership: model.Created, OperationID: "op", Properties: map[string]string{"name": "tilink-demo-probe", "mode": "probe", "connection_id": "connection"}}, worker.Event{Action: "probe"})
	if err == nil {
		t.Fatal("invoked foreign function")
	}
}

type lambdaENIs struct {
	inputs    []*ec2.DescribeNetworkInterfacesInput
	remaining int
	pages     bool
}

func (f *lambdaENIs) DescribeNetworkInterfaces(_ context.Context, in *ec2.DescribeNetworkInterfacesInput, _ ...func(*ec2.Options)) (*ec2.DescribeNetworkInterfacesOutput, error) {
	f.inputs = append(f.inputs, in)
	out := &ec2.DescribeNetworkInterfacesOutput{}
	if f.pages && in.NextToken == nil {
		out.NextToken = sdk.String("next")
		return out, nil
	}
	if f.remaining > 0 {
		f.remaining--
		out.NetworkInterfaces = []ect.NetworkInterface{{NetworkInterfaceId: sdk.String("eni-lambda"), InterfaceType: ect.NetworkInterfaceTypeLambda, VpcId: sdk.String("vpc-1"), Groups: []ect.GroupIdentifier{{GroupId: sdk.String("sg-1")}}}}
	}
	return out, nil
}
func TestAbsentFunctionRetainsRoleDependencyWhileLambdaENIsRemain(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = &fakeLambda{getErr: &lt.ResourceNotFoundException{}}
	f := &lambdaENIs{remaining: 1, pages: true}
	d.EC2 = f
	r, err := d.Refresh(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", Kind: "lambda_function", Properties: map[string]string{"name": "tilink-demo-probe", "vpc_id": "vpc-1", "security_group_id": "sg-1"}})
	if err != nil || r.Status != "deleting" {
		t.Fatalf("%+v %v", r, err)
	}
	if len(f.inputs) != 2 {
		t.Fatal("ENI pagination skipped")
	}
	filters := map[string]string{}
	for _, v := range f.inputs[0].Filters {
		filters[sdk.ToString(v.Name)] = strings.Join(v.Values, ",")
	}
	if filters["interface-type"] != "lambda" || filters["vpc-id"] != "vpc-1" || filters["group-id"] != "sg-1" {
		t.Fatal(filters)
	}
}
func TestRetryCleanupWaitsForLambdaENIRelease(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = &fakeLambda{getErr: &lt.ResourceNotFoundException{}}
	f := &lambdaENIs{remaining: 2}
	d.EC2 = f
	d.Sleep = func(context.Context, time.Duration) error { return nil }
	err := d.Delete(context.Background(), model.Config{Name: "demo"}, model.Resource{Key: "aws.probe.function", Kind: "lambda_function", Ownership: model.Created, Properties: map[string]string{"name": "tilink-demo-probe", "vpc_id": "vpc-1", "security_group_id": "sg-1"}})
	if err != nil || len(f.inputs) < 3 {
		t.Fatalf("release not verified calls=%d err=%v", len(f.inputs), err)
	}
}

func TestLambdaENITimeoutPreservesCleanupDependency(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	d.Lambda = &fakeLambda{getErr: &lt.ResourceNotFoundException{}}
	f := &lambdaENIs{remaining: 100}
	d.EC2 = f
	d.Sleep = func(context.Context, time.Duration) error { return nil }
	r := model.Resource{Key: "aws.probe.function", Kind: "lambda_function", Ownership: model.Created, Properties: map[string]string{"name": "tilink-demo-probe", "vpc_id": "vpc-1", "security_group_id": "sg-1"}}
	if err := d.Delete(context.Background(), model.Config{Name: "demo"}, r); err == nil || !strings.Contains(err.Error(), "execution role retained") {
		t.Fatalf("expected actionable incomplete cleanup: %v", err)
	}
	fresh, err := d.Refresh(context.Background(), model.Config{Name: "demo"}, r)
	if err != nil || fresh.Status != "deleting" {
		t.Fatalf("retry must retain role dependency %+v %v", fresh, err)
	}
}
