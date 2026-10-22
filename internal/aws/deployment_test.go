package aws

import (
	"context"
	"strings"
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	it "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lt "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/tenfyzhong/tilink/internal/model"
)

func TestDeploymentPlanInventoriesEveryAuxiliary(t *testing.T) {
	c := model.Config{Name: "demo", Kind: "dm", Maintenance: true, Probe: true, AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
	d := NewDeployment(sdk.Config{}, nil)
	steps, err := d.Plan(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, s := range steps {
		seen[s.Kind] = true
		if s.Key == "" {
			t.Fatal("missing key")
		}
	}
	for _, k := range []string{"iam_role", "iam_policy", "log_group", "lambda_function", "lambda_concurrency", "event_rule", "lambda_permission", "event_target"} {
		if !seen[k] {
			t.Errorf("missing %s", k)
		}
	}
}
func TestPolicyScopesELBMutations(t *testing.T) {
	p := runtimePolicy(model.Config{AWS: model.AWSConfig{Region: "us-east-1", AccountID: "123", SubnetIDs: []string{"subnet-1"}}}, []model.Resource{{Kind: "target_group", Ownership: model.Created, ID: "arn:tg"}, {Kind: "listener", Ownership: model.Created, ID: "arn:listener"}}, "reconcile", "/aws/lambda/tilink-demo-reconcile")
	if !strings.Contains(p, "arn:tg") || !strings.Contains(p, "arn:listener") {
		t.Fatal(p)
	}
	if strings.Contains(p, `"elasticloadbalancing:*"`) {
		t.Fatal("unbounded ELB access")
	}
}

type fakeLambda struct {
	LambdaAPI
	created  *lambda.CreateFunctionInput
	response *lambda.GetFunctionOutput
	getErr   error
}

func (f *fakeLambda) GetFunction(context.Context, *lambda.GetFunctionInput, ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error) {
	return f.response, f.getErr
}
func (f *fakeLambda) CreateFunction(_ context.Context, in *lambda.CreateFunctionInput, _ ...func(*lambda.Options)) (*lambda.CreateFunctionOutput, error) {
	f.created = in
	return &lambda.CreateFunctionOutput{FunctionArn: sdk.String("arn:function")}, nil
}
func TestFunctionRecoveryRequiresOwnership(t *testing.T) {
	f := &fakeLambda{response: &lambda.GetFunctionOutput{Configuration: &lt.FunctionConfiguration{FunctionArn: sdk.String("arn:function")}, Tags: map[string]string{"tilink:connection": "other"}}}
	d := NewDeployment(sdk.Config{}, []byte("zip"))
	d.Lambda = f
	_, err := d.Apply(context.Background(), model.Config{Name: "demo"}, model.Step{Key: "aws.probe.function", Kind: "lambda_function", OperationID: "op", Properties: map[string]string{"mode": "probe"}}, nil)
	if err == nil || f.created != nil {
		t.Fatal("adopted an external function")
	}
}

func TestReconcilePolicyDependsOnEveryNetworkBinding(t *testing.T) {
	c := model.Config{Name: "demo", Kind: "ticdc", Maintenance: true, Target: model.TargetConfig{Type: "kafka", Brokers: []model.Broker{{ID: "one"}}}}
	steps, _ := NewDeployment(sdk.Config{}, nil).Plan(context.Background(), c)
	for _, s := range steps {
		if s.Kind != "iam_policy" {
			continue
		}
		seen := map[string]bool{}
		for _, k := range s.DependsOn {
			seen[k] = true
		}
		for _, key := range []string{"aws.listener.primary", "aws.tg.primary", "aws.tg.primary.standby", "aws.listener.broker-one", "aws.tg.broker-one", "aws.tg.broker-one.standby", "aws.health_listener.primary", "aws.health_listener.broker-one"} {
			if !seen[key] {
				t.Errorf("missing dependency %s", key)
			}
		}
	}
}

type policyIAM struct {
	IAMAPI
	written bool
}

func (f *policyIAM) GetRole(_ context.Context, _ *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	return &iam.GetRoleOutput{Role: &it.Role{Arn: sdk.String("arn:aws:iam::123:role/tilink-demo-probe"), Tags: []it.Tag{{Key: sdk.String("tilink:connection"), Value: sdk.String("demo")}, {Key: sdk.String("tilink:key"), Value: sdk.String("aws.probe.role")}}}}, nil
}
func (f *policyIAM) GetRolePolicy(_ context.Context, _ *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	return &iam.GetRolePolicyOutput{PolicyDocument: sdk.String(`{"Statement":[{"Action":"external"}]}`)}, nil
}
func (f *policyIAM) PutRolePolicy(_ context.Context, _ *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	f.written = true
	return &iam.PutRolePolicyOutput{}, nil
}
func TestExistingPolicyIsNotOverwritten(t *testing.T) {
	d := NewDeployment(sdk.Config{}, nil)
	f := &policyIAM{}
	d.IAM = f
	_, err := d.Apply(context.Background(), model.Config{Name: "demo"}, model.Step{Kind: "iam_policy", Key: "aws.probe.policy", Properties: map[string]string{"mode": "probe"}}, []model.Resource{{Key: "aws.probe.role", ID: "arn:aws:iam::123:role/tilink-demo-probe", Properties: map[string]string{"name": "tilink-demo-probe"}}})
	if err == nil || f.written {
		t.Fatal("external policy overwritten")
	}
}

func TestDeploymentNamesFitAWSLimitsAndRemainDistinct(t *testing.T) {
	a := deploymentName(model.Config{Name: strings.Repeat("a", 63)}, "reconcile")
	b := deploymentName(model.Config{Name: strings.Repeat("a", 62) + "b"}, "reconcile")
	if len(a) > 64 || a == b {
		t.Fatalf("invalid names %q %q", a, b)
	}
}

func TestHealthListenersAreAttachedToMainBindings(t *testing.T) {
	resources := []model.Resource{{Key: "aws.tg.primary", Kind: "target_group", ID: "tg-active", Properties: map[string]string{"host": "db.internal", "port": "3306"}}, {Key: "aws.tg.primary.standby", Kind: "target_group", ID: "tg-inactive"}, {Key: "aws.listener.primary", Kind: "listener", ID: "main-listener", Properties: map[string]string{"target_key": "aws.tg.primary"}}, {Key: "aws.health_listener.primary", Kind: "listener", ID: "health-listener", Properties: map[string]string{"target_key": "aws.tg.primary.standby", "health_for": "aws.listener.primary"}}}
	e, err := workerEvent(model.Config{}, resources, "reconcile")
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Bindings) != 1 || e.Bindings[0].HealthListenerARN != "health-listener" {
		t.Fatalf("%+v", e)
	}
}
