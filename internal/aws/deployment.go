package aws

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	et "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	it "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lt "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/worker"
)

type IAMAPI interface {
	GetRole(context.Context, *iam.GetRoleInput, ...func(*iam.Options)) (*iam.GetRoleOutput, error)
	CreateRole(context.Context, *iam.CreateRoleInput, ...func(*iam.Options)) (*iam.CreateRoleOutput, error)
	DeleteRole(context.Context, *iam.DeleteRoleInput, ...func(*iam.Options)) (*iam.DeleteRoleOutput, error)
	PutRolePolicy(context.Context, *iam.PutRolePolicyInput, ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error)
	GetRolePolicy(context.Context, *iam.GetRolePolicyInput, ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error)
	DeleteRolePolicy(context.Context, *iam.DeleteRolePolicyInput, ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error)
}
type LambdaAPI interface {
	GetFunctionConcurrency(context.Context, *lambda.GetFunctionConcurrencyInput, ...func(*lambda.Options)) (*lambda.GetFunctionConcurrencyOutput, error)
	GetFunction(context.Context, *lambda.GetFunctionInput, ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error)
	CreateFunction(context.Context, *lambda.CreateFunctionInput, ...func(*lambda.Options)) (*lambda.CreateFunctionOutput, error)
	DeleteFunction(context.Context, *lambda.DeleteFunctionInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionOutput, error)
	PutFunctionConcurrency(context.Context, *lambda.PutFunctionConcurrencyInput, ...func(*lambda.Options)) (*lambda.PutFunctionConcurrencyOutput, error)
	DeleteFunctionConcurrency(context.Context, *lambda.DeleteFunctionConcurrencyInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionConcurrencyOutput, error)
	AddPermission(context.Context, *lambda.AddPermissionInput, ...func(*lambda.Options)) (*lambda.AddPermissionOutput, error)
	RemovePermission(context.Context, *lambda.RemovePermissionInput, ...func(*lambda.Options)) (*lambda.RemovePermissionOutput, error)
	GetPolicy(context.Context, *lambda.GetPolicyInput, ...func(*lambda.Options)) (*lambda.GetPolicyOutput, error)
	Invoke(context.Context, *lambda.InvokeInput, ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}
type LogsAPI interface {
	CreateLogGroup(context.Context, *cloudwatchlogs.CreateLogGroupInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	PutRetentionPolicy(context.Context, *cloudwatchlogs.PutRetentionPolicyInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutRetentionPolicyOutput, error)
	DeleteLogGroup(context.Context, *cloudwatchlogs.DeleteLogGroupInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DeleteLogGroupOutput, error)
	ListTagsForResource(context.Context, *cloudwatchlogs.ListTagsForResourceInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.ListTagsForResourceOutput, error)
}
type EventsAPI interface {
	ListTargetsByRule(context.Context, *eventbridge.ListTargetsByRuleInput, ...func(*eventbridge.Options)) (*eventbridge.ListTargetsByRuleOutput, error)
	DescribeRule(context.Context, *eventbridge.DescribeRuleInput, ...func(*eventbridge.Options)) (*eventbridge.DescribeRuleOutput, error)
	ListTagsForResource(context.Context, *eventbridge.ListTagsForResourceInput, ...func(*eventbridge.Options)) (*eventbridge.ListTagsForResourceOutput, error)
	PutRule(context.Context, *eventbridge.PutRuleInput, ...func(*eventbridge.Options)) (*eventbridge.PutRuleOutput, error)
	DisableRule(context.Context, *eventbridge.DisableRuleInput, ...func(*eventbridge.Options)) (*eventbridge.DisableRuleOutput, error)
	DeleteRule(context.Context, *eventbridge.DeleteRuleInput, ...func(*eventbridge.Options)) (*eventbridge.DeleteRuleOutput, error)
	PutTargets(context.Context, *eventbridge.PutTargetsInput, ...func(*eventbridge.Options)) (*eventbridge.PutTargetsOutput, error)
	RemoveTargets(context.Context, *eventbridge.RemoveTargetsInput, ...func(*eventbridge.Options)) (*eventbridge.RemoveTargetsOutput, error)
}
type LambdaENIAPI interface {
	DescribeNetworkInterfaces(context.Context, *ec2.DescribeNetworkInterfacesInput, ...func(*ec2.Options)) (*ec2.DescribeNetworkInterfacesOutput, error)
}
type Deployment struct {
	EC2       LambdaENIAPI
	IAM       IAMAPI
	Lambda    LambdaAPI
	Logs      LogsAPI
	Events    EventsAPI
	WorkerZip []byte
	Sleep     func(context.Context, time.Duration) error
}

func NewDeployment(c sdk.Config, zip []byte) *Deployment {
	return &Deployment{EC2: ec2.NewFromConfig(c), IAM: iam.NewFromConfig(c), Lambda: lambda.NewFromConfig(c), Logs: cloudwatchlogs.NewFromConfig(c), Events: eventbridge.NewFromConfig(c), WorkerZip: zip, Sleep: func(ctx context.Context, d time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}}
}
func deploymentName(c model.Config, mode string) string {
	name := "tilink-" + c.Name + "-" + mode
	if len(name) > 64 {
		hash := sha256.Sum256([]byte(name))
		name = fmt.Sprintf("%s-%x", name[:47], hash[:8])
	}
	return name
}
func deploymentTags(c model.Config, s model.Step) map[string]string {
	return map[string]string{"tilink:connection": c.Name, "tilink:operation": s.OperationID, "tilink:key": s.Key, "tilink:connection-id": s.Properties["connection_id"]}
}
func missing(err error) bool {
	var e smithy.APIError
	return errors.As(err, &e) && (e.ErrorCode() == "ResourceNotFoundException" || e.ErrorCode() == "NoSuchEntity" || e.ErrorCode() == "ResourceNotFound")
}
func exists(err error) bool {
	var e smithy.APIError
	return errors.As(err, &e) && (e.ErrorCode() == "ResourceAlreadyExistsException" || e.ErrorCode() == "EntityAlreadyExists" || e.ErrorCode() == "ResourceConflictException")
}
func (d *Deployment) Plan(_ context.Context, c model.Config) ([]model.Step, error) {
	var out []model.Step
	modes := []string{}
	if c.Maintenance && c.Kind != "app" && c.Target.Type != "msk" {
		modes = append(modes, "reconcile")
	}
	if c.Probe {
		modes = append(modes, "probe")
	}
	for _, mode := range modes {
		prefix := "aws." + mode
		add := func(suffix, kind string, deps ...string) {
			out = append(out, model.Step{Key: prefix + "." + suffix, Provider: "aws", Kind: kind, Action: "create", DependsOn: deps, Properties: map[string]string{"mode": mode}})
		}
		add("role", "iam_role")
		add("logs", "log_group")
		policyDeps := []string{prefix + ".role", prefix + ".logs", "aws.network.sg"}
		if mode == "reconcile" {
			policyDeps = append(policyDeps, "aws.listener.primary", "aws.tg.primary", "aws.tg.primary.standby", "aws.health_listener.primary")
			for _, b := range c.Target.Brokers {
				key := "broker-" + b.ID
				policyDeps = append(policyDeps, "aws.listener."+key, "aws.tg."+key, "aws.tg."+key+".standby", "aws.health_listener."+key)
			}
		}
		add("policy", "iam_policy", policyDeps...)
		deps := []string{prefix + ".policy", "aws.network.sg"}
		if mode == "reconcile" {
			deps = append(deps, "aws.elb.endpoint", "aws.listener.primary")
		}
		add("function", "lambda_function", deps...)
		add("concurrency", "lambda_concurrency", prefix+".function")
		if mode == "reconcile" {
			add("rule", "event_rule", prefix+".concurrency")
			add("permission", "lambda_permission", prefix+".function", prefix+".rule")
			add("target", "event_target", prefix+".permission", prefix+".rule")
		}
	}
	return out, nil
}
func depResource(resources []model.Resource, key string) (model.Resource, error) {
	for _, r := range resources {
		if r.Key == key && r.Status != "deleted" {
			return r, nil
		}
	}
	return model.Resource{}, fmt.Errorf("missing dependency %s", key)
}
func runtimePolicy(c model.Config, resources []model.Resource, mode, group string) string {
	type statement struct {
		Effect    string   `json:"Effect"`
		Action    []string `json:"Action"`
		Resource  any      `json:"Resource"`
		Condition any      `json:"Condition,omitempty"`
	}
	eniBase := "arn:aws:ec2:" + c.AWS.Region + ":" + c.AWS.AccountID + ":"
	eniResources := []string{eniBase + "network-interface/*"}
	for _, id := range c.AWS.SubnetIDs {
		eniResources = append(eniResources, eniBase+"subnet/"+id)
	}
	for _, r := range resources {
		if r.Key == "aws.network.sg" {
			eniResources = append(eniResources, eniBase+"security-group/"+r.ID)
		}
	}
	statements := []statement{{Effect: "Allow", Action: []string{"logs:CreateLogStream", "logs:PutLogEvents"}, Resource: "arn:aws:logs:" + c.AWS.Region + ":" + c.AWS.AccountID + ":log-group:" + group + ":*"},
		{Effect: "Allow", Action: []string{"ec2:DescribeNetworkInterfaces", "ec2:DescribeSubnets"}, Resource: "*"},
		{Effect: "Allow", Action: []string{"ec2:CreateNetworkInterface"}, Resource: eniResources},
		{Effect: "Allow", Action: []string{"ec2:DeleteNetworkInterface", "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses"}, Resource: eniBase + "network-interface/*"}}

	if mode == "reconcile" {
		var tgs, listeners []string
		for _, r := range resources {
			if r.Kind == "target_group" && r.Ownership == model.Created {
				tgs = append(tgs, r.ID)
			}
			if r.Kind == "listener" && r.Ownership == model.Created {
				listeners = append(listeners, r.ID)
			}
		}
		statements = append(statements, statement{Effect: "Allow", Action: []string{"elasticloadbalancing:DescribeListeners", "elasticloadbalancing:DescribeTargetHealth"}, Resource: "*"})
		if len(tgs) > 0 {
			statements = append(statements, statement{Effect: "Allow", Action: []string{"elasticloadbalancing:RegisterTargets", "elasticloadbalancing:DeregisterTargets"}, Resource: tgs})
		}
		if len(listeners) > 0 {
			statements = append(statements, statement{Effect: "Allow", Action: []string{"elasticloadbalancing:ModifyListener"}, Resource: listeners})
		}
	}
	b, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statements})
	return string(b)
}
func workerEvent(c model.Config, resources []model.Resource, mode string) (worker.Event, error) {
	e := worker.Event{Action: mode, Region: c.AWS.Region, TimeoutSeconds: 40}
	if mode != "reconcile" {
		return e, nil
	}
	for _, r := range resources {
		if r.Kind != "listener" || r.Properties["health_for"] != "" {
			continue
		}
		key := r.Properties["target_key"]
		tg, err := depResource(resources, key)
		if err != nil {
			return e, err
		}
		standby, err := depResource(resources, key+".standby")
		if err != nil {
			return e, err
		}
		port := int32(c.Target.Port)
		_, _ = fmt.Sscan(tg.Properties["port"], &port)
		host := tg.Properties["host"]
		if host == "" {
			host = c.Target.Host
		}
		health := ""
		for _, candidate := range resources {
			if candidate.Kind == "listener" && candidate.Properties["health_for"] == r.Key {
				if health != "" || candidate.Properties["target_key"] != key+".standby" {
					return e, fmt.Errorf("invalid health listener binding")
				}
				health = candidate.ID
			}
		}
		if health == "" {
			return e, fmt.Errorf("missing health listener for %s", r.Key)
		}
		e.Bindings = append(e.Bindings, worker.Binding{Hostname: host, Port: port, ListenerARN: r.ID, HealthListenerARN: health, TargetGroupARNs: [2]string{tg.ID, standby.ID}})
	}
	if len(e.Bindings) == 0 {
		return e, fmt.Errorf("no complete owned reconciliation bindings")
	}
	return e, nil
}
func (d *Deployment) Apply(ctx context.Context, c model.Config, s model.Step, resources []model.Resource) (model.Resource, error) {
	mode := s.Properties["mode"]
	name := deploymentName(c, mode)
	prefix := "aws." + mode
	r := model.Resource{Key: s.Key, Provider: "aws", Kind: s.Kind, Ownership: model.Created, DependsOn: s.DependsOn, OperationID: s.OperationID, Status: "ready", Properties: map[string]string{"mode": mode, "name": name, "connection_id": s.Properties["connection_id"]}, ObservedAt: time.Now().UTC()}
	tags := deploymentTags(c, s)
	var err error
	switch s.Kind {
	case "iam_role":
		got, e := d.IAM.GetRole(ctx, &iam.GetRoleInput{RoleName: sdk.String(name)})
		if e == nil {
			if !roleOwned(got.Role, c, s) {
				return r, fmt.Errorf("role ownership conflict")
			}
			r.ID = sdk.ToString(got.Role.Arn)
			return r, nil
		}
		if !missing(e) {
			return r, e
		}
		ts := []it.Tag{}
		for k, v := range tags {
			ts = append(ts, it.Tag{Key: sdk.String(k), Value: sdk.String(v)})
		}
		out, e := d.IAM.CreateRole(ctx, &iam.CreateRoleInput{RoleName: sdk.String(name), AssumeRolePolicyDocument: sdk.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`), Tags: ts})
		if e != nil {
			return r, e
		}
		r.ID = sdk.ToString(out.Role.Arn)
	case "iam_policy":
		role, e := depResource(resources, prefix+".role")
		if e != nil {
			return r, e
		}
		if e = d.requireRole(ctx, c, role); e != nil {
			return r, e
		}
		policy := runtimePolicy(c, resources, mode, "/aws/lambda/"+name)
		existing, e := d.IAM.GetRolePolicy(ctx, &iam.GetRolePolicyInput{RoleName: sdk.String(name), PolicyName: sdk.String("tilink-runtime")})
		if e == nil {
			decoded, _ := url.QueryUnescape(sdk.ToString(existing.PolicyDocument))
			if !equalJSON(decoded, policy) {
				return r, fmt.Errorf("role policy changed outside tilink")
			}
		} else if !missing(e) {
			return r, e
		} else {
			_, err = d.IAM.PutRolePolicy(ctx, &iam.PutRolePolicyInput{RoleName: sdk.String(name), PolicyName: sdk.String("tilink-runtime"), PolicyDocument: sdk.String(policy)})
		}
		r.ID = name + "/tilink-runtime"
		r.Properties["document"] = policy
	case "log_group":
		group := "/aws/lambda/" + name
		r.ID = "arn:aws:logs:" + c.AWS.Region + ":" + c.AWS.AccountID + ":log-group:" + group
		r.Properties["group"] = group
		got, e := d.Logs.ListTagsForResource(ctx, &cloudwatchlogs.ListTagsForResourceInput{ResourceArn: sdk.String(r.ID)})
		if e == nil {
			if !deploymentTagsOwned(got.Tags, c, s) {
				return r, fmt.Errorf("log group ownership conflict")
			}
		} else {
			if !missing(e) {
				return r, e
			}
			_, e = d.Logs.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: sdk.String(group), Tags: tags})
			if e != nil {
				return r, e
			}
		}
		_, err = d.Logs.PutRetentionPolicy(ctx, &cloudwatchlogs.PutRetentionPolicyInput{LogGroupName: sdk.String(group), RetentionInDays: sdk.Int32(14)})
	case "lambda_function":
		got, e := d.Lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: sdk.String(name)})
		if e == nil {
			if !deploymentTagsOwned(got.Tags, c, s) {
				return r, fmt.Errorf("function ownership conflict")
			}
			r.ID = sdk.ToString(got.Configuration.FunctionArn)
			if got.Configuration.VpcConfig != nil && len(got.Configuration.VpcConfig.SecurityGroupIds) == 1 {
				r.Properties["vpc_id"] = sdk.ToString(got.Configuration.VpcConfig.VpcId)
				r.Properties["security_group_id"] = got.Configuration.VpcConfig.SecurityGroupIds[0]
			} else {
				return r, fmt.Errorf("function VPC identity is unavailable; cannot establish cleanup safety")
			}
			if got.Configuration.State != lt.StateActive {
				r.Status = "pending"
			}
			return r, nil
		}
		if !missing(e) {
			return r, e
		}
		if len(d.WorkerZip) == 0 {
			return r, fmt.Errorf("worker package unavailable; use a release build")
		}
		role, e := depResource(resources, prefix+".role")
		if e != nil {
			return r, e
		}
		sg, e := depResource(resources, "aws.network.sg")
		if e != nil {
			return r, e
		}
		r.Properties["security_group_id"] = sg.ID
		r.Properties["vpc_id"] = c.AWS.VPCID
		event, e := workerEvent(c, resources, mode)
		if e != nil {
			return r, e
		}
		payload, _ := json.Marshal(event)
		in := &lambda.CreateFunctionInput{FunctionName: sdk.String(name), Role: sdk.String(role.ID), Runtime: lt.RuntimeProvidedal2023, Handler: sdk.String("bootstrap"), Architectures: []lt.Architecture{lt.ArchitectureArm64}, Code: &lt.FunctionCode{ZipFile: d.WorkerZip}, Timeout: sdk.Int32(45), MemorySize: sdk.Int32(128), Tags: tags, Environment: &lt.Environment{Variables: map[string]string{"TILINK_EVENT": string(payload)}}, VpcConfig: &lt.VpcConfig{SubnetIds: c.AWS.SubnetIDs, SecurityGroupIds: []string{sg.ID}}}
		var out *lambda.CreateFunctionOutput
		for attempt := 0; attempt < 6; attempt++ {
			out, err = d.Lambda.CreateFunction(ctx, in)
			if err == nil {
				break
			}
			var api smithy.APIError
			if !errors.As(err, &api) || api.ErrorCode() != "InvalidParameterValueException" || !strings.Contains(api.ErrorMessage(), "role") {
				return r, err
			}
			if e = d.Sleep(ctx, 5*time.Second); e != nil {
				return r, e
			}
		}
		if err == nil {
			r.ID = sdk.ToString(out.FunctionArn)
			if out.State != lt.StateActive {
				r.Status = "pending"
			}
		}
	case "lambda_concurrency":
		function, e := depResource(resources, prefix+".function")
		if e != nil {
			return r, e
		}
		if e = d.requireFunction(ctx, c, function); e != nil {
			return r, e
		}
		_, err = d.Lambda.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{FunctionName: sdk.String(name), ReservedConcurrentExecutions: sdk.Int32(1)})
		r.ID = function.ID + "/concurrency"
	case "event_rule":
		got, e := d.Events.DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: sdk.String(name)})
		if e == nil {
			t, e := d.Events.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{ResourceARN: got.Arn})
			if e != nil {
				return r, e
			}
			m := map[string]string{}
			for _, t := range t.Tags {
				m[sdk.ToString(t.Key)] = sdk.ToString(t.Value)
			}
			if !deploymentTagsOwned(m, c, s) {
				return r, fmt.Errorf("rule ownership conflict")
			}
			r.ID = sdk.ToString(got.Arn)
			return r, nil
		}
		if !missing(e) {
			return r, e
		}
		ts := []et.Tag{}
		for k, v := range tags {
			ts = append(ts, et.Tag{Key: sdk.String(k), Value: sdk.String(v)})
		}
		o, e := d.Events.PutRule(ctx, &eventbridge.PutRuleInput{Name: sdk.String(name), ScheduleExpression: sdk.String("rate(1 minute)"), State: et.RuleStateEnabled, Tags: ts})
		err = e
		if o != nil {
			r.ID = sdk.ToString(o.RuleArn)
		}
	case "lambda_permission":
		function, e := depResource(resources, prefix+".function")
		if e != nil {
			return r, e
		}
		rule, e := depResource(resources, prefix+".rule")
		if e != nil {
			return r, e
		}
		if e = d.requireFunction(ctx, c, function); e != nil {
			return r, e
		}
		_, err = d.Lambda.AddPermission(ctx, &lambda.AddPermissionInput{FunctionName: sdk.String(name), StatementId: sdk.String("tilink-schedule"), Action: sdk.String("lambda:InvokeFunction"), Principal: sdk.String("events.amazonaws.com"), SourceArn: sdk.String(rule.ID), SourceAccount: sdk.String(c.AWS.AccountID)})
		if exists(err) {
			err = d.checkPermission(ctx, c, name, rule.ID)
		}
		r.ID = function.ID + "/tilink-schedule"
		r.Properties["source_arn"] = rule.ID
	case "event_target":
		function, e := depResource(resources, prefix+".function")
		if e != nil {
			return r, e
		}
		rule, e := depResource(resources, prefix+".rule")
		if e != nil {
			return r, e
		}
		if e = d.requireRule(ctx, c, rule); e != nil {
			return r, e
		}
		event, e := workerEvent(c, resources, mode)
		if e != nil {
			return r, e
		}
		payload, _ := json.Marshal(event)
		out, e := d.Events.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: sdk.String(name), Targets: []et.Target{{Id: sdk.String("tilink"), Arn: sdk.String(function.ID), Input: sdk.String(string(payload)), RetryPolicy: &et.RetryPolicy{MaximumEventAgeInSeconds: sdk.Int32(60), MaximumRetryAttempts: sdk.Int32(0)}}}})
		err = e
		if out != nil && out.FailedEntryCount > 0 {
			err = fmt.Errorf("schedule target rejected")
		}
		r.ID = rule.ID + "/tilink"
		r.Properties["rule_arn"] = rule.ID
	default:
		return r, fmt.Errorf("unsupported deployment resource %s", s.Kind)
	}
	return r, err
}
func deploymentTagsOwned(tags map[string]string, c model.Config, s model.Step) bool {
	return tags["tilink:connection"] == c.Name && tags["tilink:key"] == s.Key && (s.OperationID == "" || tags["tilink:operation"] == s.OperationID) && (s.Properties["connection_id"] == "" || tags["tilink:connection-id"] == s.Properties["connection_id"])
}
func roleOwned(role *it.Role, c model.Config, s model.Step) bool {
	if role == nil {
		return false
	}
	tags := map[string]string{}
	for _, t := range role.Tags {
		tags[sdk.ToString(t.Key)] = sdk.ToString(t.Value)
	}
	return deploymentTagsOwned(tags, c, s)
}
func (d *Deployment) requireRole(ctx context.Context, c model.Config, r model.Resource) error {
	o, e := d.IAM.GetRole(ctx, &iam.GetRoleInput{RoleName: sdk.String(r.Properties["name"])})
	if e != nil {
		return e
	}
	if !roleOwned(o.Role, c, model.Step{Key: r.Key, OperationID: r.OperationID, Properties: r.Properties}) || sdk.ToString(o.Role.Arn) != r.ID {
		return fmt.Errorf("role ownership conflict")
	}
	return nil
}
func (d *Deployment) requireFunction(ctx context.Context, c model.Config, r model.Resource) error {
	o, e := d.Lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: sdk.String(r.Properties["name"])})
	if e != nil {
		return e
	}
	if !deploymentTagsOwned(o.Tags, c, model.Step{Key: r.Key, OperationID: r.OperationID, Properties: r.Properties}) || o.Configuration == nil || sdk.ToString(o.Configuration.FunctionArn) != r.ID {
		return fmt.Errorf("function ownership conflict")
	}
	return nil
}
func (d *Deployment) requireRule(ctx context.Context, c model.Config, r model.Resource) error {
	o, e := d.Events.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{ResourceARN: sdk.String(r.ID)})
	if e != nil {
		return e
	}
	tags := map[string]string{}
	for _, t := range o.Tags {
		tags[sdk.ToString(t.Key)] = sdk.ToString(t.Value)
	}
	if !deploymentTagsOwned(tags, c, model.Step{Key: r.Key, OperationID: r.OperationID, Properties: r.Properties}) {
		return fmt.Errorf("rule ownership conflict")
	}
	return nil
}
