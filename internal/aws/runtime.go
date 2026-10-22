package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ect "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lt "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/tenfyzhong/tilink/internal/model"
	"github.com/tenfyzhong/tilink/internal/worker"
)

func (p *Provider) planDeployment(ctx context.Context, c model.Config) ([]model.Step, error) {
	return p.Deployment.Plan(ctx, c)
}
func (p *Provider) applyDeployment(ctx context.Context, c model.Config, s model.Step, r []model.Resource) (model.Resource, error) {
	return p.Deployment.Apply(ctx, c, s, r)
}
func (p *Provider) refreshDeployment(ctx context.Context, c model.Config, r model.Resource) (model.Resource, error) {
	return p.Deployment.Refresh(ctx, c, r)
}
func (p *Provider) deleteDeployment(ctx context.Context, c model.Config, r model.Resource) error {
	return p.Deployment.Delete(ctx, c, r)
}
func (d *Deployment) InvokeOwned(ctx context.Context, c model.Config, r model.Resource, event worker.Event) (worker.Result, error) {
	if r.Kind != "lambda_function" || r.Ownership != model.Created || r.OperationID == "" || r.Properties["connection_id"] == "" {
		return worker.Result{}, fmt.Errorf("worker ownership evidence is incomplete")
	}
	mode := r.Properties["mode"]
	if mode != "probe" && mode != "reconcile" {
		return worker.Result{}, fmt.Errorf("invalid worker purpose")
	}
	if event.Action != "" && event.Action != mode {
		return worker.Result{}, fmt.Errorf("worker purpose does not match invocation")
	}
	if err := d.requireFunction(ctx, c, r); err != nil {
		return worker.Result{}, fmt.Errorf("worker ownership verification failed")
	}
	return d.invoke(ctx, r.ID, event)
}
func (d *Deployment) invoke(ctx context.Context, id string, event worker.Event) (worker.Result, error) {
	var result worker.Result
	payload, e := json.Marshal(event)
	if e != nil {
		return result, fmt.Errorf("invalid worker invocation")
	}
	ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	out, e := d.Lambda.Invoke(ctx, &lambda.InvokeInput{FunctionName: sdk.String(id), InvocationType: lt.InvocationTypeRequestResponse, LogType: lt.LogTypeNone, Payload: payload})
	if e != nil {
		return result, fmt.Errorf("worker invocation failed")
	}
	if out.FunctionError != nil {
		return result, fmt.Errorf("worker execution failed")
	}
	if out.StatusCode != 200 {
		return result, fmt.Errorf("worker invocation returned status %d", out.StatusCode)
	}
	if json.Unmarshal(out.Payload, &result) != nil || (result.Status != "pass" && result.Status != "partial" && result.Status != "fail") {
		return result, fmt.Errorf("invalid worker result")
	}
	return result, nil
}
func (d *Deployment) parentFunction(ctx context.Context, c model.Config, r model.Resource) error {
	return d.requireFunction(ctx, c, model.Resource{Key: "aws." + r.Properties["mode"] + ".function", ID: functionARN(c, r.Properties["name"]), Properties: r.Properties})
}
func functionARN(c model.Config, name string) string {
	return "arn:aws:lambda:" + c.AWS.Region + ":" + c.AWS.AccountID + ":function:" + name
}
func (d *Deployment) parentRole(ctx context.Context, c model.Config, r model.Resource) error {
	return d.requireRole(ctx, c, model.Resource{Key: "aws." + r.Properties["mode"] + ".role", ID: "arn:aws:iam::" + c.AWS.AccountID + ":role/" + r.Properties["name"], Properties: r.Properties})
}
func (d *Deployment) parentRule(ctx context.Context, c model.Config, r model.Resource) error {
	return d.requireRule(ctx, c, model.Resource{Key: "aws." + r.Properties["mode"] + ".rule", ID: "arn:aws:events:" + c.AWS.Region + ":" + c.AWS.AccountID + ":rule/" + r.Properties["name"], Properties: r.Properties})
}
func (d *Deployment) Refresh(ctx context.Context, c model.Config, r model.Resource) (model.Resource, error) {
	var err error
	if r.Properties == nil {
		r.Properties = map[string]string{}
	}
	name := r.Properties["name"]
	switch r.Kind {
	case "iam_role":
		err = d.requireRole(ctx, c, r)
	case "iam_policy":
		if err = d.parentRole(ctx, c, r); err == nil {
			o, e := d.IAM.GetRolePolicy(ctx, &iam.GetRolePolicyInput{RoleName: sdk.String(name), PolicyName: sdk.String("tilink-runtime")})
			err = e
			if e == nil {
				doc, _ := url.QueryUnescape(sdk.ToString(o.PolicyDocument))
				if expected := r.Properties["document"]; expected != "" && !equalJSON(doc, expected) {
					err = fmt.Errorf("role policy changed outside tilink")
				}
			}
		}
	case "lambda_function":
		err = d.requireFunction(ctx, c, r)
		if err == nil {
			o, e := d.Lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: sdk.String(name)})
			err = e
			if e == nil && o.Configuration != nil {
				r.Properties["state"] = string(o.Configuration.State)
				r.Properties["update_status"] = string(o.Configuration.LastUpdateStatus)
				if o.Configuration.State != lt.StateActive {
					r.Status = "pending"
				} else {
					r.Status = "ready"
				}
			}
		}
	case "lambda_concurrency":
		err = d.parentFunction(ctx, c, r)
		if err == nil {
			o, e := d.Lambda.GetFunctionConcurrency(ctx, &lambda.GetFunctionConcurrencyInput{FunctionName: sdk.String(name)})
			err = e
			if e == nil {
				r.Properties["concurrency"] = strconv.Itoa(int(sdk.ToInt32(o.ReservedConcurrentExecutions)))
			}
		}
	case "lambda_permission":
		err = d.parentFunction(ctx, c, r)
		if err == nil {
			err = d.checkPermission(ctx, c, name, r.Properties["source_arn"])
		}
	case "event_rule":
		err = d.requireRule(ctx, c, r)
		if err == nil {
			o, e := d.Events.DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: sdk.String(name)})
			err = e
			if e == nil {
				r.Properties["rule_state"] = string(o.State)
			}
		}
	case "event_target":
		err = d.parentRule(ctx, c, r)
		if err == nil {
			var token *string
			found := false
			for {
				o, e := d.Events.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{Rule: sdk.String(name), NextToken: token})
				if e != nil {
					err = e
					break
				}
				for _, t := range o.Targets {
					if sdk.ToString(t.Id) == "tilink" {
						found = true
						if sdk.ToString(t.Arn) != functionARN(c, name) {
							err = fmt.Errorf("schedule target changed outside tilink")
						}
						r.Properties["input"] = sdk.ToString(t.Input)
					}
				}
				if o.NextToken == nil {
					break
				}
				token = o.NextToken
			}
			if err == nil && !found {
				r.Status = "deleted"
			}
		}
	case "log_group":
		o, e := d.Logs.ListTagsForResource(ctx, &cloudwatchlogs.ListTagsForResourceInput{ResourceArn: sdk.String(r.ID)})
		err = e
		if err == nil && !deploymentTagsOwned(o.Tags, c, model.Step{Key: r.Key, OperationID: r.OperationID, Properties: r.Properties}) {
			err = fmt.Errorf("log group ownership conflict")
		}
	default:
		return r, fmt.Errorf("unsupported deployment resource %s", r.Kind)
	}
	if missing(err) {
		if r.Kind == "lambda_function" {
			pending, e := d.lambdaENIsRemain(ctx, &r)
			if e != nil {
				return r, e
			}
			if pending {
				r.Status = "deleting"
			} else {
				r.Status = "deleted"
			}
		} else {
			r.Status = "deleted"
		}
		err = nil
	}
	r.ObservedAt = time.Now().UTC()
	return r, err
}
func (d *Deployment) Delete(ctx context.Context, c model.Config, r model.Resource) error {
	if r.Ownership != model.Created {
		return fmt.Errorf("refusing to delete unowned deployment resource")
	}
	fresh, e := d.Refresh(ctx, c, r)
	if e != nil {
		return e
	}
	if fresh.Status == "deleted" {
		return nil
	}
	name := r.Properties["name"]
	if r.Kind == "lambda_function" && fresh.Status == "deleting" {
		return d.waitLambdaENIs(ctx, r)
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	switch r.Kind {
	case "event_target":
		_, e = d.Events.DisableRule(ctx, &eventbridge.DisableRuleInput{Name: sdk.String(name)})
		if e != nil && !missing(e) {
			return e
		}
		out, err := d.Events.RemoveTargets(ctx, &eventbridge.RemoveTargetsInput{Rule: sdk.String(name), Ids: []string{"tilink"}})
		e = err
		if out != nil && out.FailedEntryCount > 0 {
			return fmt.Errorf("schedule target removal failed")
		}
	case "lambda_permission":
		_, e = d.Lambda.RemovePermission(ctx, &lambda.RemovePermissionInput{FunctionName: sdk.String(name), StatementId: sdk.String("tilink-schedule")})
	case "event_rule":
		_, e = d.Events.DisableRule(ctx, &eventbridge.DisableRuleInput{Name: sdk.String(name)})
		if e == nil {
			_, e = d.Events.DeleteRule(ctx, &eventbridge.DeleteRuleInput{Name: sdk.String(name)})
		}
	case "lambda_concurrency":
		// Keeping zero concurrency blocks new work until the function is deleted.
		_, e = d.Lambda.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{FunctionName: sdk.String(name), ReservedConcurrentExecutions: sdk.Int32(0)})
	case "lambda_function":
		_, e = d.Lambda.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{FunctionName: sdk.String(name), ReservedConcurrentExecutions: sdk.Int32(0)})
		if e != nil {
			return e
		}
		// A previous invocation is bounded by the configured 45-second timeout.
		if e = d.Sleep(ctx, 46*time.Second); e != nil {
			return e
		}
		_, e = d.Lambda.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: sdk.String(name)})
		if e == nil {
			for attempt := 0; attempt < 15; attempt++ {
				observed := d.requireFunction(ctx, c, r)
				if missing(observed) {
					return d.waitLambdaENIs(ctx, r)
				}
				if observed != nil {
					return observed
				}
				if attempt < 14 {
					if err := d.Sleep(ctx, 2*time.Second); err != nil {
						return err
					}
				}
			}
			return fmt.Errorf("function deletion is still pending; retry cleanup")
		}
	case "iam_policy":
		_, e = d.IAM.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{RoleName: sdk.String(name), PolicyName: sdk.String("tilink-runtime")})
	case "iam_role":
		_, e = d.IAM.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: sdk.String(name)})
	case "log_group":
		_, e = d.Logs.DeleteLogGroup(ctx, &cloudwatchlogs.DeleteLogGroupInput{LogGroupName: sdk.String(r.Properties["group"])})
	default:
		return fmt.Errorf("unsupported deployment resource %s", r.Kind)
	}
	if missing(e) {
		return nil
	}
	return e
}

func equalJSON(a, b string) bool {
	var x, y any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	xx, _ := json.Marshal(x)
	yy, _ := json.Marshal(y)
	return string(xx) == string(yy)
}
func (d *Deployment) checkPermission(ctx context.Context, c model.Config, name, source string) error {
	o, e := d.Lambda.GetPolicy(ctx, &lambda.GetPolicyInput{FunctionName: sdk.String(name)})
	if e != nil {
		return e
	}
	var p struct {
		Statement []struct {
			Sid       string
			Effect    string
			Action    string
			Principal map[string]string
			Condition map[string]map[string]string
		}
	}
	if json.Unmarshal([]byte(sdk.ToString(o.Policy)), &p) != nil {
		return fmt.Errorf("invalid function policy")
	}
	for _, s := range p.Statement {
		if s.Sid != "tilink-schedule" {
			continue
		}
		if s.Effect != "Allow" || s.Action != "lambda:InvokeFunction" || s.Principal["Service"] != "events.amazonaws.com" || s.Condition["ArnLike"]["AWS:SourceArn"] != source || s.Condition["StringEquals"]["AWS:SourceAccount"] != c.AWS.AccountID {
			return fmt.Errorf("function schedule permission changed outside tilink")
		}
		return nil
	}
	return &lt.ResourceNotFoundException{}
}

// Lambda owns these interfaces. Observation only protects role dependencies.
func (d *Deployment) lambdaENIsRemain(ctx context.Context, r *model.Resource) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	sg, vpc := r.Properties["security_group_id"], r.Properties["vpc_id"]
	if sg == "" || vpc == "" {
		return false, fmt.Errorf("Lambda VPC cleanup scope is missing; retain execution role and recover the function security group/VPC identity")
	}
	var token *string
	var ids []string
	for {
		o, e := d.EC2.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{Filters: []ect.Filter{{Name: sdk.String("interface-type"), Values: []string{"lambda"}}, {Name: sdk.String("group-id"), Values: []string{sg}}, {Name: sdk.String("vpc-id"), Values: []string{vpc}}}, NextToken: token})
		if e != nil {
			return false, fmt.Errorf("cannot verify Lambda-managed ENI cleanup; retain execution role and retry")
		}
		for _, eni := range o.NetworkInterfaces {
			if eni.InterfaceType != ect.NetworkInterfaceTypeLambda || sdk.ToString(eni.VpcId) != vpc {
				continue
			}
			for _, g := range eni.Groups {
				if sdk.ToString(g.GroupId) == sg {
					ids = append(ids, sdk.ToString(eni.NetworkInterfaceId))
					break
				}
			}
		}
		if o.NextToken == nil {
			break
		}
		token = o.NextToken
	}
	sort.Strings(ids)
	r.Properties["lambda_eni_ids"] = strings.Join(ids, ",")
	if len(ids) > 0 {
		r.Properties["cleanup_status"] = "Waiting for AWS to release Lambda-managed ENIs; retain execution role and retry destroy. Shared functions or versions may keep these interfaces in use."
	} else {
		delete(r.Properties, "cleanup_status")
	}
	return len(ids) > 0, nil
}
func (d *Deployment) waitLambdaENIs(ctx context.Context, r model.Resource) error {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	for attempt := 0; attempt < 15; attempt++ {
		pending, e := d.lambdaENIsRemain(ctx, &r)
		if e != nil {
			return e
		}
		if !pending {
			return nil
		}
		if attempt < 14 {
			if e = d.Sleep(ctx, 2*time.Second); e != nil {
				return fmt.Errorf("Lambda-managed ENI cleanup is pending; execution role retained; retry destroy")
			}
		}
	}
	return fmt.Errorf("Lambda-managed ENI cleanup is pending; execution role retained; retry destroy after AWS releases ENIs (this may take 20 minutes or longer)")
}
