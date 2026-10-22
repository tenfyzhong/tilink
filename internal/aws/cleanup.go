package aws

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/smithy-go"
	"github.com/tenfyzhong/tilink/internal/model"
	"strings"
	"time"
)

func notFound(err error) bool {
	var e smithy.APIError
	if !errors.As(err, &e) {
		return false
	}
	switch e.ErrorCode() {
	case "InvalidGroup.NotFound", "InvalidVpcEndpointId.NotFound", "InvalidServiceId.NotFound", "InvalidSecurityGroupRuleId.NotFound", "LoadBalancerNotFound", "TargetGroupNotFound", "ListenerNotFound":
		return true
	}
	return false
}
func tagsOwned(tags []et.Tag, c model.Config, key string, expected ...string) bool {
	vals := map[string]string{}
	for _, t := range tags {
		vals[sdk.ToString(t.Key)] = sdk.ToString(t.Value)
	}
	return vals["tilink:connection"] == c.Name && vals["tilink:key"] == key && (len(expected) == 0 || expected[0] == "" || vals["tilink:connection-id"] == expected[0])
}
func (p *Provider) checkELBTags(ctx context.Context, c model.Config, id, key string, expected ...string) error {
	o, e := p.ELB.DescribeTags(ctx, &elasticloadbalancingv2.DescribeTagsInput{ResourceArns: []string{id}})
	if e != nil {
		if notFound(e) {
			return e
		}
		return fmt.Errorf("load balancer ownership lookup failed")
	}
	if len(o.TagDescriptions) != 1 {
		return fmt.Errorf("resource ownership is unknown")
	}
	tags := map[string]string{}
	for _, t := range o.TagDescriptions[0].Tags {
		tags[sdk.ToString(t.Key)] = sdk.ToString(t.Value)
	}
	if tags["tilink:connection"] != c.Name || tags["tilink:key"] != key || len(expected) > 0 && expected[0] != "" && tags["tilink:connection-id"] != expected[0] {
		return fmt.Errorf("resource ownership conflicts with local record")
	}
	return nil
}
func metadataOwned(c model.Config, r model.Resource) bool {
	return r.Ownership != model.Reused && r.Properties["account_id"] == c.AWS.AccountID && r.Properties["region"] == c.AWS.Region && r.Properties["connection"] == c.Name && r.ID != ""
}
func (p *Provider) Refresh(ctx context.Context, c model.Config, r model.Resource) (model.Resource, error) {
	if deploymentKind(r.Kind) {
		return p.refreshDeployment(ctx, c, r)
	}
	r.ObservedAt = time.Now().UTC()
	if !metadataOwned(c, r) && r.Ownership != model.Reused {
		return r, fmt.Errorf("AWS resource record does not prove ownership")
	}
	if err := p.identity(ctx, c); err != nil {
		return r, err
	}
	var err error
	switch r.Kind {
	case "nlb", "listener", "target_group":
		err = p.checkELBTags(ctx, c, r.ID, r.Key, r.Properties["connection_id"])
		if err == nil && r.Kind == "nlb" {
			o, e := p.ELB.DescribeLoadBalancers(ctx, &elasticloadbalancingv2.DescribeLoadBalancersInput{LoadBalancerArns: []string{r.ID}})
			err = e
			if e == nil && len(o.LoadBalancers) > 0 {
				r.Status = string(o.LoadBalancers[0].State.Code)
				r.Properties["dns_name"] = sdk.ToString(o.LoadBalancers[0].DNSName)
			}
		}
	case "security_group":
		o, e := p.EC2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{r.ID}})
		err = e
		if e == nil && len(o.SecurityGroups) == 0 {
			r.Status = "deleted"
			return r, nil
		}
		if e == nil && (len(o.SecurityGroups) != 1 || !tagsOwned(o.SecurityGroups[0].Tags, c, r.Key, r.Properties["connection_id"])) {
			err = fmt.Errorf("security group ownership conflicts")
		}
	case "security_group_rule":
		parts := strings.SplitN(r.ID, "/", 2)
		if len(parts) != 2 {
			return r, fmt.Errorf("invalid rule record")
		}
		var live []string
		for _, id := range strings.Split(parts[1], ",") {
			o, e := p.EC2.DescribeSecurityGroupRules(ctx, &ec2.DescribeSecurityGroupRulesInput{SecurityGroupRuleIds: []string{id}})
			if notFound(e) {
				continue
			}
			if e != nil {
				return r, fmt.Errorf("inspect owned security group rule failed")
			}
			if len(o.SecurityGroupRules) == 0 {
				continue
			}
			if len(o.SecurityGroupRules) != 1 {
				return r, fmt.Errorf("ambiguous security group rule inventory")
			}
			rule := o.SecurityGroupRules[0]
			if !tagsOwned(rule.Tags, c, r.Key, r.Properties["connection_id"]) || sdk.ToString(rule.GroupId) != parts[0] || sdk.ToString(rule.Description) != "tilink:"+c.Name+":"+r.Key {
				return r, fmt.Errorf("security group rule was externally modified")
			}
			data, _ := json.Marshal(rule)
			fingerprint := fmt.Sprintf("%x", sha256.Sum256(data))
			key := "rule_fingerprint." + id
			if previous := r.Properties[key]; previous != "" && previous != fingerprint {
				return r, fmt.Errorf("security group rule was externally modified")
			}
			r.Properties[key] = fingerprint
			live = append(live, id)
		}
		if len(live) == 0 {
			r.Status = "deleted"
			return r, nil
		}
		r.ID = parts[0] + "/" + strings.Join(live, ",")

	case "interface_endpoint":
		o, e := p.EC2.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{VpcEndpointIds: []string{r.ID}})
		err = e
		if e == nil && len(o.VpcEndpoints) == 0 {
			r.Status = "deleted"
			return r, nil
		}
		if e == nil && len(o.VpcEndpoints) == 1 {
			v := o.VpcEndpoints[0]
			if r.Ownership != model.Reused && !tagsOwned(v.Tags, c, r.Key, r.Properties["connection_id"]) {
				err = fmt.Errorf("endpoint ownership conflicts")
			}
			r.Status = string(v.State)
			r.Properties["private_dns_enabled"] = fmt.Sprint(sdk.ToBool(v.PrivateDnsEnabled))
			if sdk.ToString(v.VpcId) != c.AWS.VPCID {
				err = fmt.Errorf("endpoint VPC conflicts")
			}
		}
	case "endpoint_service":
		o, e := p.EC2.DescribeVpcEndpointServiceConfigurations(ctx, &ec2.DescribeVpcEndpointServiceConfigurationsInput{ServiceIds: []string{r.ID}})
		err = e
		if e == nil && len(o.ServiceConfigurations) == 0 {
			r.Status = "deleted"
			return r, nil
		}
		if e == nil && len(o.ServiceConfigurations) == 1 {
			s := o.ServiceConfigurations[0]
			if !tagsOwned(s.Tags, c, r.Key, r.Properties["connection_id"]) {
				err = fmt.Errorf("endpoint service ownership conflicts")
			}
			r.Status = string(s.ServiceState)
			r.Properties["service_name"] = sdk.ToString(s.ServiceName)
		}
	case "private_dns":
		parent := r
		parent.Key = "aws.endpoint"
		parent.Kind = "interface_endpoint"
		parent.Ownership = model.Created
		refreshed, e := p.Refresh(ctx, c, parent)
		err = e
		if e == nil && refreshed.Status == "deleted" {
			r.Status = "deleted"
		} else if e == nil && refreshed.Properties["private_dns_enabled"] != "true" {
			r.Status = "deleted"
		}
	case "endpoint_acceptance":
		parts := strings.SplitN(r.ID, "/", 2)
		if len(parts) != 2 {
			return r, fmt.Errorf("invalid endpoint acceptance record")
		}
		parent := r
		parent.ID = parts[0]
		parent.Key = "aws.endpoint_service"
		parent.Kind = "endpoint_service"
		parent.Ownership = model.Created
		_, err = p.Refresh(ctx, c, parent)
		if err == nil {
			out, e := p.EC2.DescribeVpcEndpointConnections(ctx, &ec2.DescribeVpcEndpointConnectionsInput{Filters: []et.Filter{{Name: sdk.String("service-id"), Values: []string{parts[0]}}, {Name: sdk.String("vpc-endpoint-id"), Values: []string{parts[1]}}}})
			err = e
			if e == nil {
				if len(out.VpcEndpointConnections) == 0 {
					r.Status = "deleted"
				} else {
					r.Status = string(out.VpcEndpointConnections[0].VpcEndpointState)
					if strings.EqualFold(r.Status, "rejected") || strings.EqualFold(r.Status, "deleted") {
						r.Status = "deleted"
					}
				}
			}
		}
	case "service_permission":
		service := strings.SplitN(r.ID, "/", 2)[0]
		parent := r
		parent.ID = service
		parent.Kind = "endpoint_service"
		parent.Key = "aws.endpoint_service"
		parent.Ownership = model.Created
		_, err = p.Refresh(ctx, c, parent)
		if err == nil {
			permissions, e := p.EC2.DescribeVpcEndpointServicePermissions(ctx, &ec2.DescribeVpcEndpointServicePermissionsInput{ServiceId: sdk.String(service)})
			err = e
			if e == nil {
				present := false
				for _, permission := range permissions.AllowedPrincipals {
					present = present || sdk.ToString(permission.Principal) == r.Properties["principal"]
				}
				if !present {
					r.Status = "deleted"
				}
			}
		}
	default:
		return r, fmt.Errorf("unsupported AWS refresh kind %s", r.Kind)
	}
	if notFound(err) {
		r.Status = "deleted"
		return r, nil
	}
	if err != nil {
		r.Status = "unknown"
		return r, fmt.Errorf("refresh %s failed: %w", r.Kind, err)
	}
	r.Status = strings.ToLower(r.Status)
	if r.Status == "pendingacceptance" {
		r.Status = "pending_acceptance"
	}
	if r.Status == "Available" {
		r.Status = "available"
	}
	if r.Status == "" {
		r.Status = "ready"
	}
	return r, nil
}
func (p *Provider) Delete(ctx context.Context, c model.Config, r model.Resource) error {
	if deploymentKind(r.Kind) {
		return p.deleteDeployment(ctx, c, r)
	}
	if !metadataOwned(c, r) {
		return fmt.Errorf("refusing deletion without matching owned account, region and connection")
	}
	current, err := p.Refresh(ctx, c, r)
	if err != nil {
		return err
	}
	if current.Status == "deleted" {
		return nil
	}
	r = current
	switch r.Kind {
	case "private_dns":
		_, err = p.EC2.ModifyVpcEndpoint(ctx, &ec2.ModifyVpcEndpointInput{VpcEndpointId: sdk.String(r.ID), PrivateDnsEnabled: sdk.Bool(false)})
	case "endpoint_acceptance":
		parts := strings.SplitN(r.ID, "/", 2)
		o, e := p.EC2.RejectVpcEndpointConnections(ctx, &ec2.RejectVpcEndpointConnectionsInput{ServiceId: sdk.String(parts[0]), VpcEndpointIds: []string{parts[1]}})
		err = e
		if e == nil && len(o.Unsuccessful) > 0 {
			err = fmt.Errorf("reject consumer endpoint failed")
		}
	case "security_group":
		_, err = p.EC2.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: sdk.String(r.ID)})
	case "security_group_rule":
		parts := strings.SplitN(r.ID, "/", 2)
		_, err = p.EC2.RevokeSecurityGroupIngress(ctx, &ec2.RevokeSecurityGroupIngressInput{GroupId: sdk.String(parts[0]), SecurityGroupRuleIds: strings.Split(parts[1], ",")})
	case "nlb":
		_, err = p.ELB.DeleteLoadBalancer(ctx, &elasticloadbalancingv2.DeleteLoadBalancerInput{LoadBalancerArn: sdk.String(r.ID)})
	case "listener":
		_, err = p.ELB.DeleteListener(ctx, &elasticloadbalancingv2.DeleteListenerInput{ListenerArn: sdk.String(r.ID)})
	case "target_group":
		_, err = p.ELB.DeleteTargetGroup(ctx, &elasticloadbalancingv2.DeleteTargetGroupInput{TargetGroupArn: sdk.String(r.ID)})
	case "interface_endpoint":
		o, e := p.EC2.DeleteVpcEndpoints(ctx, &ec2.DeleteVpcEndpointsInput{VpcEndpointIds: []string{r.ID}})
		err = e
		if e == nil && len(o.Unsuccessful) > 0 {
			err = fmt.Errorf("AWS endpoint deletion was unsuccessful")
		}
	case "endpoint_service":
		present, e := p.activeServiceConsumers(ctx, r.ID)
		if e != nil {
			return e
		}
		if present {
			return fmt.Errorf("endpoint service has consumer connections; remove them before deleting service")
		}

		out, e := p.EC2.DeleteVpcEndpointServiceConfigurations(ctx, &ec2.DeleteVpcEndpointServiceConfigurationsInput{ServiceIds: []string{r.ID}})
		err = e
		if e == nil && len(out.Unsuccessful) > 0 {
			err = fmt.Errorf("AWS endpoint service deletion was unsuccessful")
		}
	case "service_permission":
		parts := strings.SplitN(r.ID, "/", 2)
		_, err = p.EC2.ModifyVpcEndpointServicePermissions(ctx, &ec2.ModifyVpcEndpointServicePermissionsInput{ServiceId: sdk.String(parts[0]), RemoveAllowedPrincipals: []string{parts[1]}})
	default:
		return fmt.Errorf("unsupported AWS deletion kind %s", r.Kind)
	}
	if notFound(err) {
		return nil
	}
	if err == nil && (r.Kind == "nlb" || r.Kind == "interface_endpoint" || r.Kind == "endpoint_service") {
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		err = waitUntilAbsent(waitCtx, 2*time.Second, func() (bool, error) {
			switch r.Kind {
			case "nlb":
				out, e := p.ELB.DescribeLoadBalancers(waitCtx, &elasticloadbalancingv2.DescribeLoadBalancersInput{LoadBalancerArns: []string{r.ID}})
				if notFound(e) {
					return true, nil
				}
				return e == nil && len(out.LoadBalancers) == 0, e
			case "interface_endpoint":
				out, e := p.EC2.DescribeVpcEndpoints(waitCtx, &ec2.DescribeVpcEndpointsInput{VpcEndpointIds: []string{r.ID}})
				if notFound(e) {
					return true, nil
				}
				return e == nil && len(out.VpcEndpoints) == 0, e
			default:
				out, e := p.EC2.DescribeVpcEndpointServiceConfigurations(waitCtx, &ec2.DescribeVpcEndpointServiceConfigurationsInput{ServiceIds: []string{r.ID}})
				if notFound(e) {
					return true, nil
				}
				return e == nil && len(out.ServiceConfigurations) == 0, e
			}
		})
	}
	if err != nil {
		return fmt.Errorf("delete %s failed", r.Kind)
	}
	return nil
}

func deploymentKind(k string) bool {
	return strings.HasPrefix(k, "lambda_") || strings.HasPrefix(k, "iam_") || strings.HasPrefix(k, "event_") || k == "log_group"
}

func waitUntilAbsent(ctx context.Context, interval time.Duration, probe func() (bool, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := probe()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
