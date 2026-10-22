package aws

import (
	"context"
	"fmt"
	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	lt "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/tenfyzhong/tilink/internal/model"
	"strconv"
	"strings"
	"time"
)

func (p *Provider) create(ctx context.Context, c model.Config, s model.Step, rs []model.Resource) (string, error) {
	props := s.Properties
	port, _ := strconv.Atoi(props["port"])
	dep := func(k string) (string, error) { return resourceID(rs, k) }
	switch s.Kind {
	case "private_dns":
		endpoint, e := dep("aws.endpoint")
		if e != nil {
			return "", e
		}
		_, e = p.EC2.ModifyVpcEndpoint(ctx, &ec2.ModifyVpcEndpointInput{VpcEndpointId: sdk.String(endpoint), PrivateDnsEnabled: sdk.Bool(true)})
		if e != nil {
			return "", fmt.Errorf("enable private DNS after registration failed")
		}
		return endpoint, nil
	case "endpoint_acceptance":
		service, e := dep("aws.endpoint_service")
		if e != nil {
			return "", e
		}
		endpoint := props["endpoint_id"]
		if endpoint == "" {
			for _, r := range rs {
				if r.Key == "tidb.private_link" {
					endpoint = r.Properties["aws_endpoint_id"]
				}
			}
		}
		if endpoint == "" {
			return "", fmt.Errorf("TiDB consumer aws_endpoint_id must be supplied before accepting connection")
		}
		o, e := p.EC2.DescribeVpcEndpointConnections(ctx, &ec2.DescribeVpcEndpointConnectionsInput{Filters: []et.Filter{{Name: sdk.String("service-id"), Values: []string{service}}, {Name: sdk.String("vpc-endpoint-id"), Values: []string{endpoint}}}})
		if e != nil || len(o.VpcEndpointConnections) != 1 {
			return "", fmt.Errorf("intended TiDB consumer connection is unavailable")
		}
		expected := strings.Split(c.TiDB.PrincipalARN, ":")
		if len(expected) < 5 || sdk.ToString(o.VpcEndpointConnections[0].VpcEndpointOwner) != expected[4] {
			return "", fmt.Errorf("consumer endpoint account does not match TiDB principal")
		}
		if o.VpcEndpointConnections[0].VpcEndpointState != et.StateAvailable {
			out, e := p.EC2.AcceptVpcEndpointConnections(ctx, &ec2.AcceptVpcEndpointConnectionsInput{ServiceId: sdk.String(service), VpcEndpointIds: []string{endpoint}})
			if e != nil || len(out.Unsuccessful) > 0 {
				return "", fmt.Errorf("accept TiDB consumer endpoint failed")
			}
		}
		return service + "/" + endpoint, nil
	case "security_group":
		found, e := p.EC2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{Filters: []et.Filter{{Name: sdk.String("tag:tilink:connection"), Values: []string{c.Name}}, {Name: sdk.String("tag:tilink:key"), Values: []string{s.Key}}, {Name: sdk.String("tag:tilink:connection-id"), Values: []string{s.Properties["connection_id"]}}, {Name: sdk.String("vpc-id"), Values: []string{c.AWS.VPCID}}}})
		if e != nil {
			return "", fmt.Errorf("recover security group failed")
		}
		if len(found.SecurityGroups) > 0 {
			return sdk.ToString(found.SecurityGroups[0].GroupId), nil
		}
		o, e := p.EC2.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: sdk.String(name(c, s.Key)), Description: sdk.String("tilink private connectivity"), VpcId: sdk.String(c.AWS.VPCID), TagSpecifications: []et.TagSpecification{{ResourceType: et.ResourceTypeSecurityGroup, Tags: ecTags(c, s)}}})
		if e != nil {
			return "", fmt.Errorf("create security group failed")
		}
		return sdk.ToString(o.GroupId), nil
	case "security_group_rule":
		group := props["group_id"]
		if group == "" {
			var e error
			group, e = dep(props["group_key"])
			if e != nil {
				return "", e
			}
		}
		rule := et.IpPermission{IpProtocol: sdk.String("tcp"), FromPort: sdk.Int32(int32(port)), ToPort: sdk.Int32(int32(port))}
		if port == 0 {
			rule.FromPort = sdk.Int32(1)
			rule.ToPort = sdk.Int32(65535)
		}
		sources := props["sources"]
		if props["source_key"] != "" {
			source, e := dep(props["source_key"])
			if e != nil {
				return "", e
			}
			sources = source
		}
		for _, id := range strings.Split(sources, ",") {
			if id != "" {
				rule.UserIdGroupPairs = append(rule.UserIdGroupPairs, et.UserIdGroupPair{GroupId: sdk.String(id), Description: sdk.String("tilink:" + c.Name + ":" + s.Key)})
			}
		}
		for _, cidr := range strings.Split(props["cidrs"], ",") {
			if cidr != "" {
				rule.IpRanges = append(rule.IpRanges, et.IpRange{CidrIp: sdk.String(cidr), Description: sdk.String("tilink:" + c.Name + ":" + s.Key)})
			}
		}
		existing, e := p.EC2.DescribeSecurityGroupRules(ctx, &ec2.DescribeSecurityGroupRulesInput{Filters: []et.Filter{{Name: sdk.String("group-id"), Values: []string{group}}, {Name: sdk.String("tag:tilink:connection"), Values: []string{c.Name}}, {Name: sdk.String("tag:tilink:key"), Values: []string{s.Key}}, {Name: sdk.String("tag:tilink:connection-id"), Values: []string{s.Properties["connection_id"]}}}})
		if e != nil {
			return "", fmt.Errorf("recover security group rules failed")
		}
		if len(existing.SecurityGroupRules) > 0 {
			var ids []string
			for _, r := range existing.SecurityGroupRules {
				ids = append(ids, sdk.ToString(r.SecurityGroupRuleId))
			}
			return group + "/" + strings.Join(ids, ","), nil
		}
		o, e := p.EC2.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{GroupId: sdk.String(group), IpPermissions: []et.IpPermission{rule}, TagSpecifications: []et.TagSpecification{{ResourceType: et.ResourceTypeSecurityGroupRule, Tags: ecTags(c, s)}}})
		if e != nil {
			return "", fmt.Errorf("authorize exact security group rules failed; existing customer rules are not adopted")
		}
		var ids []string
		for _, r := range o.SecurityGroupRules {
			ids = append(ids, sdk.ToString(r.SecurityGroupRuleId))
		}
		return group + "/" + strings.Join(ids, ","), nil
	case "nlb":
		found, e := p.ELB.DescribeLoadBalancers(ctx, &elasticloadbalancingv2.DescribeLoadBalancersInput{Names: []string{name(c, s.Key)}})
		if e == nil && len(found.LoadBalancers) > 0 {
			id := sdk.ToString(found.LoadBalancers[0].LoadBalancerArn)
			if e = p.checkELBTags(ctx, c, id, s.Key, s.Properties["connection_id"]); e != nil {
				return "", e
			}
			if e = p.configureNLB(ctx, id, rs); e != nil {
				return id, e
			}
			return id, nil
		}
		if e != nil && !notFound(e) {
			return "", fmt.Errorf("recover load balancer failed")
		}
		sg, e := dep("aws.network.sg")
		if e != nil {
			return "", e
		}
		o, e := p.ELB.CreateLoadBalancer(ctx, &elasticloadbalancingv2.CreateLoadBalancerInput{Name: sdk.String(name(c, s.Key)), Type: lt.LoadBalancerTypeEnumNetwork, Scheme: lt.LoadBalancerSchemeEnumInternal, Subnets: c.AWS.SubnetIDs, SecurityGroups: []string{sg}, Tags: elbTags(c, s)})
		if e != nil {
			return "", fmt.Errorf("create internal load balancer failed")
		}
		id := sdk.ToString(o.LoadBalancers[0].LoadBalancerArn)
		if e = p.configureNLB(ctx, id, rs); e != nil {
			return id, e
		}
		return id, nil
	case "target_group":
		found, e := p.ELB.DescribeTargetGroups(ctx, &elasticloadbalancingv2.DescribeTargetGroupsInput{Names: []string{name(c, s.Key)}})
		var id string
		if e == nil && len(found.TargetGroups) > 0 {
			id = sdk.ToString(found.TargetGroups[0].TargetGroupArn)
			if e = p.checkELBTags(ctx, c, id, s.Key, s.Properties["connection_id"]); e != nil {
				return "", e
			}
		} else {
			if e != nil && !notFound(e) {
				return "", fmt.Errorf("recover target group failed")
			}
			o, e := p.ELB.CreateTargetGroup(ctx, &elasticloadbalancingv2.CreateTargetGroupInput{Name: sdk.String(name(c, s.Key)), Protocol: lt.ProtocolEnumTcp, Port: sdk.Int32(int32(port)), TargetType: lt.TargetTypeEnumIp, VpcId: sdk.String(c.AWS.VPCID), HealthCheckProtocol: lt.ProtocolEnumTcp, Tags: elbTags(c, s)})
			if e != nil {
				return "", fmt.Errorf("create IP target group failed")
			}
			id = sdk.ToString(o.TargetGroups[0].TargetGroupArn)
		}
		if props["standby"] != "true" && !c.Maintenance {
			ips, e := privateAddresses(ctx, props["host"])
			if e != nil {
				return id, e
			}
			var ts []lt.TargetDescription
			for _, ip := range ips {
				ts = append(ts, lt.TargetDescription{Id: sdk.String(ip), Port: sdk.Int32(int32(port))})
			}
			_, e = p.ELB.RegisterTargets(ctx, &elasticloadbalancingv2.RegisterTargetsInput{TargetGroupArn: sdk.String(id), Targets: ts})
			if e != nil {
				return id, fmt.Errorf("register private targets failed")
			}
		}
		return id, nil
	case "listener":
		lb, e := dep("aws.nlb")
		if e != nil {
			return "", e
		}
		tg, e := dep(props["target_key"])
		if e != nil {
			return "", e
		}
		found, e := p.ELB.DescribeListeners(ctx, &elasticloadbalancingv2.DescribeListenersInput{LoadBalancerArn: sdk.String(lb)})
		if e != nil {
			return "", fmt.Errorf("recover listener failed")
		}
		for _, l := range found.Listeners {
			if sdk.ToInt32(l.Port) == int32(port) {
				id := sdk.ToString(l.ListenerArn)
				if e = p.checkELBTags(ctx, c, id, s.Key, s.Properties["connection_id"]); e != nil {
					return "", e
				}
				return id, nil
			}
		}
		o, e := p.ELB.CreateListener(ctx, &elasticloadbalancingv2.CreateListenerInput{LoadBalancerArn: sdk.String(lb), Port: sdk.Int32(int32(port)), Protocol: lt.ProtocolEnumTcp, DefaultActions: []lt.Action{{Type: lt.ActionTypeEnumForward, TargetGroupArn: sdk.String(tg)}}, Tags: elbTags(c, s)})
		if e != nil {
			return "", fmt.Errorf("create TCP listener failed")
		}
		return sdk.ToString(o.Listeners[0].ListenerArn), nil
	case "endpoint_service":
		lb, e := dep("aws.nlb")
		if e != nil {
			return "", e
		}
		found, e := p.EC2.DescribeVpcEndpointServiceConfigurations(ctx, &ec2.DescribeVpcEndpointServiceConfigurationsInput{Filters: []et.Filter{{Name: sdk.String("tag:tilink:connection"), Values: []string{c.Name}}, {Name: sdk.String("tag:tilink:key"), Values: []string{s.Key}}, {Name: sdk.String("tag:tilink:connection-id"), Values: []string{s.Properties["connection_id"]}}}})
		if e != nil {
			return "", fmt.Errorf("recover endpoint service failed")
		}
		if len(found.ServiceConfigurations) > 0 {
			return sdk.ToString(found.ServiceConfigurations[0].ServiceId), nil
		}
		o, e := p.EC2.CreateVpcEndpointServiceConfiguration(ctx, &ec2.CreateVpcEndpointServiceConfigurationInput{NetworkLoadBalancerArns: []string{lb}, AcceptanceRequired: sdk.Bool(true), ClientToken: sdk.String(s.OperationID), TagSpecifications: []et.TagSpecification{{ResourceType: et.ResourceTypeVpcEndpointService, Tags: ecTags(c, s)}}})
		if e != nil {
			return "", fmt.Errorf("create endpoint service failed")
		}
		return sdk.ToString(o.ServiceConfiguration.ServiceId), nil
	case "service_permission":
		service, e := dep("aws.endpoint_service")
		if e != nil {
			return "", e
		}
		_, e = p.EC2.ModifyVpcEndpointServicePermissions(ctx, &ec2.ModifyVpcEndpointServicePermissionsInput{ServiceId: sdk.String(service), AddAllowedPrincipals: []string{props["principal"]}})
		if e != nil {
			return "", fmt.Errorf("authorize TiDB principal failed")
		}
		return service + "/" + props["principal"], nil
	case "interface_endpoint":
		if props["existing_id"] != "" {
			return props["existing_id"], nil
		}
		sg, e := dep("aws.network.sg")
		if props["group_key"] != "" {
			sg, e = dep(props["group_key"])
		}
		if e != nil {
			return "", e
		}
		found, e := p.EC2.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{Filters: []et.Filter{{Name: sdk.String("tag:tilink:connection"), Values: []string{c.Name}}, {Name: sdk.String("tag:tilink:key"), Values: []string{s.Key}}, {Name: sdk.String("tag:tilink:connection-id"), Values: []string{s.Properties["connection_id"]}}}})
		if e != nil {
			return "", fmt.Errorf("recover VPC endpoint failed")
		}
		if len(found.VpcEndpoints) > 0 {
			return sdk.ToString(found.VpcEndpoints[0].VpcEndpointId), nil
		}
		o, e := p.EC2.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{VpcId: sdk.String(c.AWS.VPCID), VpcEndpointType: et.VpcEndpointTypeInterface, ServiceName: sdk.String(props["service_name"]), SubnetIds: c.AWS.SubnetIDs, SecurityGroupIds: []string{sg}, PrivateDnsEnabled: sdk.Bool(props["private_dns"] == "true"), ClientToken: sdk.String(s.OperationID), TagSpecifications: []et.TagSpecification{{ResourceType: et.ResourceTypeVpcEndpoint, Tags: ecTags(c, s)}}})
		if e != nil {
			return "", fmt.Errorf("create interface endpoint failed")
		}
		return sdk.ToString(o.VpcEndpoint.VpcEndpointId), nil
	default:
		return "", fmt.Errorf("unsupported AWS resource kind %s", s.Kind)
	}
}

func (p *Provider) configureNLB(ctx context.Context, id string, rs []model.Resource) error {
	sg, e := resourceID(rs, "aws.network.sg")
	if e != nil {
		return e
	}
	_, e = p.ELB.SetSecurityGroups(ctx, &elasticloadbalancingv2.SetSecurityGroupsInput{LoadBalancerArn: sdk.String(id), SecurityGroups: []string{sg}, EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic: lt.EnforceSecurityGroupInboundRulesOnPrivateLinkTrafficEnumOff})
	if e != nil {
		return fmt.Errorf("configure PrivateLink security group enforcement failed")
	}
	_, e = p.ELB.ModifyLoadBalancerAttributes(ctx, &elasticloadbalancingv2.ModifyLoadBalancerAttributesInput{LoadBalancerArn: sdk.String(id), Attributes: []lt.LoadBalancerAttribute{{Key: sdk.String("load_balancing.cross_zone.enabled"), Value: sdk.String("true")}}})
	if e != nil {
		return fmt.Errorf("configure cross-zone forwarding failed")
	}
	e = elasticloadbalancingv2.NewLoadBalancerAvailableWaiter(p.ELB).Wait(ctx, &elasticloadbalancingv2.DescribeLoadBalancersInput{LoadBalancerArns: []string{id}}, 2*time.Minute)
	if e != nil {
		return fmt.Errorf("internal load balancer is not ready; apply can resume")
	}
	return nil
}
