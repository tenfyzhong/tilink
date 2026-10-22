package aws

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	lt "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/tenfyzhong/tilink/internal/model"
)

type Provider struct {
	EC2        *ec2.Client
	ELB        *elasticloadbalancingv2.Client
	STS        *sts.Client
	RDS        *rds.Client
	Kafka      *kafka.Client
	WorkerZip  []byte
	Deployment *Deployment
}

func New(ctx context.Context, c model.Config, options ...func(*Provider)) (*Provider, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(c.AWS.Region)}
	if c.AWS.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(c.AWS.Profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration failed")
	}
	p := &Provider{EC2: ec2.NewFromConfig(cfg), ELB: elasticloadbalancingv2.NewFromConfig(cfg), STS: sts.NewFromConfig(cfg), RDS: rds.NewFromConfig(cfg), Kafka: kafka.NewFromConfig(cfg)}
	for _, o := range options {
		o(p)
	}
	p.Deployment = NewDeployment(cfg, p.WorkerZip)
	return p, nil
}
func resourceID(rs []model.Resource, key string) (string, error) {
	for _, r := range rs {
		if r.Key == key && r.ID != "" && r.Status != "deleted" {
			return r.ID, nil
		}
	}
	return "", fmt.Errorf("resource dependency %s is unavailable", key)
}
func name(c model.Config, key string) string {
	s := "tilink-" + c.Name + "-" + strings.ReplaceAll(key, ".", "-")
	if len(s) > 32 {
		sum := sha256.Sum256([]byte(s))
		s = fmt.Sprintf("%.21s-%x", s, sum[:5])
	}
	return s
}
func ecTags(c model.Config, s model.Step) []et.Tag {
	return []et.Tag{{Key: sdk.String("tilink:connection"), Value: sdk.String(c.Name)}, {Key: sdk.String("tilink:key"), Value: sdk.String(s.Key)}, {Key: sdk.String("tilink:operation"), Value: sdk.String(s.OperationID)}, {Key: sdk.String("tilink:connection-id"), Value: sdk.String(s.Properties["connection_id"])}}
}
func elbTags(c model.Config, s model.Step) []lt.Tag {
	return []lt.Tag{{Key: sdk.String("tilink:connection"), Value: sdk.String(c.Name)}, {Key: sdk.String("tilink:key"), Value: sdk.String(s.Key)}, {Key: sdk.String("tilink:operation"), Value: sdk.String(s.OperationID)}, {Key: sdk.String("tilink:connection-id"), Value: sdk.String(s.Properties["connection_id"])}}
}
func (p *Provider) identity(ctx context.Context, c model.Config) error {
	if p.STS == nil {
		return fmt.Errorf("AWS identity client unavailable")
	}
	out, err := p.STS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("AWS account identity check failed")
	}
	if sdk.ToString(out.Account) != c.AWS.AccountID {
		return fmt.Errorf("AWS credentials do not match configured account")
	}
	return nil
}
func (p *Provider) Plan(ctx context.Context, c model.Config) ([]model.Step, error) {
	if err := p.discover(ctx, c); err != nil {
		return nil, err
	}
	if c.Kind != "app" && c.Target.Type != "msk" && !c.Maintenance {
		hosts := []string{c.Target.Host}
		for _, b := range c.Target.Brokers {
			hosts = append(hosts, b.Host)
		}
		for _, host := range hosts {
			if _, e := privateAddresses(ctx, host); e != nil {
				return nil, fmt.Errorf("maintenance disabled: target must resolve to private IPv4 addresses from the CLI host")
			}
		}
	}
	if err := p.discoverTarget(ctx, c); err != nil {
		return nil, err
	}
	steps, e := networkPlan(c)
	if e != nil {
		return nil, e
	}
	extra, e := p.planDeployment(ctx, c)
	return append(steps, extra...), e
}
func networkPlan(c model.Config) ([]model.Step, error) {
	var out []model.Step
	add := func(key, kind string, deps []string, props map[string]string) {
		out = append(out, model.Step{Key: key, Provider: "aws", Kind: kind, Action: "create", DependsOn: deps, Properties: props})
	}
	add("aws.network.sg", "security_group", nil, map[string]string{"purpose": "network"})
	if c.Kind == "app" {
		for _, cidr := range c.AWS.ClientCIDRs {
			ip, network, e := net.ParseCIDR(cidr)
			if e != nil || ip.To4() == nil || !ip.IsPrivate() {
				return nil, fmt.Errorf("client CIDRs must be private IPv4 networks")
			}
			ones, bits := network.Mask.Size()
			last := append(net.IP(nil), network.IP...)
			for i := range last {
				last[i] |= ^network.Mask[i]
			}
			if bits != 32 || ones < 8 || !last.IsPrivate() {
				return nil, fmt.Errorf("client CIDRs must remain inside private IPv4 ranges")
			}
		}
		if len(c.AWS.SecurityGroupIDs) == 0 && len(c.AWS.ClientCIDRs) == 0 {
			return nil, fmt.Errorf("application endpoint requires client security groups or private CIDRs")
		}
		if c.TiDB.ServiceName == "" {
			return nil, fmt.Errorf("TiDB endpoint service_name must be discovered before AWS planning")
		}
		port := c.TiDB.Port
		if port == 0 {
			port = 4000
		}
		add("aws.network.ingress", "security_group_rule", []string{"aws.network.sg"}, map[string]string{"port": strconv.Itoa(port), "group_key": "aws.network.sg", "sources": strings.Join(c.AWS.SecurityGroupIDs, ","), "cidrs": strings.Join(c.AWS.ClientCIDRs, ",")})
		endpointDeps := []string{"aws.network.sg", "aws.network.ingress"}
		if c.Probe {
			add("aws.probe.ingress", "security_group_rule", []string{"aws.network.sg"}, map[string]string{"group_key": "aws.network.sg", "source_key": "aws.network.sg", "port": strconv.Itoa(port)})
			endpointDeps = append(endpointDeps, "aws.probe.ingress")
		}
		add("aws.endpoint", "interface_endpoint", endpointDeps, map[string]string{"service_name": c.TiDB.ServiceName})
		return out, nil
	}
	if c.Target.Type == "msk" {
		if c.TiDB.PrivateLinkID == "" {
			out = append(out, model.Step{Key: "aws.msk.action", Provider: "aws", Kind: "manual", Action: "manual", Manual: "Grant the TiDB principal in the existing MSK cluster policy, then create the managed private connection in TiDB Cloud and supply tidb.private_link_id. Shared cluster policy changes require operator review."})
		}
		return out, nil
	}

	if c.TiDB.PrincipalARN == "" {
		return nil, fmt.Errorf("TiDB principal_arn is required for endpoint service authorization")
	}

	if c.Target.SecurityGroupID != "" {
		add("aws.target.ingress", "security_group_rule", []string{"aws.network.sg"}, map[string]string{"port": strconv.Itoa(c.Target.Port), "group_id": c.Target.SecurityGroupID, "source_key": "aws.network.sg"})
	}
	depsNLB := []string{"aws.network.sg"}
	if c.Target.SecurityGroupID != "" {
		depsNLB = append(depsNLB, "aws.target.ingress")
	}
	seenPorts := map[int]bool{c.Target.Port: true}
	for _, broker := range c.Target.Brokers {
		if !seenPorts[broker.Port] && c.Target.SecurityGroupID != "" {
			key := "aws.target.ingress." + strconv.Itoa(broker.Port)
			add(key, "security_group_rule", []string{"aws.network.sg"}, map[string]string{"port": strconv.Itoa(broker.Port), "group_id": c.Target.SecurityGroupID, "source_key": "aws.network.sg"})
			depsNLB = append(depsNLB, key)
			seenPorts[broker.Port] = true
		}
	}
	add("aws.nlb", "nlb", depsNLB, nil)
	targets := []struct {
		key, host    string
		port, listen int
	}{{"primary", c.Target.Host, c.Target.Port, c.Target.Port}}
	for _, b := range c.Target.Brokers {
		targets = append(targets, struct {
			key, host    string
			port, listen int
		}{"broker-" + b.ID, b.Host, b.Port, b.ListenerPort})
	}
	keys := make([]string, 0, len(targets))
	usedPorts := map[int]bool{}
	for _, t := range targets {
		keys = append(keys, t.key)
		usedPorts[t.listen] = true
	}
	sort.Strings(keys)
	healthPorts := map[string]int{}
	candidate := 40000
	for _, key := range keys {
		for usedPorts[candidate] && candidate <= 65535 {
			candidate++
		}
		if candidate > 65535 {
			return nil, fmt.Errorf("no free private health listener port")
		}
		healthPorts[key] = candidate
		usedPorts[candidate] = true
		candidate++
	}
	initialization := "local"
	if c.Maintenance {
		initialization = "deferred"
	}
	deps := []string{"aws.nlb"}
	for _, t := range targets {
		key := "aws.tg." + t.key
		add(key, "target_group", nil, map[string]string{"host": t.host, "port": strconv.Itoa(t.port), "initialization": initialization})
		add(key+".standby", "target_group", nil, map[string]string{"host": t.host, "port": strconv.Itoa(t.port), "standby": "true", "initialization": initialization})
		mainKey := "aws.listener." + t.key
		healthKey := "aws.health_listener." + t.key
		add(mainKey, "listener", []string{"aws.nlb", key}, map[string]string{"target_key": key, "port": strconv.Itoa(t.listen)})
		add(healthKey, "listener", []string{"aws.nlb", key + ".standby"}, map[string]string{"target_key": key + ".standby", "port": strconv.Itoa(healthPorts[t.key]), "health_for": mainKey})
		deps = append(deps, mainKey, healthKey)
	}

	add("aws.endpoint_service", "endpoint_service", deps, nil)
	add("aws.service_permission", "service_permission", []string{"aws.endpoint_service"}, map[string]string{"principal": c.TiDB.PrincipalARN})
	if c.Maintenance {
		if c.AWS.ELBEndpointID != "" {
			add("aws.elb.endpoint", "interface_endpoint", nil, map[string]string{"existing_id": c.AWS.ELBEndpointID})
		} else {
			add("aws.elb.ingress", "security_group_rule", []string{"aws.network.sg"}, map[string]string{"port": "443", "group_key": "aws.network.sg", "source_key": "aws.network.sg"})
			add("aws.elb.endpoint", "interface_endpoint", []string{"aws.network.sg", "aws.elb.ingress"}, map[string]string{"service_name": "com.amazonaws." + c.AWS.Region + ".elasticloadbalancing", "private_dns": "true"})
		}
	}
	return out, nil
}
func (p *Provider) discover(ctx context.Context, c model.Config) error {
	if err := p.identity(ctx, c); err != nil {
		return err
	}
	sub, err := p.EC2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: c.AWS.SubnetIDs})
	if err != nil {
		return fmt.Errorf("describe selected subnets failed")
	}
	if len(sub.Subnets) != len(c.AWS.SubnetIDs) {
		return fmt.Errorf("selected subnet inventory is incomplete")
	}
	seen := map[string]bool{}
	for _, s := range sub.Subnets {
		if sdk.ToString(s.VpcId) != c.AWS.VPCID {
			return fmt.Errorf("selected subnet belongs to another VPC")
		}
		az := sdk.ToString(s.AvailabilityZoneId)
		if seen[az] {
			return fmt.Errorf("select at most one subnet per availability zone")
		}
		seen[az] = true
	}
	if len(c.TiDB.AZIDs) > 0 {
		for az := range seen {
			ok := false
			for _, want := range c.TiDB.AZIDs {
				ok = ok || az == want
			}
			if !ok {
				return fmt.Errorf("selected subnet AZ ID %s is unavailable to TiDB", az)
			}
		}
	}
	if c.Kind == "app" && c.TiDB.ServiceName != "" {
		o, e := p.EC2.DescribeVpcEndpointServices(ctx, &ec2.DescribeVpcEndpointServicesInput{ServiceNames: []string{c.TiDB.ServiceName}})
		if e != nil || len(o.ServiceDetails) != 1 {
			return fmt.Errorf("TiDB endpoint service is unavailable in configured region")
		}
		for az := range seen {
			ok := false
			for _, available := range o.ServiceDetails[0].AvailabilityZoneIds {
				ok = ok || available == az
			}
			if !ok {
				return fmt.Errorf("selected subnet AZ ID is unavailable in endpoint service")
			}
		}
	}
	return nil
}
func privateAddresses(ctx context.Context, host string) ([]string, error) {
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("target DNS lookup failed")
	}
	var out []string
	for _, a := range ips {
		if !a.IP.IsPrivate() || a.IP.To4() == nil {
			return nil, fmt.Errorf("target DNS must resolve exclusively to private IPv4 addresses")
		}
		out = append(out, a.IP.String())
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil, fmt.Errorf("target DNS has no private IPv4 addresses")
	}
	return out, nil
}
func (p *Provider) Apply(ctx context.Context, c model.Config, s model.Step, rs []model.Resource) (model.Resource, error) {
	if deploymentKind(s.Kind) {
		return p.applyDeployment(ctx, c, s, rs)
	}
	r := model.Resource{Key: s.Key, Provider: "aws", Kind: s.Kind, Ownership: model.Created, DependsOn: s.DependsOn, OperationID: s.OperationID, Status: "ready", ObservedAt: time.Now().UTC(), Properties: map[string]string{"connection": c.Name, "account_id": c.AWS.AccountID, "region": c.AWS.Region}}
	for k, v := range s.Properties {
		r.Properties[k] = v
	}
	if err := p.identity(ctx, c); err != nil {
		return r, err
	}
	id, err := p.create(ctx, c, s, rs)
	r.ID = id
	if err != nil {
		return r, err
	}
	r.ID = id
	if s.Properties["existing_id"] != "" {
		r.Ownership = model.Reused
	}
	if s.Kind == "security_group_rule" || s.Kind == "service_permission" || s.Kind == "private_dns" || s.Kind == "endpoint_acceptance" {
		r.Ownership = model.Modified
	}
	return p.Refresh(ctx, c, r)
}
