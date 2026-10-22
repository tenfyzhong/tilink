package aws

import (
	"context"
	"fmt"
	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/tenfyzhong/tilink/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testProvider(t *testing.T, handle func(string) string) (*Provider, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if e := r.ParseForm(); e != nil {
			t.Error(e)
		}
		action := r.Form.Get("Action")
		body := handle(action)
		if body == "" {
			t.Errorf("unexpected AWS action %s", action)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	cfg := sdk.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	return &Provider{ELB: elasticloadbalancingv2.NewFromConfig(cfg, func(o *elasticloadbalancingv2.Options) { o.BaseEndpoint = sdk.String(srv.URL) }), STS: sts.NewFromConfig(cfg, func(o *sts.Options) { o.BaseEndpoint = sdk.String(srv.URL) }), EC2: ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.BaseEndpoint = sdk.String(srv.URL) })}, &calls
}

const identityXML = `<GetCallerIdentityResponse><GetCallerIdentityResult><Account>123</Account><Arn>arn:aws:iam::123:role/test</Arn><UserId>test</UserId></GetCallerIdentityResult></GetCallerIdentityResponse>`

func TestDeleteChecksCloudOwnershipBeforeMutation(t *testing.T) {
	p, calls := testProvider(t, func(action string) string {
		switch action {
		case "GetCallerIdentity":
			return identityXML
		case "DescribeSecurityGroups":
			return `<DescribeSecurityGroupsResponse><securityGroupInfo><item><groupId>sg-1</groupId><tagSet><item><key>tilink:connection</key><value>someone-else</value></item></tagSet></item></securityGroupInfo></DescribeSecurityGroupsResponse>`
		}
		return ""
	})
	c := model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
	r := model.Resource{ID: "sg-1", Key: "aws.network.sg", Kind: "security_group", Ownership: model.Created, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders"}}
	if e := p.Delete(context.Background(), c, r); e == nil {
		t.Fatal("foreign cloud ownership accepted")
	}
	if *calls != 2 {
		t.Fatal(*calls)
	}
}
func TestAccountMismatchStopsDiscovery(t *testing.T) {
	p, calls := testProvider(t, func(action string) string {
		if action == "GetCallerIdentity" {
			return identityXML
		}
		return ""
	})
	e := p.discover(context.Background(), model.Config{AWS: model.AWSConfig{AccountID: "456"}})
	if e == nil || !strings.Contains(e.Error(), "account") {
		t.Fatal(e)
	}
	if *calls != 1 {
		t.Fatal(*calls)
	}
}
func TestDiscoveryUsesAZIDsAndRejectsWrongVPC(t *testing.T) {
	p, _ := testProvider(t, func(action string) string {
		if action == "GetCallerIdentity" {
			return identityXML
		}
		if action == "DescribeSubnets" {
			return `<DescribeSubnetsResponse><subnetSet><item><subnetId>subnet-1</subnetId><vpcId>vpc-foreign</vpcId><availabilityZoneId>use1-az1</availabilityZoneId></item></subnetSet></DescribeSubnetsResponse>`
		}
		return ""
	})
	e := p.discover(context.Background(), model.Config{AWS: model.AWSConfig{AccountID: "123", VPCID: "vpc-1", SubnetIDs: []string{"subnet-1"}}})
	if e == nil || !strings.Contains(e.Error(), "VPC") {
		t.Fatal(e)
	}
}
func TestDeleteMissingResourceIsIdempotent(t *testing.T) {
	p, _ := testProvider(t, func(action string) string {
		if action == "GetCallerIdentity" {
			return identityXML
		}
		if action == "DescribeSecurityGroups" {
			return `<DescribeSecurityGroupsResponse><securityGroupInfo/></DescribeSecurityGroupsResponse>`
		}
		return ""
	})
	c := model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
	r := model.Resource{ID: "sg-gone", Key: "aws.network.sg", Kind: "security_group", Ownership: model.Created, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders"}}
	if e := p.Delete(context.Background(), c, r); e != nil {
		t.Fatal(e)
	}
}
func TestEndpointPendingAcceptanceNormalized(t *testing.T) {
	p, _ := testProvider(t, func(action string) string {
		if action == "GetCallerIdentity" {
			return identityXML
		}
		if action == "DescribeVpcEndpoints" {
			return `<DescribeVpcEndpointsResponse><vpcEndpointSet><item><vpcEndpointId>vpce-1</vpcEndpointId><vpcId>vpc-1</vpcId><state>PendingAcceptance</state><tagSet><item><key>tilink:connection</key><value>orders</value></item><item><key>tilink:key</key><value>aws.endpoint</value></item></tagSet></item></vpcEndpointSet></DescribeVpcEndpointsResponse>`
		}
		return ""
	})
	r, e := p.Refresh(context.Background(), model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1", VPCID: "vpc-1"}}, model.Resource{ID: "vpce-1", Key: "aws.endpoint", Kind: "interface_endpoint", Ownership: model.Created, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders"}})
	if e != nil || r.Status != "pending_acceptance" {
		t.Fatal(r.Status, e)
	}
}
func TestRefreshKeepsRemainingAggregatedRules(t *testing.T) {
	lookups := 0
	p, _ := testProvider(t, func(action string) string {
		if action == "GetCallerIdentity" {
			return identityXML
		}
		if action == "DescribeSecurityGroupRules" {
			lookups++
			if lookups == 1 {
				return `<DescribeSecurityGroupRulesResponse><securityGroupRuleSet/></DescribeSecurityGroupRulesResponse>`
			}
			return `<DescribeSecurityGroupRulesResponse><securityGroupRuleSet><item><securityGroupRuleId>rule-b</securityGroupRuleId><groupId>sg-1</groupId><description>tilink:orders:ingress</description><tagSet><item><key>tilink:connection</key><value>orders</value></item><item><key>tilink:key</key><value>ingress</value></item></tagSet></item></securityGroupRuleSet></DescribeSecurityGroupRulesResponse>`
		}
		return ""
	})
	r, e := p.Refresh(context.Background(), model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}, model.Resource{ID: "sg-1/rule-a,rule-b", Key: "ingress", Kind: "security_group_rule", Ownership: model.Modified, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders"}})
	if e != nil || r.Status == "deleted" || r.ID != "sg-1/rule-b" {
		t.Fatal(r, e)
	}
}
func TestRefreshPrivateDNSDriftIsMissingAction(t *testing.T) {
	p, _ := testProvider(t, func(action string) string {
		if action == "GetCallerIdentity" {
			return identityXML
		}
		if action == "DescribeVpcEndpoints" {
			return `<DescribeVpcEndpointsResponse><vpcEndpointSet><item><vpcEndpointId>vpce-1</vpcEndpointId><vpcId>vpc-1</vpcId><state>Available</state><privateDnsEnabled>false</privateDnsEnabled><tagSet><item><key>tilink:connection</key><value>orders</value></item><item><key>tilink:key</key><value>aws.endpoint</value></item></tagSet></item></vpcEndpointSet></DescribeVpcEndpointsResponse>`
		}
		return ""
	})
	r, e := p.Refresh(context.Background(), model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1", VPCID: "vpc-1"}}, model.Resource{ID: "vpce-1", Key: "aws.private_dns", Kind: "private_dns", Ownership: model.Modified, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders"}})
	if e != nil || r.Status != "deleted" {
		t.Fatal(r, e)
	}
}
func TestRefreshRemovedPrincipalIsMissingAction(t *testing.T) {
	p, _ := testProvider(t, func(action string) string {
		switch action {
		case "GetCallerIdentity":
			return identityXML
		case "DescribeVpcEndpointServiceConfigurations":
			return `<DescribeVpcEndpointServiceConfigurationsResponse><serviceConfigurationSet><item><serviceId>vpce-svc-1</serviceId><serviceState>Available</serviceState><tagSet><item><key>tilink:connection</key><value>orders</value></item><item><key>tilink:key</key><value>aws.endpoint_service</value></item></tagSet></item></serviceConfigurationSet></DescribeVpcEndpointServiceConfigurationsResponse>`
		case "DescribeVpcEndpointServicePermissions":
			return `<DescribeVpcEndpointServicePermissionsResponse><allowedPrincipals/></DescribeVpcEndpointServicePermissionsResponse>`
		}
		return ""
	})
	r, e := p.Refresh(context.Background(), model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}, model.Resource{ID: "vpce-svc-1/arn:aws:iam::456:root", Key: "aws.service_permission", Kind: "service_permission", Ownership: model.Modified, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders", "principal": "arn:aws:iam::456:root"}})
	if e != nil || r.Status != "deleted" {
		t.Fatal(r, e)
	}
}
func TestManagedTargetInitializationDoesNotResolveLocalDNS(t *testing.T) {
	p, _ := testProvider(t, func(action string) string {
		switch action {
		case "DescribeTargetGroups":
			return `<DescribeTargetGroupsResponse><DescribeTargetGroupsResult><TargetGroups><member><TargetGroupArn>arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/example/id</TargetGroupArn></member></TargetGroups></DescribeTargetGroupsResult></DescribeTargetGroupsResponse>`
		case "DescribeTags":
			return `<DescribeTagsResponse><DescribeTagsResult><TagDescriptions><member><ResourceArn>arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/example/id</ResourceArn><Tags><member><Key>tilink:connection</Key><Value>orders</Value></member><member><Key>tilink:key</Key><Value>aws.tg.primary</Value></member></Tags></member></TagDescriptions></DescribeTagsResult></DescribeTagsResponse>`
		}
		return ""
	})
	c := model.Config{Name: "orders", Maintenance: true}
	s := model.Step{Key: "aws.tg.primary", Kind: "target_group", Properties: map[string]string{"host": "only-vpc.internal.invalid", "port": "3306", "initialization": "deferred"}}
	id, e := p.create(context.Background(), c, s, nil)
	if e != nil || id == "" {
		t.Fatal(id, e)
	}
}
