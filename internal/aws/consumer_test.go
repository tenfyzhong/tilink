package aws

import (
	"context"
	"testing"
	"time"

	"github.com/tenfyzhong/tilink/internal/model"
)

const ownedServiceXML = `<DescribeVpcEndpointServiceConfigurationsResponse><serviceConfigurationSet><item><serviceId>vpce-svc-1</serviceId><serviceState>Available</serviceState><tagSet><item><key>tilink:connection</key><value>orders</value></item><item><key>tilink:key</key><value>aws.endpoint_service</value></item><item><key>tilink:connection-id</key><value>connection-1</value></item></tagSet></item></serviceConfigurationSet></DescribeVpcEndpointServiceConfigurationsResponse>`

func TestConsumerPresentUsesReadOnlyExactServiceAndEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		present     bool
	}{{"absent", "", false}, {"deleted", "Deleted", false}, {"rejected", "Rejected", false}, {"active", "Available", true}, {"pending", "PendingAcceptance", true}, {"unknown", "FutureState", true}} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := testProvider(t, func(action string) string {
				switch action {
				case "GetCallerIdentity":
					return identityXML
				case "DescribeVpcEndpointServiceConfigurations":
					return ownedServiceXML
				case "DescribeVpcEndpointConnections":
					if tc.state == "" {
						return `<DescribeVpcEndpointConnectionsResponse><vpcEndpointConnectionSet/></DescribeVpcEndpointConnectionsResponse>`
					}
					return `<DescribeVpcEndpointConnectionsResponse><vpcEndpointConnectionSet><item><serviceId>vpce-svc-1</serviceId><vpcEndpointId>vpce-1</vpcEndpointId><vpcEndpointState>` + tc.state + `</vpcEndpointState></item></vpcEndpointConnectionSet></DescribeVpcEndpointConnectionsResponse>`
				}
				return ""
			})
			c := model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
			r := model.Resource{Key: "aws.endpoint_service", ID: "vpce-svc-1", Kind: "endpoint_service", Ownership: model.Created, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders", "connection_id": "connection-1"}}
			present, e := p.ConsumerPresent(context.Background(), c, r, "vpce-1")
			if e != nil || present != tc.present {
				t.Fatal(present, e)
			}
		})
	}
}
func TestConsumerPresentRejectsUnownedOrMissingEndpointBeforeAPI(t *testing.T) {
	p := &Provider{}
	c := model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
	for _, r := range []model.Resource{{Kind: "endpoint_service", Ownership: model.Reused}, {Kind: "nlb", Ownership: model.Created, ID: "lb-1", Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders"}}} {
		if _, e := p.ConsumerPresent(context.Background(), c, r, "vpce-1"); e == nil {
			t.Fatal("unowned service accepted")
		}
	}
	if _, e := p.ConsumerPresent(context.Background(), c, model.Resource{}, ""); e == nil {
		t.Fatal("missing endpoint accepted")
	}
}
func TestConsumerPresentPreservesOwnershipOnUnknownEvidence(t *testing.T) {
	for _, response := range []string{`<broken`, `<DescribeVpcEndpointConnectionsResponse><vpcEndpointConnectionSet><item><serviceId>vpce-svc-other</serviceId><vpcEndpointId>vpce-1</vpcEndpointId><vpcEndpointState>Deleted</vpcEndpointState></item></vpcEndpointConnectionSet></DescribeVpcEndpointConnectionsResponse>`, `<DescribeVpcEndpointConnectionsResponse><vpcEndpointConnectionSet><item><serviceId>vpce-svc-1</serviceId><vpcEndpointId>vpce-other</vpcEndpointId><vpcEndpointState>Deleted</vpcEndpointState></item></vpcEndpointConnectionSet></DescribeVpcEndpointConnectionsResponse>`} {
		p, _ := testProvider(t, func(action string) string {
			switch action {
			case "GetCallerIdentity":
				return identityXML
			case "DescribeVpcEndpointServiceConfigurations":
				return ownedServiceXML
			case "DescribeVpcEndpointConnections":
				return response
			}
			return ""
		})
		c := model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
		service := model.Resource{Key: "aws.endpoint_service", ID: "vpce-svc-1", Kind: "endpoint_service", Ownership: model.Created, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders", "connection_id": "connection-1"}}
		if _, e := p.ConsumerPresent(context.Background(), c, service, "vpce-1"); e == nil {
			t.Fatal("untrusted consumer evidence accepted", response)
		}
	}
}
func TestDeleteServiceChecksConsumersOnLaterPagesBeforeMutation(t *testing.T) {
	pages := 0
	mutations := 0
	p, _ := testProvider(t, func(action string) string {
		switch action {
		case "GetCallerIdentity":
			return identityXML
		case "DescribeVpcEndpointServiceConfigurations":
			return ownedServiceXML
		case "DescribeVpcEndpointConnections":
			pages++
			if pages == 1 {
				return `<DescribeVpcEndpointConnectionsResponse><vpcEndpointConnectionSet><item><serviceId>vpce-svc-1</serviceId><vpcEndpointId>vpce-old</vpcEndpointId><vpcEndpointState>Deleted</vpcEndpointState></item></vpcEndpointConnectionSet><nextToken>page-two</nextToken></DescribeVpcEndpointConnectionsResponse>`
			}
			return `<DescribeVpcEndpointConnectionsResponse><vpcEndpointConnectionSet><item><serviceId>vpce-svc-1</serviceId><vpcEndpointId>vpce-other</vpcEndpointId><vpcEndpointState>Available</vpcEndpointState></item></vpcEndpointConnectionSet></DescribeVpcEndpointConnectionsResponse>`
		case "DeleteVpcEndpointServiceConfigurations":
			mutations++
			return `<DeleteVpcEndpointServiceConfigurationsResponse/>`
		}
		return ""
	})
	c := model.Config{Name: "orders", AWS: model.AWSConfig{AccountID: "123", Region: "us-east-1"}}
	r := model.Resource{Key: "aws.endpoint_service", ID: "vpce-svc-1", Kind: "endpoint_service", Ownership: model.Created, Properties: map[string]string{"account_id": "123", "region": "us-east-1", "connection": "orders", "connection_id": "connection-1"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := p.Delete(ctx, c, r); e == nil {
		t.Fatal("active consumer on second page ignored")
	}
	if pages != 2 {
		t.Fatal("consumer scan did not reach second page", pages)
	}
	if mutations != 0 {
		t.Fatal("delete called despite consumer", mutations)
	}
}
