package aws

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/tenfyzhong/tilink/internal/model"
)

// ConsumerPresent checks one recorded consumer without changing its endpoint or service.
// A false result requires a successful owned-service lookup and an absent or terminal consumer.
func (p *Provider) ConsumerPresent(ctx context.Context, c model.Config, service model.Resource, endpointID string) (bool, error) {
	if service.Kind != "endpoint_service" || service.Ownership != model.Created || !metadataOwned(c, service) || service.Properties["connection_id"] == "" {
		return false, fmt.Errorf("consumer inspection requires the exact owned endpoint service")
	}
	if !strings.HasPrefix(endpointID, "vpce-") || strings.HasPrefix(endpointID, "vpce-svc-") {
		return false, fmt.Errorf("a recorded consumer VPC endpoint ID is required")
	}
	observed, err := p.Refresh(ctx, c, service)
	if err != nil {
		return false, err
	}
	if observed.Status == "deleted" {
		return false, nil
	}
	var token *string
	for {
		out, err := p.EC2.DescribeVpcEndpointConnections(ctx, &ec2.DescribeVpcEndpointConnectionsInput{Filters: []et.Filter{{Name: sdk.String("service-id"), Values: []string{service.ID}}, {Name: sdk.String("vpc-endpoint-id"), Values: []string{endpointID}}}, NextToken: token})
		if err != nil {
			return false, fmt.Errorf("inspect exact endpoint consumer failed")
		}
		for _, connection := range out.VpcEndpointConnections {
			if sdk.ToString(connection.ServiceId) != service.ID || sdk.ToString(connection.VpcEndpointId) != endpointID {
				return false, fmt.Errorf("endpoint consumer response did not match the recorded service and endpoint")
			}
			switch strings.ToLower(string(connection.VpcEndpointState)) {
			case "deleted", "rejected":
			default:
				return true, nil
			}
		}
		if sdk.ToString(out.NextToken) == "" {
			return false, nil
		}
		if token != nil && sdk.ToString(token) == sdk.ToString(out.NextToken) {
			return false, fmt.Errorf("endpoint consumer pagination did not advance")
		}
		token = out.NextToken
	}
}

func (p *Provider) activeServiceConsumers(ctx context.Context, serviceID string) (bool, error) {
	var token *string
	seen := map[string]bool{}
	for {
		out, err := p.EC2.DescribeVpcEndpointConnections(ctx, &ec2.DescribeVpcEndpointConnectionsInput{Filters: []et.Filter{{Name: sdk.String("service-id"), Values: []string{serviceID}}}, NextToken: token})
		if err != nil {
			return false, fmt.Errorf("inspect endpoint service consumers failed")
		}
		for _, connection := range out.VpcEndpointConnections {
			if sdk.ToString(connection.ServiceId) != serviceID {
				return false, fmt.Errorf("consumer inventory does not match owned endpoint service")
			}
			switch strings.ToLower(string(connection.VpcEndpointState)) {
			case "deleted", "rejected":
			default:
				return true, nil
			}
		}
		next := sdk.ToString(out.NextToken)
		if next == "" {
			return false, nil
		}
		if seen[next] {
			return false, fmt.Errorf("endpoint consumer pagination did not advance")
		}
		seen[next] = true
		token = out.NextToken
	}
}
