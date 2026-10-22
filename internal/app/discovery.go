package app

import (
	"context"
	"fmt"
	"sort"

	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	cloudaws "github.com/tenfyzhong/tilink/internal/aws"
	"github.com/tenfyzhong/tilink/internal/cli"
	"github.com/tenfyzhong/tilink/internal/model"
)

// Discover inventories resources in the operator's selected AWS account and region.
func Discover(ctx context.Context, c model.AWSConfig) (cli.Inventory, error) {
	out := cli.Inventory{}
	p, err := cloudaws.New(ctx, model.Config{AWS: c})
	if err != nil {
		return out, err
	}
	identity, err := p.STS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return out, fmt.Errorf("AWS account discovery failed")
	}
	out.AccountID = sdk.ToString(identity.Account)
	var token *string
	for {
		page, err := p.EC2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{NextToken: token})
		if err != nil {
			return out, fmt.Errorf("AWS VPC discovery failed")
		}
		for _, v := range page.Vpcs {
			out.VPCIDs = append(out.VPCIDs, sdk.ToString(v.VpcId))
		}
		if page.NextToken == nil || sdk.ToString(page.NextToken) == "" {
			break
		}
		token = page.NextToken
	}
	token = nil
	for {
		page, err := p.EC2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{NextToken: token})
		if err != nil {
			return out, fmt.Errorf("AWS subnet discovery failed")
		}
		for _, v := range page.Subnets {
			out.SubnetIDs = append(out.SubnetIDs, sdk.ToString(v.SubnetId))
		}
		if page.NextToken == nil || sdk.ToString(page.NextToken) == "" {
			break
		}
		token = page.NextToken
	}
	instances := rds.NewDescribeDBInstancesPaginator(p.RDS, &rds.DescribeDBInstancesInput{})
	for instances.HasMorePages() {
		page, err := instances.NextPage(ctx)
		if err != nil {
			return out, fmt.Errorf("RDS endpoint discovery failed")
		}
		for _, db := range page.DBInstances {
			if db.Endpoint != nil && sdk.ToString(db.Endpoint.Address) != "" {
				out.DatabaseHosts = append(out.DatabaseHosts, sdk.ToString(db.Endpoint.Address))
			}
		}
	}
	clusters := rds.NewDescribeDBClustersPaginator(p.RDS, &rds.DescribeDBClustersInput{})
	for clusters.HasMorePages() {
		page, err := clusters.NextPage(ctx)
		if err != nil {
			return out, fmt.Errorf("Aurora endpoint discovery failed")
		}
		for _, db := range page.DBClusters {
			if sdk.ToString(db.Endpoint) != "" {
				out.DatabaseHosts = append(out.DatabaseHosts, sdk.ToString(db.Endpoint))
			}
		}
	}
	sort.Strings(out.VPCIDs)
	sort.Strings(out.SubnetIDs)
	sort.Strings(out.DatabaseHosts)
	return out, nil
}
