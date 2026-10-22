package aws

import (
	"context"
	"fmt"
	sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/tenfyzhong/tilink/internal/model"
	"strings"
)

func (p *Provider) discoverTarget(ctx context.Context, c model.Config) error {
	if c.Kind == "app" {
		return nil
	}
	switch c.Target.Type {
	case "rds":
		pager := rds.NewDescribeDBInstancesPaginator(p.RDS, &rds.DescribeDBInstancesInput{})
		for pager.HasMorePages() {
			page, e := pager.NextPage(ctx)
			if e != nil {
				return fmt.Errorf("discover RDS instances failed")
			}
			for _, db := range page.DBInstances {
				if db.Endpoint == nil || sdk.ToString(db.Endpoint.Address) != c.Target.Host {
					continue
				}
				if db.DBSubnetGroup == nil || sdk.ToString(db.DBSubnetGroup.VpcId) != c.AWS.VPCID {
					return fmt.Errorf("RDS source is in another VPC")
				}
				if sdk.ToInt32(db.Endpoint.Port) != int32(c.Target.Port) {
					return fmt.Errorf("RDS endpoint port conflicts with configuration")
				}
				for _, sg := range db.VpcSecurityGroups {
					if sdk.ToString(sg.VpcSecurityGroupId) == c.Target.SecurityGroupID {
						return nil
					}
				}
				return fmt.Errorf("RDS target security group is missing or not attached")
			}
		}
		return fmt.Errorf("configured RDS endpoint not found in this account and region")
	case "aurora":
		pager := rds.NewDescribeDBClustersPaginator(p.RDS, &rds.DescribeDBClustersInput{})
		for pager.HasMorePages() {
			page, e := pager.NextPage(ctx)
			if e != nil {
				return fmt.Errorf("discover Aurora clusters failed")
			}
			for _, db := range page.DBClusters {
				if sdk.ToString(db.Endpoint) != c.Target.Host {
					continue
				}
				groups, e := p.RDS.DescribeDBSubnetGroups(ctx, &rds.DescribeDBSubnetGroupsInput{DBSubnetGroupName: db.DBSubnetGroup})
				if e != nil || len(groups.DBSubnetGroups) != 1 || sdk.ToString(groups.DBSubnetGroups[0].VpcId) != c.AWS.VPCID {
					return fmt.Errorf("Aurora writer is in another VPC or subnet group is unavailable")
				}
				if sdk.ToInt32(db.Port) != int32(c.Target.Port) {
					return fmt.Errorf("Aurora writer port conflicts")
				}
				for _, sg := range db.VpcSecurityGroups {
					if sdk.ToString(sg.VpcSecurityGroupId) == c.Target.SecurityGroupID {
						return nil
					}
				}
				return fmt.Errorf("Aurora target security group is missing or not attached")
			}
		}
		return fmt.Errorf("configured Aurora writer endpoint not found")
	case "msk":
		if !strings.HasPrefix(c.Target.MSKClusterARN, "arn:aws:kafka:"+c.AWS.Region+":"+c.AWS.AccountID+":cluster/") {
			return fmt.Errorf("MSK ARN must match configured account and region")
		}
		out, e := p.Kafka.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{ClusterArn: sdk.String(c.Target.MSKClusterARN)})
		if e != nil {
			return fmt.Errorf("describe MSK cluster failed")
		}
		if out.ClusterInfo == nil || out.ClusterInfo.Provisioned == nil {
			return fmt.Errorf("only MSK Provisioned is supported")
		}
		db := out.ClusterInfo.Provisioned
		if db.ClientAuthentication == nil || db.ClientAuthentication.Sasl == nil || db.ClientAuthentication.Sasl.Scram == nil || !sdk.ToBool(db.ClientAuthentication.Sasl.Scram.Enabled) {
			return fmt.Errorf("MSK SASL/SCRAM must already be enabled")
		}
		if db.BrokerNodeGroupInfo == nil || db.BrokerNodeGroupInfo.ConnectivityInfo == nil || db.BrokerNodeGroupInfo.ConnectivityInfo.VpcConnectivity == nil || db.BrokerNodeGroupInfo.ConnectivityInfo.VpcConnectivity.ClientAuthentication == nil || db.BrokerNodeGroupInfo.ConnectivityInfo.VpcConnectivity.ClientAuthentication.Sasl == nil || db.BrokerNodeGroupInfo.ConnectivityInfo.VpcConnectivity.ClientAuthentication.Sasl.Scram == nil || !sdk.ToBool(db.BrokerNodeGroupInfo.ConnectivityInfo.VpcConnectivity.ClientAuthentication.Sasl.Scram.Enabled) {
			return fmt.Errorf("MSK multi-VPC SASL/SCRAM must already be enabled")
		}
	}
	return nil
}
