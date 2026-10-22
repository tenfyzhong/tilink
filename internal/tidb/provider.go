package tidb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tenfyzhong/tilink/internal/model"
)

type Provider struct{ Client *Client }

func NewProvider(client *Client) *Provider { return &Provider{Client: client} }
func (p *Provider) Plan(ctx context.Context, c model.Config) ([]model.Step, error) {
	if p.Client == nil {
		return nil, errors.New("TiDB API client is required")
	}
	if c.Kind == "app" {
		step := model.Step{Key: "tidb.registration", Provider: "tidb", Kind: "endpoint_registration", Action: "create", DependsOn: []string{"aws.endpoint"}}
		if c.TiDB.ServiceName == "" {
			service, err := p.Client.GetEndpointService(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID)
			if err != nil {
				var api *APIError
				if !errors.As(err, &api) || !api.NotFound() {
					return nil, err
				}
			}
			if service.Name == "" {
				step.Manual = "Create the TiDB endpoint service in the console, then set tidb.service_name, tidb.host, tidb.port and tidb.az_ids. The documented API has no service deletion operation."
			}
		}
		return []model.Step{step}, nil
	}
	step := model.Step{Key: "tidb.private_link", Provider: "tidb", Kind: "private_link", Action: "create", DependsOn: []string{"aws.service_permission"}}
	if p.Client.dataflow == nil && c.TiDB.PrivateLinkID == "" {
		step.Manual = "Configure tidb.dataflow_base_url for the verified PrivateLink API, or create a connection in the console and set tidb.private_link_id, tidb.endpoint_id and tidb.service_name."
	}
	if p.Client.dataflow == nil && c.TiDB.PrivateLinkID != "" && (c.TiDB.EndpointID == "" || c.TiDB.ServiceName == "") {
		step.Manual = "Configure tidb.dataflow_base_url to verify the supplied private_link_id, cluster identity and endpoint service before reusing it."
	}
	steps := []model.Step{step}
	if c.Target.Type == "kafka" {
		domainStep := model.Step{Key: "tidb.domains", Provider: "tidb", Kind: "private_link_domains", Action: "create", DependsOn: []string{"tidb.private_link"}}
		if p.Client.dataflow == nil && c.TiDB.Host == "" {
			domainStep.Manual = "Set tidb.host to the managed hostname provided by the operator's PrivateLink domain attachment, then deploy the generated Kafka advertised listeners."
		}
		steps = append(steps, domainStep)
	}
	return steps, nil
}
func dependent(resources []model.Resource, key string) (model.Resource, error) {
	for _, r := range resources {
		if r.Key == key && r.Status != "deleted" {
			return r, nil
		}
	}
	return model.Resource{}, fmt.Errorf("TiDB prerequisite %s is missing", key)
}

// PrepareStep supplies a read-only registration snapshot for the durable operation journal.
func (p *Provider) PrepareStep(ctx context.Context, c model.Config, s model.Step, resources []model.Resource) (model.Step, error) {
	if s.Kind != "endpoint_registration" {
		return s, nil
	}
	if (s.Properties["prior_operation_status"] == "uncertain" || s.Properties["prior_operation_status"] == "pending") && s.Properties["registration_disposition"] != "" {
		return s, nil
	}
	if p.Client == nil {
		return s, errors.New("TiDB API client is required")
	}
	endpoint, err := dependent(resources, "aws.endpoint")
	if err != nil {
		return s, err
	}
	endpoints, err := p.Client.ListEndpoints(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID)
	if err != nil {
		return s, err
	}
	props := map[string]string{}
	for key, value := range s.Properties {
		props[key] = value
	}
	s.Properties = props
	delete(props, "registration_id")
	props["registration_disposition"] = "absent"
	props["aws_endpoint_id"] = endpoint.ID
	for _, item := range endpoints {
		if item.EndpointName != endpoint.ID {
			continue
		}
		if props["registration_id"] != "" {
			return s, errors.New("multiple TiDB registrations match the AWS endpoint")
		}
		if c.TiDB.EndpointID != "" && c.TiDB.EndpointID != item.ID {
			return s, errors.New("supplied TiDB endpoint registration ID does not match the AWS endpoint")
		}
		props["registration_disposition"] = "reused"
		props["registration_id"] = item.ID
	}
	return s, nil
}
func (p *Provider) Apply(ctx context.Context, c model.Config, s model.Step, resources []model.Resource) (model.Resource, error) {
	r := model.Resource{Key: s.Key, Provider: "tidb", Kind: s.Kind, Ownership: model.Created, DependsOn: s.DependsOn, OperationID: s.OperationID, Properties: map[string]string{"project_id": c.TiDB.ProjectID, "cluster_id": c.TiDB.ClusterID}, ObservedAt: time.Now().UTC()}
	if p.Client == nil {
		return r, errors.New("TiDB API client is required")
	}
	switch s.Kind {
	case "endpoint_registration":
		endpoint, err := dependent(resources, "aws.endpoint")
		if err != nil {
			return r, err
		}
		endpoints, err := p.Client.ListEndpoints(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID)
		if err != nil {
			return r, err
		}
		var registered Endpoint
		for _, item := range endpoints {
			if c.TiDB.EndpointID != "" && item.ID == c.TiDB.EndpointID && item.EndpointName != endpoint.ID {
				return r, errors.New("supplied TiDB endpoint registration belongs to another AWS endpoint")
			}
			if item.EndpointName == endpoint.ID {
				if registered.ID != "" {
					return r, errors.New("multiple endpoint registrations match")
				}
				registered = item
			}
		}
		if registered.ID != "" {
			provenPending := false
			for _, old := range resources {
				if old.Key == s.Key && old.OperationID == s.OperationID && old.Ownership == model.Created && old.Properties["registration_create_pending"] == "true" && old.Properties["aws_endpoint_id"] == endpoint.ID {
					provenPending = true
				}
			}
			checkpointReused := s.Properties["registration_disposition"] == "reused" && s.Properties["registration_id"] == registered.ID && s.Properties["aws_endpoint_id"] == endpoint.ID
			if s.Properties["registration_disposition"] == "absent" && !provenPending {
				r.Ownership = "unknown"
				return r, errors.New("registration appeared after the preparation snapshot; resolve its ownership before continuing")
			}
			if s.Properties["registration_disposition"] == "reused" && !checkpointReused {
				r.Ownership = "unknown"
				return r, errors.New("registration identity changed after the preparation snapshot")
			}
			if (s.Properties["prior_operation_status"] == "uncertain" || s.Properties["prior_operation_status"] == "pending") && !provenPending && !checkpointReused {
				r.Ownership = "unknown"
				return r, errors.New("registration creation outcome is uncertain; inspect the endpoint registration and resolve ownership before retrying")
			}
			if c.TiDB.EndpointID != "" && registered.ID != c.TiDB.EndpointID {
				return r, errors.New("supplied TiDB endpoint registration ID does not match the AWS endpoint")
			}
			r.Ownership = model.Reused
			for _, old := range resources {
				if old.Key == s.Key && (old.ID == registered.ID || old.Properties["registration_create_pending"] == "true" && old.Properties["aws_endpoint_id"] == endpoint.ID) && old.OperationID == s.OperationID && old.Ownership == model.Created {
					r.Ownership = model.Created
				}
			}
		} else {
			if c.TiDB.EndpointID != "" {
				return r, errors.New("supplied TiDB endpoint registration was not found")
			}
			if s.Properties["prior_operation_status"] == "uncertain" || s.Properties["prior_operation_status"] == "pending" {
				r.Ownership = "unknown"
				return r, errors.New("registration creation outcome is uncertain; confirm absence and resolve the prior operation before creating again")
			}
			registered, err = p.Client.CreateEndpoint(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID, endpoint.ID)
			if err != nil {
				var api *APIError
				if !errors.As(err, &api) || api.StatusCode >= 500 {
					r.ID = endpoint.ID
					r.Status = "pending"
					r.Properties["aws_endpoint_id"] = endpoint.ID
					r.Properties["registration_create_pending"] = "true"
				}
				return r, err
			}
		}
		r.ID = registered.ID
		r.Status = strings.ToLower(registered.Status)
		r.Properties["aws_endpoint_id"] = endpoint.ID
		r.Properties["service_name"] = registered.ServiceName
		if registered.ClusterID != "" && registered.ClusterID != c.TiDB.ClusterID {
			return r, errors.New("TiDB endpoint cluster identity conflicts")
		}
		if c.TiDB.ServiceName != "" && registered.ServiceName != "" && registered.ServiceName != c.TiDB.ServiceName {
			return r, errors.New("TiDB endpoint service identity conflicts")
		}
		if registered.Status != "ACTIVE" {
			return r, errors.New("TiDB endpoint registration is not active; run apply again after acceptance")
		}
		return r, nil
	case "private_link":
		service, err := dependent(resources, "aws.endpoint_service")
		if err != nil {
			return r, err
		}
		name := service.Properties["service_name"]
		if name == "" {
			return r, errors.New("AWS endpoint service name is missing")
		}
		if c.TiDB.ServiceName != "" && c.TiDB.ServiceName != name {
			return r, errors.New("configured TiDB service differs from the AWS endpoint service")
		}
		if p.Client.dataflow == nil && c.TiDB.PrivateLinkID != "" && c.TiDB.EndpointID != "" && c.TiDB.ServiceName == name {
			if err := ids(c.TiDB.PrivateLinkID, c.TiDB.EndpointID); err != nil {
				return r, err
			}
			r.ID = c.TiDB.PrivateLinkID
			r.Status = "ready"
			r.Ownership = model.Reused
			r.Properties["operator_supplied"] = "true"
			r.Properties["verification"] = "operator_supplied_unverified"
			r.Properties["aws_endpoint_id"] = c.TiDB.EndpointID
			r.Properties["service_name"] = name
			return r, nil
		}
		var link PrivateLink
		if c.TiDB.PrivateLinkID != "" {
			r.Ownership = model.Reused
			link, err = p.Client.GetPrivateLink(ctx, c.TiDB.ClusterID, c.TiDB.PrivateLinkID)
		} else {
			if s.OperationID == "" {
				return r, errors.New("private link creation requires a persisted operation ID")
			}
			link, err = p.Client.EnsurePrivateLink(ctx, c.TiDB.ClusterID, PrivateLink{ClusterID: c.TiDB.ClusterID, DisplayName: "tilink-" + s.OperationID, Type: "AWS_ENDPOINT_SERVICE", AWSEndpointService: &AWSEndpointService{Name: name, Region: c.AWS.Region}})
		}
		if err != nil {
			return r, err
		}
		r.ID = link.ID
		r.Status = strings.ToLower(link.State)
		r.Properties["service_name"] = name
		r.Properties["display_name"] = link.DisplayName
		if link.ClusterID != "" && link.ClusterID != c.TiDB.ClusterID || link.AWSEndpointService == nil || link.AWSEndpointService.Name != name {
			return r, errors.New("TiDB private link service or cluster identity conflicts")
		}
		r.Properties["aws_endpoint_id"] = link.AWSEndpointService.EndpointID
		if c.TiDB.EndpointID != "" && c.TiDB.EndpointID != link.AWSEndpointService.EndpointID {
			return r, errors.New("configured AWS endpoint identity conflicts")
		}
		if link.State != "ACTIVE" && link.State != "PENDING_ACCEPTANCE" || link.AWSEndpointService.EndpointID == "" {
			return r, errors.New("TiDB private link is not ready for AWS acceptance; run apply again")
		}
		return r, nil
	case "private_link_domains":
		parent, err := dependent(resources, "tidb.private_link")
		if err != nil {
			return r, err
		}
		if p.Client.dataflow == nil && c.TiDB.Host != "" && parent.Properties["operator_supplied"] == "true" {
			r.ID = c.TiDB.Host
			r.Status = "ready"
			r.Ownership = model.Reused
			r.Properties["private_link_id"] = parent.ID
			r.Properties["operator_supplied"] = "true"
			r.Properties["verification"] = "operator_supplied_unverified"
			names, _ := json.Marshal([]string{c.TiDB.Host})
			r.Properties["domains"] = string(names)
			return r, nil
		}
		if s.OperationID == "" {
			return r, errors.New("domain creation requires a persisted operation ID")
		}
		unique := "tilink-" + s.OperationID
		link, err := p.Client.GetPrivateLink(ctx, c.TiDB.ClusterID, parent.ID)
		if err != nil {
			return r, err
		}
		var domain AttachedDomain
		for _, item := range link.AttachedDomains {
			if item.UniqueName == unique && item.Type == "TIDBCLOUD_MANAGED" {
				if domain.ID != "" {
					return r, errors.New("ambiguous private link domain recovery")
				}
				domain = item
			}
		}
		if domain.ID == "" {
			domain, err = p.Client.AttachDomains(ctx, c.TiDB.ClusterID, parent.ID, AttachedDomain{Type: "TIDBCLOUD_MANAGED", UniqueName: unique}, false)
			if err != nil {
				return r, err
			}
		}
		r.ID = domain.ID
		r.Properties["private_link_id"] = parent.ID
		r.Properties["unique_name"] = unique
		if err := updateDomainResource(&r, domain); err != nil {
			return r, err
		}
		return r, nil
	default:
		return r, errors.New("unknown TiDB resource kind")
	}
}
func (p *Provider) Refresh(ctx context.Context, c model.Config, r model.Resource) (model.Resource, error) {
	if p.Client == nil {
		return r, errors.New("TiDB API client is required")
	}
	if r.Properties["cluster_id"] != c.TiDB.ClusterID || r.Properties["project_id"] != c.TiDB.ProjectID {
		return r, errors.New("TiDB resource belongs to another project or cluster")
	}
	r.ObservedAt = time.Now().UTC()
	if r.Properties["operator_supplied"] == "true" {
		if r.Ownership != model.Reused {
			return r, errors.New("operator supplied TiDB resource cannot be tool owned")
		}
		if r.Kind == "private_link" && (r.ID != c.TiDB.PrivateLinkID || r.Properties["aws_endpoint_id"] != c.TiDB.EndpointID || r.Properties["service_name"] != c.TiDB.ServiceName) {
			return r, errors.New("operator supplied TiDB identity changed")
		}
		if r.Kind == "private_link_domains" && r.ID != c.TiDB.Host {
			return r, errors.New("operator supplied TiDB domain changed")
		}
		r.Status = "ready"
		return r, nil
	}
	switch r.Kind {
	case "endpoint_registration":
		endpoints, err := p.Client.ListEndpoints(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID)
		if err != nil {
			return r, err
		}
		for _, endpoint := range endpoints {
			if r.Properties["registration_create_pending"] == "true" {
				if endpoint.EndpointName != r.Properties["aws_endpoint_id"] {
					continue
				}
				r.ID = endpoint.ID
			} else if endpoint.ID != r.ID {
				continue
			}
			if endpoint.EndpointName != r.Properties["aws_endpoint_id"] {
				return r, errors.New("TiDB registration ownership identity changed")
			}
			if endpoint.ClusterID != "" && endpoint.ClusterID != c.TiDB.ClusterID {
				return r, errors.New("TiDB registration cluster identity changed")
			}
			r.Status = strings.ToLower(endpoint.Status)
			return r, nil
		}
		r.Status = "deleted"
		return r, nil
	case "private_link":
		link, err := p.Client.GetPrivateLink(ctx, c.TiDB.ClusterID, r.ID)
		if err != nil {
			var api *APIError
			if errors.As(err, &api) && api.NotFound() {
				r.Status = "deleted"
				return r, nil
			}
			return r, err
		}
		if link.ClusterID != "" && link.ClusterID != c.TiDB.ClusterID || link.AWSEndpointService == nil || link.AWSEndpointService.Name != r.Properties["service_name"] {
			return r, errors.New("TiDB private link ownership identity changed")
		}
		if r.Ownership == model.Created && (link.DisplayName != r.Properties["display_name"] || link.DisplayName != "tilink-"+r.OperationID) {
			return r, errors.New("TiDB private link operation ownership changed")
		}
		if r.Properties["aws_endpoint_id"] != "" && link.AWSEndpointService.EndpointID != "" && r.Properties["aws_endpoint_id"] != link.AWSEndpointService.EndpointID {
			return r, errors.New("TiDB private link consumer endpoint changed")
		}
		r.Status = strings.ToLower(link.State)
		if r.Properties == nil {
			r.Properties = map[string]string{}
		}
		r.Properties["aws_endpoint_id"] = link.AWSEndpointService.EndpointID
		return r, nil
	case "private_link_domains":
		link, err := p.Client.GetPrivateLink(ctx, c.TiDB.ClusterID, r.Properties["private_link_id"])
		if err != nil {
			var api *APIError
			if errors.As(err, &api) && api.NotFound() {
				r.Status = "deleted"
				return r, nil
			}
			return r, err
		}
		for _, domain := range link.AttachedDomains {
			if domain.ID != r.ID {
				continue
			}
			if domain.UniqueName != r.Properties["unique_name"] || domain.UniqueName != "tilink-"+r.OperationID || domain.Type != "TIDBCLOUD_MANAGED" {
				return r, errors.New("private link domain ownership changed")
			}
			err = updateDomainResource(&r, domain)
			return r, err
		}
		r.Status = "deleted"
		return r, nil
	default:
		return r, errors.New("unknown TiDB resource kind")
	}
}
func updateDomainResource(r *model.Resource, domain AttachedDomain) error {
	if domain.ID == "" {
		return errors.New("domain attachment returned no ID")
	}
	names := []string{}
	ready := len(domain.Domains) > 0
	for _, item := range domain.Domains {
		names = append(names, item.Name)
		if item.State != "ACTIVE" {
			ready = false
		}
	}
	data, _ := json.Marshal(names)
	r.Properties["domains"] = string(data)
	r.Status = "creating"
	if ready {
		r.Status = "active"
		return nil
	}
	return errors.New("private link domains are not active; run apply again")
}
func (p *Provider) Delete(ctx context.Context, c model.Config, r model.Resource) error {
	if r.Ownership != model.Created || r.OperationID == "" {
		return errors.New("TiDB resource has no proven creation ownership")
	}
	fresh, err := p.Refresh(ctx, c, r)
	if err != nil {
		return err
	}
	if fresh.Status == "deleted" {
		return nil
	}
	switch r.Kind {
	case "endpoint_registration":
		err = p.Client.DeleteEndpoint(ctx, c.TiDB.ProjectID, c.TiDB.ClusterID, fresh.ID)
	case "private_link_domains":
		err = p.Client.DetachDomains(ctx, c.TiDB.ClusterID, r.Properties["private_link_id"], r.ID)
	case "private_link":
		err = p.Client.DeletePrivateLink(ctx, c.TiDB.ClusterID, r.ID)
	default:
		return errors.New("unknown TiDB resource kind")
	}
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 10; attempt++ {
		fresh, err = p.Refresh(ctx, c, r)
		if err != nil {
			return err
		}
		if strings.EqualFold(fresh.Status, "deleted") {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("TiDB resource deletion remains in progress; retry destroy after cloud cleanup completes")
}
