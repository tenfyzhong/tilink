package tidb

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

type EndpointService struct {
	Name          string   `json:"name"`
	DNSName       string   `json:"dns_name"`
	Port          int      `json:"port"`
	AZIDs         []string `json:"az_ids"`
	CloudProvider string   `json:"cloud_provider"`
	Status        string   `json:"status"`
}
type Endpoint struct {
	ID            string `json:"id"`
	EndpointName  string `json:"endpoint_name"`
	Status        string `json:"status"`
	ServiceName   string `json:"service_name"`
	CloudProvider string `json:"cloud_provider"`
	ClusterID     string `json:"cluster_id"`
}

func clusterPath(project, cluster string) string {
	return "/projects/" + project + "/clusters/" + cluster
}
func (c *Client) GetEndpointService(ctx context.Context, project, cluster string) (EndpointService, error) {
	var out struct {
		Service EndpointService `json:"private_endpoint_service"`
	}
	if err := ids(project, cluster); err != nil {
		return out.Service, err
	}
	err := c.request(ctx, c.base, http.MethodGet, clusterPath(project, cluster)+"/private_endpoint_service", nil, nil, &out)
	return out.Service, err
}
func (c *Client) ListEndpoints(ctx context.Context, project, cluster string) ([]Endpoint, error) {
	var out struct {
		Endpoints []Endpoint `json:"endpoints"`
	}
	if err := ids(project, cluster); err != nil {
		return nil, err
	}
	err := c.request(ctx, c.base, http.MethodGet, clusterPath(project, cluster)+"/private_endpoints", nil, nil, &out)
	return out.Endpoints, err
}

// RegisterEndpoint recovers a prior registration by AWS endpoint identity before creating.
func (c *Client) RegisterEndpoint(ctx context.Context, project, cluster, endpointName string) (Endpoint, error) {
	if err := ids(project, cluster, endpointName); err != nil {
		return Endpoint{}, err
	}
	endpoints, err := c.ListEndpoints(ctx, project, cluster)
	if err != nil {
		return Endpoint{}, err
	}
	var matches []Endpoint
	for _, endpoint := range endpoints {
		if endpoint.EndpointName == endpointName {
			matches = append(matches, endpoint)
		}
	}
	if len(matches) > 1 {
		return Endpoint{}, errors.New("multiple TiDB registrations match the AWS endpoint")
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return c.CreateEndpoint(ctx, project, cluster, endpointName)
}

// CreateEndpoint performs one create request. The caller owns discovery and recovery.
func (c *Client) CreateEndpoint(ctx context.Context, project, cluster, endpointName string) (Endpoint, error) {
	if err := ids(project, cluster, endpointName); err != nil {
		return Endpoint{}, err
	}
	var out struct {
		Endpoint Endpoint `json:"private_endpoint"`
	}
	err := c.request(ctx, c.base, http.MethodPost, clusterPath(project, cluster)+"/private_endpoints", nil, map[string]string{"endpoint_name": endpointName}, &out)
	if err == nil && out.Endpoint.ID == "" {
		err = errors.New("TiDB registration returned no internal endpoint ID")
	}
	return out.Endpoint, err
}
func (c *Client) DeleteEndpoint(ctx context.Context, project, cluster, internalID string) error {
	if err := ids(project, cluster, internalID); err != nil {
		return err
	}
	if strings.HasPrefix(internalID, "vpce-") {
		return errors.New("TiDB endpoint deletion requires the internal endpoint ID")
	}
	err := c.request(ctx, c.base, http.MethodDelete, clusterPath(project, cluster)+"/private_endpoints/"+internalID, nil, nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.NotFound() {
		return nil
	}
	return err
}

type AWSEndpointService struct {
	Name                string   `json:"name"`
	Region              string   `json:"region,omitempty"`
	EndpointID          string   `json:"endpointId,omitempty"`
	AvailabilityZoneIDs []string `json:"availabilityZoneIds,omitempty"`
}
type PrivateLink struct {
	ID                 string              `json:"privateLinkConnectionId,omitempty"`
	ClusterID          string              `json:"clusterId"`
	DisplayName        string              `json:"displayName"`
	State              string              `json:"state,omitempty"`
	Type               string              `json:"type"`
	AWSEndpointService *AWSEndpointService `json:"awsEndpointService,omitempty"`
	AttachedDomains    []AttachedDomain    `json:"attachedDomains,omitempty"`
}
type AvailabilityZoneInfo struct {
	AccountID string   `json:"accountId"`
	AZIDs     []string `json:"azIds"`
}
type Domain struct {
	Name  string `json:"name"`
	State string `json:"state"`
}
type AttachedDomain struct {
	ID         string   `json:"attachDomainId,omitempty"`
	Type       string   `json:"type"`
	UniqueName string   `json:"uniqueName,omitempty"`
	Domains    []Domain `json:"domains,omitempty"`
}

func dataflowPath(cluster string) string { return "/clusters/" + cluster + "/privateLinkConnections" }
func (c *Client) ListPrivateLinks(ctx context.Context, cluster string) ([]PrivateLink, error) {
	if err := ids(cluster); err != nil {
		return nil, err
	}
	var all []PrivateLink
	token := ""
	seen := map[string]bool{}
	for page := 0; page < 1000; page++ {
		query := url.Values{"pageSize": {"100"}}
		if token != "" {
			query.Set("pageToken", token)
		}
		var out struct {
			Links []PrivateLink `json:"privateLinkConnections"`
			Next  string        `json:"nextPageToken"`
		}
		if err := c.request(ctx, c.dataflow, http.MethodGet, dataflowPath(cluster), query, nil, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Links...)
		if out.Next == "" {
			return all, nil
		}
		if seen[out.Next] {
			return nil, errors.New("TiDB pagination token repeated")
		}
		seen[out.Next] = true
		token = out.Next
	}
	return nil, errors.New("TiDB pagination exceeded limit")
}
func (c *Client) GetPrivateLink(ctx context.Context, cluster, id string) (PrivateLink, error) {
	var out PrivateLink
	if err := ids(cluster, id); err != nil {
		return out, err
	}
	err := c.request(ctx, c.dataflow, http.MethodGet, dataflowPath(cluster)+"/"+id, nil, nil, &out)
	return out, err
}
func (c *Client) CreatePrivateLink(ctx context.Context, cluster string, link PrivateLink) (PrivateLink, error) {
	var out PrivateLink
	if err := ids(cluster); err != nil {
		return out, err
	}
	if link.ClusterID != cluster || link.DisplayName == "" || link.Type != "AWS_ENDPOINT_SERVICE" || link.AWSEndpointService == nil || link.AWSEndpointService.Name == "" {
		return out, errors.New("incomplete AWS private link request")
	}
	// Output-only fields are never replayed into a create request.
	payload := PrivateLink{ClusterID: cluster, DisplayName: link.DisplayName, Type: link.Type, AWSEndpointService: &AWSEndpointService{Name: link.AWSEndpointService.Name, Region: link.AWSEndpointService.Region}}
	err := c.request(ctx, c.dataflow, http.MethodPost, dataflowPath(cluster), nil, map[string]any{"privateLinkConnection": payload}, &out)
	if err == nil && out.ID == "" {
		err = errors.New("TiDB private link returned no ID")
	}
	return out, err
}

// EnsurePrivateLink requires a stable operation display name and rejects conflicting recovery matches.
func (c *Client) EnsurePrivateLink(ctx context.Context, cluster string, want PrivateLink) (PrivateLink, error) {
	links, err := c.ListPrivateLinks(ctx, cluster)
	if err != nil {
		return PrivateLink{}, err
	}
	var matches []PrivateLink
	for _, link := range links {
		if link.DisplayName != want.DisplayName {
			continue
		}
		if link.Type != want.Type || link.AWSEndpointService == nil || want.AWSEndpointService == nil || link.AWSEndpointService.Name != want.AWSEndpointService.Name {
			return PrivateLink{}, errors.New("TiDB private link recovery identity conflicts")
		}
		matches = append(matches, link)
	}
	if len(matches) > 1 {
		return PrivateLink{}, errors.New("ambiguous TiDB private link recovery")
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return c.CreatePrivateLink(ctx, cluster, want)
}
func (c *Client) DeletePrivateLink(ctx context.Context, cluster, id string) error {
	if err := ids(cluster, id); err != nil {
		return err
	}
	err := c.request(ctx, c.dataflow, http.MethodDelete, dataflowPath(cluster)+"/"+id, nil, nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.NotFound() {
		return nil
	}
	return err
}
func (c *Client) AvailabilityZones(ctx context.Context, cluster string) (AvailabilityZoneInfo, error) {
	var out AvailabilityZoneInfo
	if err := ids(cluster); err != nil {
		return out, err
	}
	err := c.request(ctx, c.dataflow, http.MethodGet, dataflowPath(cluster)+":getAvailabilityZones", nil, nil, &out)
	return out, err
}
func (c *Client) AttachDomains(ctx context.Context, cluster, id string, domain AttachedDomain, validateOnly bool) (AttachedDomain, error) {
	var out AttachedDomain
	if err := ids(cluster, id); err != nil {
		return out, err
	}
	if domain.Type != "TIDBCLOUD_MANAGED" && domain.Type != "CONFLUENT" {
		return out, errors.New("invalid private link domain type")
	}
	if domain.UniqueName == "" && (!validateOnly || domain.Type != "TIDBCLOUD_MANAGED") {
		return out, errors.New("private link domain unique name is required")
	}
	payload := AttachedDomain{Type: domain.Type, UniqueName: domain.UniqueName}
	err := c.request(ctx, c.dataflow, http.MethodPost, dataflowPath(cluster)+"/"+id+":attachDomains", nil, map[string]any{"attachDomain": payload, "validateOnly": validateOnly}, &out)
	return out, err
}
func (c *Client) DetachDomains(ctx context.Context, cluster, id, attachmentID string) error {
	if err := ids(cluster, id, attachmentID); err != nil {
		return err
	}
	return c.request(ctx, c.dataflow, http.MethodPost, dataflowPath(cluster)+"/"+id+":detachDomains", nil, map[string]string{"attachDomainId": attachmentID}, nil)
}
