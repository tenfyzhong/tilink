package model

import (
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Version      int                `json:"version" yaml:"version"`
	Name         string             `json:"name" yaml:"name"`
	Kind         string             `json:"kind" yaml:"kind"`
	Provider     string             `json:"provider" yaml:"provider"`
	AWS          AWSConfig          `json:"aws" yaml:"aws"`
	TiDB         TiDBConfig         `json:"tidb" yaml:"tidb"`
	Target       TargetConfig       `json:"target" yaml:"target"`
	Verification VerificationConfig `json:"verification,omitempty" yaml:"verification,omitempty"`
	Maintenance  bool               `json:"maintenance" yaml:"maintenance"`
	Probe        bool               `json:"probe" yaml:"probe"`
}
type AWSConfig struct {
	Profile          string   `json:"profile,omitempty" yaml:"profile,omitempty"`
	Region           string   `json:"region" yaml:"region"`
	AccountID        string   `json:"account_id" yaml:"account_id"`
	VPCID            string   `json:"vpc_id" yaml:"vpc_id"`
	SubnetIDs        []string `json:"subnet_ids" yaml:"subnet_ids"`
	SecurityGroupIDs []string `json:"security_group_ids,omitempty" yaml:"security_group_ids,omitempty"`
	ClientCIDRs      []string `json:"client_cidrs,omitempty" yaml:"client_cidrs,omitempty"`
	ELBEndpointID    string   `json:"elb_endpoint_id,omitempty" yaml:"elb_endpoint_id,omitempty"`
}
type TiDBConfig struct {
	ProjectID       string   `json:"project_id" yaml:"project_id"`
	ClusterID       string   `json:"cluster_id" yaml:"cluster_id"`
	PublicKeyEnv    string   `json:"public_key_env,omitempty" yaml:"public_key_env,omitempty"`
	PrivateKeyEnv   string   `json:"private_key_env,omitempty" yaml:"private_key_env,omitempty"`
	BaseURL         string   `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	DataflowBaseURL string   `json:"dataflow_base_url,omitempty" yaml:"dataflow_base_url,omitempty"`
	PrincipalARN    string   `json:"principal_arn,omitempty" yaml:"principal_arn,omitempty"`
	AZIDs           []string `json:"az_ids,omitempty" yaml:"az_ids,omitempty"`
	ServiceName     string   `json:"service_name,omitempty" yaml:"service_name,omitempty"`
	PrivateLinkID   string   `json:"private_link_id,omitempty" yaml:"private_link_id,omitempty"`
	EndpointID      string   `json:"endpoint_id,omitempty" yaml:"endpoint_id,omitempty"`
	Host            string   `json:"host,omitempty" yaml:"host,omitempty"`
	Port            int      `json:"port,omitempty" yaml:"port,omitempty"`
}
type TargetConfig struct {
	AdvertisedListenersPublished bool     `json:"advertised_listeners_published,omitempty" yaml:"advertised_listeners_published,omitempty"`
	Type                         string   `json:"type" yaml:"type"`
	Host                         string   `json:"host,omitempty" yaml:"host,omitempty"`
	Port                         int      `json:"port,omitempty" yaml:"port,omitempty"`
	SecurityGroupID              string   `json:"security_group_id,omitempty" yaml:"security_group_id,omitempty"`
	MSKClusterARN                string   `json:"msk_cluster_arn,omitempty" yaml:"msk_cluster_arn,omitempty"`
	Brokers                      []Broker `json:"brokers,omitempty" yaml:"brokers,omitempty"`
}
type Broker struct {
	ID           string `json:"id" yaml:"id"`
	Host         string `json:"host" yaml:"host"`
	Port         int    `json:"port" yaml:"port"`
	ListenerPort int    `json:"listener_port" yaml:"listener_port"`
}
type VerificationConfig struct {
	TargetUsernameEnv string `json:"target_username_env,omitempty" yaml:"target_username_env,omitempty"`
	TargetPasswordEnv string `json:"target_password_env,omitempty" yaml:"target_password_env,omitempty"`
	SASLMechanism     string `json:"sasl_mechanism,omitempty" yaml:"sasl_mechanism,omitempty"`
	UsernameEnv       string `json:"username_env,omitempty" yaml:"username_env,omitempty"`
	PasswordEnv       string `json:"password_env,omitempty" yaml:"password_env,omitempty"`
	Database          string `json:"database,omitempty" yaml:"database,omitempty"`
	ServerName        string `json:"server_name,omitempty" yaml:"server_name,omitempty"`
	CAFile            string `json:"ca_file,omitempty" yaml:"ca_file,omitempty"`
}

const (
	Created  = "created"
	Reused   = "reused"
	Modified = "modified"
)

type Resource struct {
	Key         string            `json:"key"`
	ID          string            `json:"id"`
	Provider    string            `json:"provider"`
	Kind        string            `json:"kind"`
	Ownership   string            `json:"ownership"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Status      string            `json:"status"`
	OperationID string            `json:"operation_id"`
	Properties  map[string]string `json:"properties,omitempty"`
	ObservedAt  time.Time         `json:"observed_at"`
}
type Step struct {
	Key         string            `json:"key"`
	Provider    string            `json:"provider"`
	Kind        string            `json:"kind"`
	Action      string            `json:"action"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
	Manual      string            `json:"manual,omitempty"`
	OperationID string            `json:"operation_id,omitempty"`
}
type Plan struct {
	ConnectionID string    `json:"connection_id"`
	Steps        []Step    `json:"steps"`
	Findings     []Finding `json:"findings,omitempty"`
}
type Operation struct {
	ID     string `json:"id"`
	Step   Step   `json:"step"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
type GeneratedFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}
type State struct {
	Version        int             `json:"version"`
	ConnectionID   string          `json:"connection_id"`
	Revision       int64           `json:"revision"`
	Config         Config          `json:"config"`
	Status         string          `json:"status"`
	Resources      []Resource      `json:"resources"`
	Operations     []Operation     `json:"operations,omitempty"`
	GeneratedFiles []GeneratedFile `json:"generated_files,omitempty"`
	Findings       []Finding       `json:"findings,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at"`
}
type Finding struct {
	Code        string `json:"code"`
	Severity    string `json:"severity"`
	ResourceKey string `json:"resource_key,omitempty"`
	Message     string `json:"message"`
	NextStep    string `json:"next_step,omitempty"`
}
type Verification struct {
	Status    string    `json:"status"`
	Location  string    `json:"location"`
	CheckedAt time.Time `json:"checked_at"`
	Findings  []Finding `json:"findings,omitempty"`
	Uncovered []string  `json:"uncovered,omitempty"`
}

var validName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`)

func ValidName(name string) bool { return validName.MatchString(name) }
func DecodeConfig(r io.Reader) (Config, error) {
	var c Config
	d := yaml.NewDecoder(r)
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("decode configuration: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("configuration must contain one document")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	envPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	for _, name := range []string{c.TiDB.PublicKeyEnv, c.TiDB.PrivateKeyEnv, c.Verification.UsernameEnv, c.Verification.PasswordEnv, c.Verification.TargetUsernameEnv, c.Verification.TargetPasswordEnv} {
		if name != "" && !envPattern.MatchString(name) {
			return fmt.Errorf("credential references must be environment variable names")
		}
	}
	for _, endpoint := range []string{c.TiDB.BaseURL, c.TiDB.DataflowBaseURL} {
		if endpoint != "" {
			u, err := url.Parse(endpoint)
			if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return fmt.Errorf("API endpoint must be an HTTP URL without credentials, query or fragment")
			}
		}
	}
	if c.Version != 1 {
		return fmt.Errorf("unsupported configuration version %d", c.Version)
	}
	if !ValidName(c.Name) {
		return fmt.Errorf("invalid connection name")
	}
	if c.Kind != "app" && c.Kind != "dm" && c.Kind != "ticdc" {
		return fmt.Errorf("kind must be app, dm, or ticdc")
	}
	if c.Provider != "aws" {
		return fmt.Errorf("provider must be aws")
	}
	if c.AWS.Region == "" || c.AWS.AccountID == "" || c.AWS.VPCID == "" || len(c.AWS.SubnetIDs) == 0 {
		return fmt.Errorf("AWS region, account_id, vpc_id and subnet_ids are required")
	}
	if !regexp.MustCompile(`^[0-9]{12}$`).MatchString(c.AWS.AccountID) {
		return fmt.Errorf("AWS account_id must contain 12 digits")
	}
	for _, cidr := range c.AWS.ClientCIDRs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil || prefix.Bits() == 0 {
			return fmt.Errorf("client CIDRs must be explicit non-public ranges")
		}
	}
	if c.TiDB.Port < 0 || c.TiDB.Port > 65535 {
		return fmt.Errorf("invalid TiDB port")
	}
	if c.Kind == "dm" && (c.Target.Type == "kafka" || c.Target.Type == "msk") {
		return fmt.Errorf("DM requires a MySQL-compatible source")
	}
	ids, ports := map[string]bool{}, map[int]bool{}
	for _, b := range c.Target.Brokers {
		if b.ID == "" || b.Host == "" || b.Port < 1 || b.Port > 65535 || b.ListenerPort < 1 || b.ListenerPort > 65535 || ids[b.ID] || ports[b.ListenerPort] {
			return fmt.Errorf("brokers require unique IDs/listener ports and valid hosts/ports")
		}
		ids[b.ID] = true
		ports[b.ListenerPort] = true
	}
	if c.Target.Type == "kafka" && len(c.Target.Brokers) == 0 {
		return fmt.Errorf("Kafka requires every broker to be configured")
	}
	if c.TiDB.ProjectID == "" || c.TiDB.ClusterID == "" {
		return fmt.Errorf("TiDB project_id and cluster_id are required")
	}
	if c.Kind != "app" {
		if c.Target.Type != "mysql" && c.Target.Type != "rds" && c.Target.Type != "aurora" && c.Target.Type != "kafka" && c.Target.Type != "msk" {
			return fmt.Errorf("unsupported target type")
		}
		if c.Target.Type != "msk" && (c.Target.Host == "" || c.Target.Port < 1 || c.Target.Port > 65535) {
			return fmt.Errorf("target host and valid port are required")
		}
		if c.Target.Type == "msk" && c.Target.MSKClusterARN == "" {
			return fmt.Errorf("MSK cluster ARN is required")
		}
	}
	return nil
}
func CheckProperties(p map[string]string) error {
	for k := range p {
		s := strings.ToLower(k)
		if strings.Contains(s, "password") || strings.Contains(s, "secret") || strings.Contains(s, "private_key") || strings.Contains(s, "token") {
			return fmt.Errorf("sensitive property %q cannot be persisted", k)
		}
	}
	return nil
}
func CompatibleUpdate(old, next Config) bool {
	if old.Target.AdvertisedListenersPublished && !next.Target.AdvertisedListenersPublished {
		return false
	}
	old.Target.AdvertisedListenersPublished = next.Target.AdvertisedListenersPublished
	before, after := reflect.ValueOf(old.TiDB), reflect.ValueOf(next.TiDB)
	manual := map[string]bool{"ServiceName": true, "PrivateLinkID": true, "EndpointID": true, "Host": true, "Port": true, "PrincipalARN": true, "AZIDs": true}
	for i := 0; i < before.NumField(); i++ {
		if manual[before.Type().Field(i).Name] && !reflect.DeepEqual(before.Field(i).Interface(), after.Field(i).Interface()) && !before.Field(i).IsZero() {
			return false
		}
	}
	old.TiDB.ServiceName = next.TiDB.ServiceName
	old.TiDB.PrivateLinkID = next.TiDB.PrivateLinkID
	old.TiDB.EndpointID = next.TiDB.EndpointID
	old.TiDB.Host = next.TiDB.Host
	old.TiDB.Port = next.TiDB.Port
	old.TiDB.PrincipalARN = next.TiDB.PrincipalARN
	old.TiDB.AZIDs = next.TiDB.AZIDs
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(next)
	return string(a) == string(b)
}
