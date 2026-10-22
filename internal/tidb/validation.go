package tidb

import (
	"context"
	"errors"
	"net/http"
)

// Credentials in validation requests are ephemeral and must never be persisted.
type MigrationTarget struct {
	User     string `json:"user"`
	Password string `json:"password"`
}
type ConnectionSecurity struct {
	AllowedCNs  []string `json:"certAllowedCn,omitempty"`
	CA          []byte   `json:"sslCaContent,omitempty"`
	Certificate []byte   `json:"sslCertContent,omitempty"`
	Key         []byte   `json:"sslKeyContent,omitempty"`
}
type ConnectionProfile struct {
	ConnType   string              `json:"connType"`
	EndpointID string              `json:"endpointId,omitempty"`
	Host       string              `json:"host,omitempty"`
	Port       int                 `json:"port"`
	User       string              `json:"user"`
	Password   string              `json:"password"`
	Security   *ConnectionSecurity `json:"security,omitempty"`
}
type MigrationSource struct {
	ConnProfile    ConnectionProfile `json:"connProfile"`
	SourceType     string            `json:"sourceType"`
	BinlogName     string            `json:"binlogName,omitempty"`
	BinlogPosition int               `json:"binlogPosition,omitempty"`
	BinlogGTID     string            `json:"binlogGtid,omitempty"`
}
type MigrationPrecheckRequest struct {
	DisplayName string            `json:"displayName"`
	Sources     []MigrationSource `json:"sources"`
	Target      MigrationTarget   `json:"target"`
	Mode        string            `json:"mode"`
	ShardMode   string            `json:"shardMode,omitempty"`
}
type PrecheckItem struct {
	Description    string `json:"description"`
	Status         string `json:"status"`
	Solution       string `json:"solution"`
	Reason         string `json:"reason"`
	SolutionDocURL string `json:"solutionDocUrl"`
	Type           string `json:"type"`
}
type MigrationPrecheck struct {
	ID           string         `json:"precheckId"`
	Total        int            `json:"total"`
	FailedCount  int            `json:"failedCnt"`
	WarningCount int            `json:"warnCnt"`
	SuccessCount int            `json:"successCnt"`
	Status       string         `json:"status"`
	Items        []PrecheckItem `json:"items"`
}

func precheckPath(cluster string) string { return "/clusters/" + cluster + "/migrationPrechecks" }
func (c *Client) CreateMigrationPrecheck(ctx context.Context, cluster string, request MigrationPrecheckRequest) (string, error) {
	if err := ids(cluster); err != nil {
		return "", err
	}
	if request.DisplayName == "" || len(request.Sources) == 0 || request.Mode == "" {
		return "", errors.New("incomplete migration precheck request")
	}
	var out struct {
		ID string `json:"precheckId"`
	}
	err := c.request(ctx, c.dataflow, http.MethodPost, precheckPath(cluster), nil, request, &out)
	if err == nil && out.ID == "" {
		err = errors.New("TiDB precheck returned no ID")
	}
	return out.ID, err
}
func (c *Client) GetMigrationPrecheck(ctx context.Context, cluster, id string) (MigrationPrecheck, error) {
	var out MigrationPrecheck
	if err := ids(cluster, id); err != nil {
		return out, err
	}
	err := c.request(ctx, c.dataflow, http.MethodGet, precheckPath(cluster)+"/"+id, nil, nil, &out)
	return out, err
}

// CancelMigrationPrecheck stops a precheck; its audit record may remain remotely.
func (c *Client) CancelMigrationPrecheck(ctx context.Context, cluster, id string) error {
	if err := ids(cluster, id); err != nil {
		return err
	}
	err := c.request(ctx, c.dataflow, http.MethodDelete, precheckPath(cluster)+"/"+id, nil, nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.NotFound() {
		return nil
	}
	return err
}

type SinkPrivateLink struct {
	ID   string `json:"privateLinkConnectionId"`
	Port int    `json:"port"`
}
type SinkNetwork struct {
	NetworkType    string           `json:"networkType"`
	PrivateLink    *SinkPrivateLink `json:"privateLink,omitempty"`
	PublicEndpoint string           `json:"publicEndpoint,omitempty"`
}
type MySQLAuthentication struct {
	Username  string `json:"userName"`
	Password  string `json:"password"`
	EnableTLS bool   `json:"enableTls"`
}
type MySQLSink struct {
	Network        SinkNetwork         `json:"network"`
	Authentication MySQLAuthentication `json:"authentication"`
}
type KafkaNetwork struct {
	NetworkType     string           `json:"networkType"`
	PrivateLink     *SinkPrivateLink `json:"privateLink,omitempty"`
	PublicEndpoints string           `json:"publicEndpoints,omitempty"`
}
type KafkaAuthentication struct {
	AuthType   string `json:"authType"`
	Username   string `json:"username,omitempty"`
	Password   string `json:"password,omitempty"`
	EnableTLS  bool   `json:"enableTls"`
	ServerName string `json:"serverName,omitempty"`
}
type KafkaBroker struct {
	KafkaVersion string `json:"kafkaVersion,omitempty"`
	Compression  string `json:"compression,omitempty"`
}
type KafkaSink struct {
	Network        KafkaNetwork        `json:"network"`
	Authentication KafkaAuthentication `json:"authentication"`
	Broker         *KafkaBroker        `json:"broker,omitempty"`
}
type Sink struct {
	Type  string     `json:"type"`
	MySQL *MySQLSink `json:"mysql,omitempty"`
	Kafka *KafkaSink `json:"kafka,omitempty"`
}

func (c *Client) TestSink(ctx context.Context, cluster string, sink Sink) error {
	if err := ids(cluster); err != nil {
		return err
	}
	if sink.Type == "MYSQL" {
		if sink.MySQL == nil || sink.Kafka != nil {
			return errors.New("invalid MySQL sink")
		}
	} else if sink.Type == "KAFKA" {
		if sink.Kafka == nil || sink.MySQL != nil {
			return errors.New("invalid Kafka sink")
		}
	} else {
		return errors.New("invalid sink type")
	}
	return c.request(ctx, c.dataflow, http.MethodPost, "/clusters/"+cluster+"/changefeeds:test", nil, map[string]any{"sink": sink}, nil)
}
