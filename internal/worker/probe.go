package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"io"
	"log"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type ProbeRequest struct {
	Protocol      string `json:"protocol"`
	Host          string `json:"host"`
	Port          string `json:"port"`
	TLS           bool   `json:"tls,omitempty"`
	ServerName    string `json:"server_name,omitempty"`
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	SASLMechanism string `json:"sasl_mechanism,omitempty"`
	Database      string `json:"database,omitempty"`
	CAPEM         []byte `json:"ca_pem,omitempty"`
}
type Protocols interface {
	MySQL(context.Context, ProbeRequest) error
	Kafka(context.Context, ProbeRequest) ([]string, error)
}
type Probe struct {
	DialContext socketDial
	DNS         DNS
	Protocols   Protocols
}

func (p Probe) Run(ctx context.Context, req ProbeRequest) Result {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	out := Result{Status: "partial", Vantage: "aws_vpc_probe", UpdatedAt: time.Now().UTC()}
	add := func(name, status, detail string) {
		out.Checks = append(out.Checks, Check{name, status, detail})
		if status == "fail" {
			out.Status = "fail"
		}
	}
	port, e := strconv.Atoi(req.Port)
	if req.Host == "" || e != nil || port < 1 || port > 65535 || (req.Protocol != "tcp" && req.Protocol != "mysql" && req.Protocol != "kafka") {
		add("configuration", "fail", "Invalid protocol, host, or port")
		return out
	}
	if p.DNS == nil {
		p.DNS = net.DefaultResolver
	}
	ips, e := p.DNS.LookupHost(ctx, req.Host)
	if e != nil || len(ips) == 0 {
		add("dns", "fail", "DNS resolution failed")
		return out
	}
	for _, ip := range ips {
		address, err := netip.ParseAddr(ip)
		if err != nil || !address.IsPrivate() {
			add("dns", "fail", "DNS must resolve exclusively to private addresses")
			return out
		}
	}
	add("dns", "pass", "Hostname resolves from the probe VPC")
	address := net.JoinHostPort(req.Host, req.Port)
	if e = dialProbe(ctx, address, req, req.Protocol == "tcp" && req.TLS, privateDialer{DNS: p.DNS, Dial: p.DialContext}); e != nil {
		add("tcp", "fail", "TCP or TLS connection failed")
		return out
	}
	add("tcp", "pass", "Connection established from the probe VPC")
	if req.Protocol == "tcp" {
		if req.TLS {
			add("tls", "pass", "Server certificate and identity verified")
		}
		return out
	}
	if p.Protocols == nil {
		p.Protocols = NetworkProtocols{DNS: p.DNS, DialContext: p.DialContext}
	}
	if req.Protocol == "mysql" {
		if req.Username == "" {
			add("mysql", "partial", "Credentials required for SELECT 1")
			return out
		}
		if e = p.Protocols.MySQL(ctx, req); e != nil {
			add("mysql", "fail", "MySQL authentication, TLS, or SELECT 1 failed")
			return out
		}
		add("mysql", "pass", "SELECT 1 succeeded")
		out.Status = "pass"
		return out
	}
	brokers, e := p.Protocols.Kafka(ctx, req)
	if e != nil || len(brokers) == 0 {
		add("kafka_metadata", "fail", "Kafka metadata or authentication failed")
		return out
	}
	add("kafka_metadata", "pass", "Broker metadata received")
	for _, broker := range brokers {
		host, port, e := net.SplitHostPort(broker)
		if e != nil {
			add("broker", "fail", "Invalid broker address in metadata")
			continue
		}
		n, pe := strconv.Atoi(port)
		ips, de := p.DNS.LookupHost(ctx, host)
		valid := pe == nil && n > 0 && n <= 65535 && de == nil && len(ips) > 0
		for _, ip := range ips {
			a, err := netip.ParseAddr(ip)
			if err != nil || !a.IsPrivate() {
				valid = false
			}
		}
		if !valid {
			add("broker", "fail", "Broker metadata must resolve exclusively to private addresses")
			continue
		}
		b := req
		b.Host = host
		b.Port = port
		b.ServerName = host
		one, e := p.Protocols.Kafka(ctx, b)
		if e != nil || len(one) == 0 {
			add("broker:"+broker, "fail", "Broker protocol check failed")
		} else {
			add("broker:"+broker, "pass", "Broker protocol reachable")
		}
	}
	if out.Status != "fail" {
		out.Status = "pass"
	}
	return out
}
func dialProbe(ctx context.Context, address string, req ProbeRequest, useTLS bool, d privateDialer) error {
	c, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer c.Close()
	if useTLS {
		cfg, e := tlsConfig(req)
		if e != nil {
			return e
		}
		secure := tls.Client(c, cfg)
		return secure.HandshakeContext(ctx)
	}
	return nil
}

func tlsConfig(req ProbeRequest) (*tls.Config, error) {
	name := req.ServerName
	if name == "" {
		name = req.Host
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: name}
	if len(req.CAPEM) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(req.CAPEM) {
			return nil, errors.New("invalid CA certificate")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

type NetworkProtocols struct {
	DNS         DNS
	DialContext socketDial
}

func (p NetworkProtocols) MySQL(ctx context.Context, req ProbeRequest) error {
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.DialFunc = (privateDialer{DNS: p.DNS, Dial: p.DialContext}).DialContext
	cfg.Addr = net.JoinHostPort(req.Host, req.Port)
	cfg.User = req.Username
	cfg.Passwd = req.Password
	cfg.DBName = req.Database
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	cfg.Logger = log.New(io.Discard, "", 0)
	if req.TLS {
		var e error
		cfg.TLS, e = tlsConfig(req)
		if e != nil {
			return errors.New("invalid MySQL TLS settings")
		}
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return errors.New("invalid MySQL settings")
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	var one int
	if err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		return errors.New("MySQL probe failed")
	}
	return nil
}
func (p NetworkProtocols) Kafka(ctx context.Context, req ProbeRequest) ([]string, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(net.JoinHostPort(req.Host, req.Port)), kgo.DialTimeout(5 * time.Second), kgo.RequestTimeoutOverhead(5 * time.Second), kgo.RequestRetries(0), kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		c, e := (privateDialer{DNS: p.DNS, Dial: p.DialContext}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		if !req.TLS {
			return c, nil
		}
		cfg, e := tlsConfig(req)
		if e != nil {
			c.Close()
			return nil, e
		}
		host, _, _ := net.SplitHostPort(address)
		if host != req.Host {
			cfg.ServerName = host
		}
		secure := tls.Client(c, cfg)
		if e = secure.HandshakeContext(ctx); e != nil {
			c.Close()
			return nil, errors.New("Kafka TLS handshake failed")
		}
		return secure, nil
	})}
	if req.TLS {
		cfg, e := tlsConfig(req)
		if e != nil {
			return nil, errors.New("invalid Kafka TLS settings")
		}
		_ = cfg
	}
	switch req.SASLMechanism {
	case "":
		if req.Username != "" || req.Password != "" {
			return nil, errors.New("SASL mechanism required")
		}
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: req.Username, Pass: req.Password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: req.Username, Pass: req.Password}.AsSha512Mechanism()))
	case "PLAIN":
		if !req.TLS {
			return nil, errors.New("TLS required for PLAIN")
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: req.Username, Pass: req.Password}.AsMechanism()))
	default:
		return nil, errors.New("unsupported SASL mechanism")
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.New("invalid Kafka settings")
	}
	defer client.Close()
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{}
	response, err := client.Request(ctx, request)
	if err != nil {
		return nil, errors.New("Kafka metadata failed")
	}
	metadata, ok := response.(*kmsg.MetadataResponse)
	if !ok {
		return nil, errors.New("invalid Kafka response")
	}
	out := make([]string, 0, len(metadata.Brokers))
	for _, broker := range metadata.Brokers {
		out = append(out, net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port))))
	}
	return out, nil
}
