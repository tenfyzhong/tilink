package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func kafkaFixture(t *testing.T, metadataError int16) (string, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	p, _ := strconv.Atoi(port)
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				c.SetDeadline(time.Now().Add(3 * time.Second))
				for {
					var size [4]byte
					if _, e := io.ReadFull(c, size[:]); e != nil {
						return
					}
					n := binary.BigEndian.Uint32(size[:])
					if n > 1024*1024 {
						return
					}
					request := make([]byte, n)
					if _, e := io.ReadFull(c, request); e != nil || len(request) < 8 {
						return
					}
					key := int16(binary.BigEndian.Uint16(request))
					version := int16(binary.BigEndian.Uint16(request[2:]))
					response := append([]byte(nil), request[4:8]...)
					switch key {
					case 18:
						r := kmsg.NewPtrApiVersionsResponse()
						r.Version = version
						r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, {ApiKey: 3, MinVersion: 0, MaxVersion: 13}}
						response = r.AppendTo(response)
					case 3:
						r := kmsg.NewPtrMetadataResponse()
						r.Version = version
						r.ErrorCode = metadataError
						if version >= 9 {
							response = append(response, 0)
						}
						r.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0, Host: host, Port: int32(p)}}
						response = r.AppendTo(response)
					default:
						return
					}
					binary.BigEndian.PutUint32(size[:], uint32(len(response)))
					if _, e := c.Write(append(size[:], response...)); e != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return host, port
}
func TestNetworkKafkaMetadataFixture(t *testing.T) {
	host, port := kafkaFixture(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	brokers, e := (NetworkProtocols{DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, DialContext: localFixtureDial}).Kafka(ctx, ProbeRequest{Protocol: "kafka", Host: host, Port: port})
	if e != nil || len(brokers) != 1 || brokers[0] != net.JoinHostPort(host, port) {
		t.Fatalf("%v %v", brokers, e)
	}
}
func TestNetworkMySQLCancellationIsSanitized(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	go func() {
		c, e := listener.Accept()
		if e == nil {
			defer c.Close()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	e = (NetworkProtocols{DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, DialContext: localFixtureDial}).MySQL(ctx, ProbeRequest{Host: host, Port: port, Username: "user", Password: "secret"})
	if e == nil || e.Error() != "MySQL probe failed" {
		t.Fatal(e)
	}
}
func TestReconcilerCancellationPreservesActiveForwarding(t *testing.T) {
	f := &fakeLB{active: "a", groups: map[string][]Target{"a": {{"10.0.0.1", 3306}}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := Reconciler{DNS: &fakeDNS{values: [][]string{{"10.0.0.2"}}}, LB: f, PollInterval: time.Millisecond}
	out := r.Run(ctx, []Binding{{Hostname: "db", Port: 3306, HealthListenerARN: "h", ListenerARN: "l", TargetGroupARNs: [2]string{"a", "b"}}})
	if out.Status != "fail" || f.active != "a" || len(f.calls) != 0 {
		t.Fatal(out)
	}
}

func mysqlFixture(t *testing.T, expectedDatabase ...string) (string, string) {
	t.Helper()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				c.SetDeadline(time.Now().Add(2 * time.Second))
				write := func(seq byte, data []byte) bool {
					header := []byte{byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), seq}
					_, e := c.Write(append(header, data...))
					return e == nil
				}
				read := func() []byte {
					var h [4]byte
					if _, e := io.ReadFull(c, h[:]); e != nil {
						return nil
					}
					n := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
					if n > 65536 {
						return nil
					}
					v := make([]byte, n)
					if _, e := io.ReadFull(c, v); e != nil {
						return nil
					}
					return v
				}
				capabilities := uint32(1 | 8 | 0x200 | 0x8000 | 0x80000)
				handshake := append([]byte{10}, []byte("8.0.36\x00")...)
				handshake = append(handshake, 1, 0, 0, 0)
				handshake = append(handshake, []byte("12345678")...)
				handshake = append(handshake, 0, byte(capabilities), byte(capabilities>>8), 45, 2, 0, byte(capabilities>>16), byte(capabilities>>24), 21)
				handshake = append(handshake, make([]byte, 10)...)
				handshake = append(handshake, []byte("abcdefghijkl\x00mysql_native_password\x00")...)
				if !write(0, handshake) {
					return
				}
				auth := read()
				if auth == nil {
					return
				}
				if len(expectedDatabase) > 0 && (len(auth) < 4 || binary.LittleEndian.Uint32(auth[:4])&8 == 0 || !bytes.Contains(auth, []byte(expectedDatabase[0]+"\x00"))) {
					return
				}
				if !write(2, []byte{0, 0, 0, 2, 0, 0, 0}) {
					return
				}
				for {
					query := read()
					if len(query) == 0 || query[0] == 1 {
						return
					}
					if query[0] != 3 {
						return
					}
					if string(query[1:]) != "SELECT 1" {
						if !write(1, []byte{0, 0, 0, 2, 0, 0, 0}) {
							return
						}
						continue
					}
					column := []byte{3, 'd', 'e', 'f', 0, 0, 0, 1, '1', 0, 12, 45, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0}
					if !write(1, []byte{1}) || !write(2, column) || !write(3, []byte{254, 0, 0, 2, 0}) || !write(4, []byte{1, '1'}) || !write(5, []byte{254, 0, 0, 2, 0}) {
						return
					}
				}
			}(c)
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	return host, port
}
func TestNetworkMySQLSelectFixture(t *testing.T) {
	host, port := mysqlFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := (NetworkProtocols{DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, DialContext: localFixtureDial}).MySQL(ctx, ProbeRequest{Host: host, Port: port, Username: "fixture", Password: "secret"}); e != nil {
		t.Fatal(e)
	}
}

func TestNetworkKafkaRejectsMetadataError(t *testing.T) {
	host, port := kafkaFixture(t, 42)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, e := (NetworkProtocols{DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, DialContext: localFixtureDial}).Kafka(ctx, ProbeRequest{Host: host, Port: port}); e == nil {
		t.Fatal("metadata error reported as success")
	}
}

func TestTLSConfigRejectsInvalidCA(t *testing.T) {
	if _, e := tlsConfig(ProbeRequest{Host: "db", TLS: true, CAPEM: []byte("invalid")}); e == nil {
		t.Fatal("invalid certificate accepted")
	}
}
func TestTLSConfigVerifiesIdentityAndCustomCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	cfg, e := tlsConfig(ProbeRequest{Host: "db.internal", ServerName: "db.internal", TLS: true, CAPEM: pemBytes})
	if e != nil || cfg.InsecureSkipVerify || cfg.MinVersion < tls.VersionTLS12 || cfg.ServerName != "db.internal" || cfg.RootCAs == nil {
		t.Fatal(cfg, e)
	}
}

func TestNetworkMySQLUsesRequestedDatabase(t *testing.T) {
	host, port := mysqlFixture(t, "fixture_db")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := (NetworkProtocols{DNS: &fakeDNS{values: [][]string{{"10.0.0.1"}}}, DialContext: localFixtureDial}).MySQL(ctx, ProbeRequest{Host: host, Port: port, Database: "fixture_db", Username: "fixture", Password: "secret"}); e != nil {
		t.Fatal(e)
	}
}

func localFixtureDial(ctx context.Context, network, address string) (net.Conn, error) {
	_, port, e := net.SplitHostPort(address)
	if e != nil {
		return nil, e
	}
	return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
}
