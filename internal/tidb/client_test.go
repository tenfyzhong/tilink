package tidb

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func clientFor(t *testing.T, s *httptest.Server) *Client {
	t.Helper()
	c, err := NewClient(Options{BaseURL: s.URL + "/api/v1beta", DataflowBaseURL: s.URL + "/v1beta1", PublicKey: "public", PrivateKey: "secret", HTTPClient: s.Client(), MaxRetries: 2})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEndpointContractsAndRecovery(t *testing.T) {
	var posts int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "private_endpoint_service"):
			fmt.Fprint(w, `{"private_endpoint_service":{"name":"service","dns_name":"private.test","port":4000,"az_ids":["use1-az1"],"status":"ACTIVE"}}`)
		case r.Method == "GET":
			fmt.Fprint(w, `{"endpoints":[{"id":"42","endpoint_name":"vpce-existing","status":"ACTIVE"}]}`)
		case r.Method == "POST":
			posts++
			var b map[string]string
			json.NewDecoder(r.Body).Decode(&b)
			if b["endpoint_name"] != "vpce-new" {
				t.Errorf("unexpected registration: %v", b)
			}
			fmt.Fprint(w, `{"private_endpoint":{"id":"43","endpoint_name":"vpce-new"}}`)
		case r.Method == "DELETE":
			if !strings.HasSuffix(r.URL.Path, "/42") {
				t.Errorf("delete must use internal ID: %s", r.URL.Path)
			}
			fmt.Fprint(w, `{}`)
		}
	}))
	defer s.Close()
	c := clientFor(t, s)
	ctx := context.Background()
	service, err := c.GetEndpointService(ctx, "p", "c")
	if err != nil || service.Port != 4000 {
		t.Fatalf("service=%+v error=%v", service, err)
	}
	found, err := c.RegisterEndpoint(ctx, "p", "c", "vpce-existing")
	if err != nil || found.ID != "42" || posts != 0 {
		t.Fatalf("recovery=%+v posts=%d err=%v", found, posts, err)
	}
	created, err := c.RegisterEndpoint(ctx, "p", "c", "vpce-new")
	if err != nil || created.ID != "43" || posts != 1 {
		t.Fatalf("create=%+v posts=%d err=%v", created, posts, err)
	}
	if err := c.DeleteEndpoint(ctx, "p", "c", "42"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteEndpoint(ctx, "p", "c", "vpce-existing"); err == nil {
		t.Fatal("AWS endpoint ID accepted as TiDB internal ID")
	}
}

func TestOutboundPaginationAndCreateRecovery(t *testing.T) {
	var lists, posts int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			posts++
			t.Error("must recover existing connection without create")
		}
		lists++
		if r.URL.Query().Get("pageToken") == "" {
			fmt.Fprint(w, `{"privateLinkConnections":[],"nextPageToken":"next"}`)
			return
		}
		fmt.Fprint(w, `{"privateLinkConnections":[{"privateLinkConnectionId":"plc-one","displayName":"stable-operation","type":"AWS_ENDPOINT_SERVICE","awsEndpointService":{"name":"service"}}]}`)
	}))
	defer s.Close()
	c := clientFor(t, s)
	got, err := c.EnsurePrivateLink(context.Background(), "cluster", PrivateLink{ClusterID: "cluster", DisplayName: "stable-operation", Type: "AWS_ENDPOINT_SERVICE", AWSEndpointService: &AWSEndpointService{Name: "service"}})
	if err != nil || got.ID != "plc-one" || lists != 2 || posts != 0 {
		t.Fatalf("result=%+v lists=%d posts=%d error=%v", got, lists, posts, err)
	}
}

func TestDigestAndRedirectIsolation(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer other.Close()
	var authenticated bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="test", nonce="nonce", qop="auth", algorithm=MD5`)
			w.WriteHeader(401)
			return
		}
		p, err := parseDigest(strings.TrimPrefix(auth, "Digest "))
		if err != nil {
			t.Error(err)
		}
		hash := func(s string) string { h := md5.Sum([]byte(s)); return hex.EncodeToString(h[:]) }
		expected := hash(hash("public:test:secret") + ":nonce:" + p["nc"] + ":" + p["cnonce"] + ":auth:" + hash(r.Method+":"+r.URL.RequestURI()))
		if p["response"] != expected {
			t.Errorf("incorrect Digest response")
		}
		authenticated = true
		http.Redirect(w, r, other.URL, 302)
	}))
	defer s.Close()
	c := clientFor(t, s)
	_, err := c.ListEndpoints(context.Background(), "p", "c")
	if err == nil || !authenticated || leaked.Load() {
		t.Fatalf("err=%v authenticated=%v redirect=%v", err, authenticated, leaked.Load())
	}
}

func TestSafeRetriesErrorsAndCancellation(t *testing.T) {
	var calls int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(503)
		fmt.Fprint(w, `{"message":"secret password=database-secret"}`)
	}))
	defer s.Close()
	c := clientFor(t, s)
	_, err := c.ListEndpoints(context.Background(), "p", "c")
	var api *APIError
	if !errors.As(err, &api) || api.StatusCode != 503 || calls != 3 || strings.Contains(err.Error(), "secret") {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	calls = 0
	_, err = c.CreatePrivateLink(context.Background(), "c", PrivateLink{ClusterID: "c", DisplayName: "name", Type: "AWS_ENDPOINT_SERVICE", AWSEndpointService: &AWSEndpointService{Name: "svc"}})
	if err == nil || calls != 1 {
		t.Fatalf("unsafe mutation retried %d times: %v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.ListEndpoints(ctx, "p", "c")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestMissingDataflowAndInvalidIdentifiers(t *testing.T) {
	c, err := NewClient(Options{PublicKey: "p", PrivateKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.AvailabilityZones(context.Background(), "cluster")
	if !errors.Is(err, ErrUnverifiedOperation) {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.ListEndpoints(ctx, "../secret", "cluster"); err == nil {
		t.Fatal("path traversal accepted")
	}
}

func TestDigestRejectsUnsupportedChallenges(t *testing.T) {
	c, err := NewClient(Options{PublicKey: "public", PrivateKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	for _, challenge := range []string{`realm="test", nonce="nonce", qop="auth-int"`, `realm="test", nonce="nonce", algorithm=SHA-512`, `realm="test",nonce="one",nonce="two"`, `realm="test",nonce="unterminated`} {
		if _, err := c.authorization("GET", "/path", challenge); err == nil {
			t.Fatalf("accepted invalid Digest challenge %s", challenge)
		}
	}
}

func TestPermissionAndMissingResourceRemainDistinct(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, `{"message":"private-key-secret"}`)
			}))
			defer s.Close()
			_, err := clientFor(t, s).GetEndpointService(context.Background(), "project", "cluster")
			var api *APIError
			if !errors.As(err, &api) || api.NotFound() != (status == 404) || api.PermissionDenied() != (status == 401 || status == 403) || errors.Is(err, ErrUnverifiedOperation) {
				t.Fatalf("incorrect capability inference: %v", err)
			}
		})
	}
}

func TestCreateRecoveryRefusesConflictingIdentity(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Fatal("must not mutate on conflicting recovery")
		}
		fmt.Fprint(w, `{"privateLinkConnections":[{"privateLinkConnectionId":"plc-one","displayName":"operation","type":"AWS_ENDPOINT_SERVICE","awsEndpointService":{"name":"other-service"}}]}`)
	}))
	defer s.Close()
	_, err := clientFor(t, s).EnsurePrivateLink(context.Background(), "cluster", PrivateLink{ClusterID: "cluster", DisplayName: "operation", Type: "AWS_ENDPOINT_SERVICE", AWSEndpointService: &AWSEndpointService{Name: "wanted-service"}})
	if err == nil {
		t.Fatal("adopted unrelated private link")
	}
}

func TestBaseURLsRejectEmbeddedCredentialsAndInsecureOrigins(t *testing.T) {
	for _, base := range []string{"http://example.com", "https://public:secret@example.com", "https://example.com?secret=key", "https://example.com#fragment"} {
		if _, err := NewClient(Options{BaseURL: base, PublicKey: "public", PrivateKey: "secret"}); err == nil || strings.Contains(err.Error(), "public:secret") {
			t.Fatalf("unsafe URL handling: %v", err)
		}
	}
}
