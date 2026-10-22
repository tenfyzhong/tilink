package tidb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRemoteValidationNeverCreatesReplicationTasks(t *testing.T) {
	var seen []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		switch {
		case strings.HasSuffix(r.URL.Path, "changefeeds:test"):
			var b map[string]json.RawMessage
			json.NewDecoder(r.Body).Decode(&b)
			if b["sink"] == nil {
				t.Error("missing sink envelope")
			}
			fmt.Fprint(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "migrationPrechecks"):
			var b MigrationPrecheckRequest
			json.NewDecoder(r.Body).Decode(&b)
			if b.Sources[0].ConnProfile.EndpointID != "plc-source" {
				t.Error("lost private endpoint")
			}
			fmt.Fprint(w, `{"precheckId":"check-one"}`)
		case r.Method == "GET":
			fmt.Fprint(w, `{"precheckId":"check-one","status":"FINISHED","failedCnt":0,"successCnt":3,"total":3}`)
		case r.Method == "DELETE":
			fmt.Fprint(w, `{}`)
		default:
			t.Errorf("unexpected mutation %s", r.URL.Path)
		}
	}))
	defer s.Close()
	c := clientFor(t, s)
	ctx := context.Background()
	sink := Sink{Type: "MYSQL", MySQL: &MySQLSink{Network: SinkNetwork{NetworkType: "PRIVATE_LINK", PrivateLink: &SinkPrivateLink{ID: "plc-sink", Port: 3306}}, Authentication: MySQLAuthentication{Username: "user", Password: "database-secret", EnableTLS: true}}}
	if err := c.TestSink(ctx, "cluster", sink); err != nil {
		t.Fatal(err)
	}
	req := MigrationPrecheckRequest{DisplayName: "connection-check", Mode: "ALL", Target: MigrationTarget{User: "target", Password: "target-secret"}, Sources: []MigrationSource{{SourceType: "AWS_RDS_MYSQL", ConnProfile: ConnectionProfile{ConnType: "PRIVATE_LINK", EndpointID: "plc-source", Host: "mysql.private", Port: 3306, User: "source", Password: "source-secret"}}}}
	id, err := c.CreateMigrationPrecheck(ctx, "cluster", req)
	if err != nil || id != "check-one" {
		t.Fatalf("id=%s err=%v", id, err)
	}
	got, err := c.GetMigrationPrecheck(ctx, "cluster", id)
	if err != nil || got.Status != "FINISHED" || got.SuccessCount != 3 {
		t.Fatalf("precheck=%+v err=%v", got, err)
	}
	if err := c.CancelMigrationPrecheck(ctx, "cluster", id); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 4 {
		t.Fatalf("unexpected calls %v", seen)
	}
}

func TestDataflowWireContracts(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, ":getAvailabilityZones"):
			fmt.Fprint(w, `{"accountId":"1234","azIds":["use1-az1"]}`)
		case strings.HasSuffix(r.URL.Path, ":attachDomains"):
			var b map[string]json.RawMessage
			json.NewDecoder(r.Body).Decode(&b)
			if string(b["validateOnly"]) != "true" {
				t.Error("missing validation flag")
			}
			fmt.Fprint(w, `{"attachDomainId":"domain-one","domains":[{"name":"sink.private","state":"ACTIVE"}]}`)
		case strings.HasSuffix(r.URL.Path, ":detachDomains"):
			var b map[string]string
			json.NewDecoder(r.Body).Decode(&b)
			if b["attachDomainId"] != "domain-one" {
				t.Error("wrong domain ID")
			}
			fmt.Fprint(w, `{}`)
		case r.Method == "POST":
			var b struct {
				Link PrivateLink `json:"privateLinkConnection"`
			}
			json.NewDecoder(r.Body).Decode(&b)
			if b.Link.ID != "" || b.Link.AWSEndpointService.EndpointID != "" || b.Link.ClusterID != "cluster" {
				t.Error("incorrect create payload")
			}
			fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","state":"CREATING"}`)
		case r.Method == "GET":
			fmt.Fprint(w, `{"privateLinkConnectionId":"plc-one","state":"ACTIVE"}`)
		case r.Method == "DELETE":
			w.WriteHeader(404)
		}
	}))
	defer s.Close()
	c := clientFor(t, s)
	ctx := context.Background()
	zones, err := c.AvailabilityZones(ctx, "cluster")
	if err != nil || zones.AccountID != "1234" {
		t.Fatalf("zones=%+v err=%v", zones, err)
	}
	_, err = c.CreatePrivateLink(ctx, "cluster", PrivateLink{ID: "ignored", ClusterID: "cluster", DisplayName: "operation", Type: "AWS_ENDPOINT_SERVICE", AWSEndpointService: &AWSEndpointService{Name: "service", EndpointID: "ignored"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.GetPrivateLink(ctx, "cluster", "plc-one")
	if err != nil || got.State != "ACTIVE" {
		t.Fatalf("link=%+v err=%v", got, err)
	}
	attached, err := c.AttachDomains(ctx, "cluster", "plc-one", AttachedDomain{Type: "TIDBCLOUD_MANAGED"}, true)
	if err != nil || attached.ID != "domain-one" {
		t.Fatalf("attachment=%+v err=%v", attached, err)
	}
	if err := c.DetachDomains(ctx, "cluster", "plc-one", "domain-one"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeletePrivateLink(ctx, "cluster", "plc-one"); err != nil {
		t.Fatal(err)
	}
}

func TestPaginationLoopFailsClosed(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"nextPageToken":"repeated"}`) }))
	defer s.Close()
	if _, err := clientFor(t, s).ListPrivateLinks(context.Background(), "cluster"); err == nil {
		t.Fatal("pagination loop accepted")
	}
}

func TestRemoteValidationErrorsDoNotExposeDatabaseSecrets(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		fmt.Fprint(w, `{"message":"database-secret"}`)
	}))
	defer s.Close()
	err := clientFor(t, s).TestSink(context.Background(), "cluster", Sink{Type: "MYSQL", MySQL: &MySQLSink{Authentication: MySQLAuthentication{Password: "database-secret"}}})
	if err == nil || strings.Contains(err.Error(), "database-secret") {
		t.Fatalf("unsafe error: %v", err)
	}
}
