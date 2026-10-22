// Package tidb implements the shared TiDB Cloud connectivity API client.
package tidb

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrUnverifiedOperation = errors.New("TiDB API operation requires an explicitly configured dataflow endpoint")

type Options struct {
	BaseURL         string
	DataflowBaseURL string
	PublicKey       string
	PrivateKey      string
	HTTPClient      *http.Client
	MaxRetries      int
}

type Client struct {
	base, dataflow  *url.URL
	public, private string
	http            *http.Client
	retries         int
}

type APIError struct {
	StatusCode int
	Operation  string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("TiDB API %s failed (HTTP %d)", e.Operation, e.StatusCode)
}

// MutationUncertain reports whether an error response can conceal a completed mutation.
func (e *APIError) MutationUncertain() bool {
	return e.StatusCode < 400 || e.StatusCode >= 500 || e.StatusCode == 408 || e.StatusCode == 429
}
func (e *APIError) NotFound() bool         { return e.StatusCode == 404 }
func (e *APIError) PermissionDenied() bool { return e.StatusCode == 401 || e.StatusCode == 403 }

func NewClient(o Options) (*Client, error) {
	if o.BaseURL == "" {
		o.BaseURL = "https://api.tidbcloud.com/api/v1beta"
	}
	parse := func(raw string) (*url.URL, error) {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("invalid TiDB API base URL")
		}
		if u.Scheme == "http" && u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1" {
			return nil, errors.New("TiDB API requires HTTPS")
		}
		u.Path = strings.TrimRight(u.Path, "/")
		return u, nil
	}
	base, err := parse(o.BaseURL)
	if err != nil {
		return nil, err
	}
	var dataflow *url.URL
	if o.DataflowBaseURL != "" {
		dataflow, err = parse(o.DataflowBaseURL)
		if err != nil {
			return nil, err
		}
	}
	if o.PublicKey == "" || o.PrivateKey == "" {
		return nil, errors.New("TiDB API credentials are required")
	}
	h := http.Client{Timeout: 30 * time.Second}
	if o.HTTPClient != nil {
		h = *o.HTTPClient
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("TiDB API redirects are disabled") }
	retries := o.MaxRetries
	if retries < 0 {
		retries = 0
	}
	if retries > 3 {
		retries = 3
	}
	return &Client{base: base, dataflow: dataflow, public: o.PublicKey, private: o.PrivateKey, http: &h, retries: retries}, nil
}

func validID(id string) bool {
	if id == "" || id == "." || id == ".." {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
func ids(values ...string) error {
	for _, id := range values {
		if !validID(id) {
			return errors.New("invalid TiDB resource identifier")
		}
	}
	return nil
}

func (c *Client) request(ctx context.Context, base *url.URL, method, path string, query url.Values, body, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if base == nil {
		return ErrUnverifiedOperation
	}
	u := *base
	u.Path += path
	u.RawQuery = query.Encode()
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return errors.New("cannot encode TiDB request")
		}
	}
	attempts := 1
	if method == http.MethodGet {
		attempts += c.retries
	}
	for attempt := 0; attempt < attempts; attempt++ {
		response, err := c.exchange(ctx, method, &u, payload)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("TiDB API transport failed; reconcile before retrying a mutation")
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			defer response.Body.Close()
			if out == nil {
				_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<20))
				return err
			}
			data, readErr := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
			if readErr != nil || len(data) > 4<<20 {
				return errors.New("invalid TiDB API response")
			}
			if err = json.Unmarshal(data, out); err != nil {
				return errors.New("invalid TiDB API response")
			}
			return nil
		}
		status := response.StatusCode
		io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		response.Body.Close()
		if method != http.MethodGet || attempt+1 == attempts || !(status == 429 || status == 502 || status == 503 || status == 504) {
			return &APIError{StatusCode: status, Operation: method}
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("TiDB API request exhausted retries")
}

func (c *Client) exchange(ctx context.Context, method string, u *url.URL, payload []byte) (*http.Response, error) {
	send := func(auth string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		return c.http.Do(req)
	}
	res, err := send("")
	if err != nil || res.StatusCode != 401 {
		return res, err
	}
	challenge := res.Header.Get("WWW-Authenticate")
	if !strings.HasPrefix(strings.ToLower(challenge), "digest ") {
		return res, nil
	}
	io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	res.Body.Close()
	auth, err := c.authorization(method, u.RequestURI(), challenge[7:])
	if err != nil {
		return nil, err
	}
	return send(auth)
}

func parseDigest(s string) (map[string]string, error) {
	result := map[string]string{}
	for strings.TrimSpace(s) != "" {
		s = strings.TrimSpace(s)
		i := strings.IndexByte(s, '=')
		if i <= 0 {
			return nil, errors.New("invalid Digest challenge")
		}
		key := strings.ToLower(strings.TrimSpace(s[:i]))
		s = strings.TrimSpace(s[i+1:])
		var value string
		if strings.HasPrefix(s, "\"") {
			end := 1
			escaped := false
			for ; end < len(s); end++ {
				if !escaped && s[end] == '"' {
					break
				}
				if !escaped && s[end] == '\\' {
					escaped = true
				} else {
					escaped = false
				}
			}
			if end >= len(s) {
				return nil, errors.New("invalid Digest challenge")
			}
			var err error
			value, err = strconv.Unquote(s[:end+1])
			if err != nil {
				return nil, errors.New("invalid Digest challenge")
			}
			s = s[end+1:]
		} else {
			end := strings.IndexByte(s, ',')
			if end < 0 {
				value = s
				s = ""
			} else {
				value = strings.TrimSpace(s[:end])
				s = s[end:]
			}
		}
		if _, exists := result[key]; exists {
			return nil, errors.New("duplicate Digest parameter")
		}
		result[key] = value
		s = strings.TrimSpace(s)
		if s != "" {
			if s[0] != ',' {
				return nil, errors.New("invalid Digest challenge")
			}
			s = s[1:]
		}
	}
	return result, nil
}

func (c *Client) authorization(method, uri, challenge string) (string, error) {
	p, err := parseDigest(challenge)
	if err != nil {
		return "", err
	}
	if p["nonce"] == "" || p["realm"] == "" {
		return "", errors.New("incomplete Digest challenge")
	}
	algorithm := strings.ToUpper(p["algorithm"])
	if algorithm == "" {
		algorithm = "MD5"
	}
	hash := func(s string) string {
		if strings.HasPrefix(algorithm, "SHA-256") {
			v := sha256.Sum256([]byte(s))
			return hex.EncodeToString(v[:])
		}
		v := md5.Sum([]byte(s))
		return hex.EncodeToString(v[:])
	}
	if algorithm != "MD5" && algorithm != "MD5-SESS" && algorithm != "SHA-256" && algorithm != "SHA-256-SESS" {
		return "", errors.New("unsupported Digest algorithm")
	}
	qop := ""
	if p["qop"] != "" {
		for _, v := range strings.Split(p["qop"], ",") {
			if strings.TrimSpace(v) == "auth" {
				qop = "auth"
			}
		}
		if qop == "" {
			return "", errors.New("unsupported Digest quality of protection")
		}
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", errors.New("Digest randomness unavailable")
	}
	cnonce := hex.EncodeToString(random)
	ha1 := hash(c.public + ":" + p["realm"] + ":" + c.private)
	if strings.HasSuffix(algorithm, "-SESS") {
		ha1 = hash(ha1 + ":" + p["nonce"] + ":" + cnonce)
	}
	response := hash(ha1 + ":" + p["nonce"] + ":" + hash(method+":"+uri))
	if qop != "" {
		response = hash(ha1 + ":" + p["nonce"] + ":00000001:" + cnonce + ":auth:" + hash(method+":"+uri))
	}
	fields := []string{"username=" + strconv.Quote(c.public), "realm=" + strconv.Quote(p["realm"]), "nonce=" + strconv.Quote(p["nonce"]), "uri=" + strconv.Quote(uri), "response=" + strconv.Quote(response), "algorithm=" + algorithm}
	if qop != "" {
		fields = append(fields, "qop=auth", "nc=00000001", "cnonce="+strconv.Quote(cnonce))
	} else if strings.HasSuffix(algorithm, "-SESS") {
		fields = append(fields, "cnonce="+strconv.Quote(cnonce))
	}
	if p["opaque"] != "" {
		fields = append(fields, "opaque="+strconv.Quote(p["opaque"]))
	}
	return "Digest " + strings.Join(fields, ", "), nil
}
