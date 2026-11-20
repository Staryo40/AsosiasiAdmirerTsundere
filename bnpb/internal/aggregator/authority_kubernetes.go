package aggregator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const authorityConfigMapKey = "authority.json"

var errAuthorityConflict = errors.New("authority ConfigMap compare-and-swap conflict")
var errAuthorityMissing = errors.New("authority ConfigMap is missing")

var errHostGrantInactive = errors.New("host grant is inactive or expired")

type KubernetesConfig struct{ ServerURL, Namespace, ConfigMap, TokenFile, CAFile string }

type authorityKubernetesClient interface {
	Read(context.Context) (authorityRecord, string, error)
	CompareAndSwap(context.Context, string, authorityRecord) (string, error)
}

type kubernetesAuthorityClient struct {
	baseURL, namespace, name, tokenFile string
	http                                *http.Client
}
type configMapEnvelope struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Data map[string]string `json:"data"`
}

func NewKubernetesAuthorityClient(cfg KubernetesConfig) (authorityKubernetesClient, error) {
	if !strings.HasPrefix(cfg.ServerURL, "https://") {
		return nil, errors.New("Kubernetes API URL must use HTTPS")
	}
	if cfg.Namespace == "" || cfg.ConfigMap == "" || cfg.TokenFile == "" || cfg.CAFile == "" {
		return nil, errors.New("Kubernetes authority requires namespace, ConfigMap, token and CA")
	}
	if _, err := readBearerToken(cfg.TokenFile, "Kubernetes service-account"); err != nil {
		return nil, err
	}
	transport, err := tlsTransport(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &kubernetesAuthorityClient{baseURL: strings.TrimRight(cfg.ServerURL, "/"), namespace: cfg.Namespace, name: cfg.ConfigMap,
		tokenFile: cfg.TokenFile, http: httpClient}, nil
}

const maxBearerTokenBytes = 16 << 10

func readBearerToken(path, label string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("%s token file is required", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("read %s token: %w", label, err)
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, maxBearerTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("read %s token: %w", label, err)
	}
	if len(content) > maxBearerTokenBytes {
		return "", fmt.Errorf("invalid %s bearer token", label)
	}
	token := strings.TrimSpace(string(content))
	if token == "" || len(token) > maxBearerTokenBytes || !validBearerToken(token) {
		return "", fmt.Errorf("invalid %s bearer token", label)
	}
	return token, nil
}

func validBearerToken(token string) bool {
	padding := false
	hasTokenByte := false
	for i := range token {
		c := token[i]
		switch {
		case c == '=':
			padding = true
		case padding:
			return false
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-' || c == '.' || c == '_' || c == '~' || c == '+' || c == '/':
			hasTokenByte = true
		default:
			return false
		}
	}
	return hasTokenByte
}

func tlsTransport(caFile string) (*http.Transport, error) {
	if caFile == "" {
		return nil, errors.New("TLS CA file is required")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("TLS CA file contains no certificates")
	}
	return &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, nil
}

func (client *kubernetesAuthorityClient) endpoint() string {
	return client.baseURL + "/api/v1/namespaces/" + url.PathEscape(client.namespace) + "/configmaps/" + url.PathEscape(client.name)
}

func (client *kubernetesAuthorityClient) request(ctx context.Context, resourceVersion string, record *authorityRecord) (*http.Response, error) {
	method := http.MethodGet
	var body io.Reader
	if record != nil {
		method = http.MethodPut
		envelope := configMapEnvelope{APIVersion: "v1", Kind: "ConfigMap", Data: map[string]string{authorityConfigMapKey: ""}}
		envelope.Metadata.Name, envelope.Metadata.Namespace, envelope.Metadata.ResourceVersion = client.name, client.namespace, resourceVersion
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		envelope.Data[authorityConfigMapKey] = string(encoded)
		payload, err := json.Marshal(envelope)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(payload)
	}
	token, err := readBearerToken(client.tokenFile, "Kubernetes service-account")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, client.endpoint(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return client.http.Do(req)
}

func (client *kubernetesAuthorityClient) Read(ctx context.Context) (authorityRecord, string, error) {
	response, err := client.request(ctx, "", nil)
	if err != nil {
		return authorityRecord{}, "", err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return authorityRecord{}, "", errAuthorityMissing
	}
	if response.StatusCode != http.StatusOK {
		return authorityRecord{}, "", fmt.Errorf("Kubernetes ConfigMap GET returned HTTP %d", response.StatusCode)
	}
	var envelope configMapEnvelope
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&envelope); err != nil {
		return authorityRecord{}, "", fmt.Errorf("decode Kubernetes ConfigMap: %w", err)
	}
	if envelope.Metadata.ResourceVersion == "" {
		return authorityRecord{}, "", errors.New("Kubernetes ConfigMap omitted resourceVersion")
	}
	encoded, ok := envelope.Data[authorityConfigMapKey]
	if !ok {
		return authorityRecord{}, "", errors.New("Kubernetes ConfigMap omitted authority record")
	}
	var record authorityRecord
	if err := json.Unmarshal([]byte(encoded), &record); err != nil {
		return authorityRecord{}, "", fmt.Errorf("decode authority record: %w", err)
	}
	return record, envelope.Metadata.ResourceVersion, nil
}

func (client *kubernetesAuthorityClient) CompareAndSwap(ctx context.Context, version string, record authorityRecord) (string, error) {
	if version == "" {
		return "", errors.New("opaque resourceVersion is required for ConfigMap update")
	}
	response, err := client.request(ctx, version, &record)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return "", errAuthorityConflict
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Kubernetes ConfigMap PUT returned HTTP %d", response.StatusCode)
	}
	var envelope configMapEnvelope
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&envelope); err != nil {
		return "", fmt.Errorf("decode updated ConfigMap: %w", err)
	}
	if envelope.Metadata.ResourceVersion == "" {
		return "", errors.New("updated ConfigMap omitted resourceVersion")
	}
	return envelope.Metadata.ResourceVersion, nil
}

type HostFenceConfig struct{ BaseURL, TokenFile, CAFile string }
type hostFenceClient struct {
	baseURL, token string
	http           *http.Client
}

func NewHostFenceClient(cfg HostFenceConfig) (HostFenceExecutor, error) {
	if !strings.HasPrefix(cfg.BaseURL, "https://") {
		return nil, errors.New("host fence URL must use HTTPS")
	}
	token, err := readBearerToken(cfg.TokenFile, "host fence")
	if err != nil {
		return nil, err
	}
	transport, err := tlsTransport(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &hostFenceClient{baseURL: strings.TrimRight(cfg.BaseURL, "/"), token: token, http: httpClient}, nil
}

func (client *hostFenceClient) request(ctx context.Context, method, target string, payload any) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, client.baseURL+target, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+client.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return client.http.Do(req)
}

type hostGrantRequest struct {
	DomainUUID      string         `json:"domain_uuid"`
	Epoch           uint64         `json:"epoch"`
	RenewalSeq      uint64         `json:"renewal_seq"`
	Owner           string         `json:"owner"`
	OwnerDomainUUID string         `json:"owner_domain_uuid"`
	Mode            hostRevokeMode `json:"mode,omitempty"`
}

func hostGrantPayload(identity hostFenceIdentity) hostGrantRequest {
	return hostGrantRequest{
		DomainUUID: identity.DomainUUID, Epoch: identity.Epoch, RenewalSeq: identity.RenewalSeq,
		Owner: identity.Owner, OwnerDomainUUID: identity.OwnerDomainUUID,
	}
}

func (client *hostFenceClient) Grant(ctx context.Context, identity hostFenceIdentity) (authorityGrant, error) {
	response, err := client.request(ctx, http.MethodPost, "/v1/grant", hostGrantPayload(identity))
	if err != nil {
		return authorityGrant{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return authorityGrant{}, fmt.Errorf("host grant returned HTTP %d", response.StatusCode)
	}
	return decodeHostGrant(response.Body)
}

func (client *hostFenceClient) Current(ctx context.Context, domain string) (authorityGrant, error) {
	response, err := client.request(ctx, http.MethodGet, "/v1/grant?domain_uuid="+url.QueryEscape(domain), nil)
	if err != nil {
		return authorityGrant{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
		grant, decodeErr := decodeInactiveHostGrant(response.Body)
		if decodeErr != nil {
			return authorityGrant{}, decodeErr
		}
		return grant, errHostGrantInactive
	}
	if response.StatusCode != http.StatusOK {
		return authorityGrant{}, fmt.Errorf("host current grant returned HTTP %d", response.StatusCode)
	}
	return decodeHostGrant(response.Body)
}

func decodeHostGrant(body io.Reader) (authorityGrant, error) {
	var grant authorityGrant
	if err := json.NewDecoder(io.LimitReader(body, 16<<10)).Decode(&grant); err != nil {
		return authorityGrant{}, fmt.Errorf("decode host grant: %w", err)
	}
	if grant.DomainUUID == "" || grant.HostBootID == "" || grant.Epoch < 1 || grant.RenewalSeq < 1 || grant.DeadlineBootTimeNS < 1 {
		return authorityGrant{}, errors.New("host grant omitted identity or absolute deadline")
	}
	return grant, nil
}
func decodeInactiveHostGrant(body io.Reader) (authorityGrant, error) {
	var grant authorityGrant
	if err := json.NewDecoder(io.LimitReader(body, 16<<10)).Decode(&grant); err != nil {
		return authorityGrant{}, fmt.Errorf("decode inactive host grant: %w", err)
	}
	if grant.DomainUUID == "" && grant.HostBootID == "" && grant.Epoch == 0 && grant.RenewalSeq == 0 && grant.DeadlineBootTimeNS == 0 {
		return authorityGrant{}, nil
	}
	if grant.DomainUUID == "" || grant.HostBootID == "" || grant.Epoch < 1 || grant.RenewalSeq < 1 || grant.DeadlineBootTimeNS < 0 {
		return authorityGrant{}, errors.New("inactive host grant omitted high-water identity")
	}
	return grant, nil
}

func (client *hostFenceClient) Revoke(ctx context.Context, identity hostFenceIdentity, mode hostRevokeMode) error {
	request := hostGrantPayload(identity)
	request.Mode = mode
	response, err := client.request(ctx, http.MethodPost, "/v1/revoke", request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("host revoke returned HTTP %d", response.StatusCode)
	}
	return nil
}
