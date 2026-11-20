// Authority configuration follows bnpb/contracts/v1/README.md.
package aggregator

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

type AuthorityConfig struct {
	Kubernetes      KubernetesConfig
	HostFence       HostFenceConfig
	OwnerDomain     string
	OwnerEndpoint   string
	Domains         []string
	RenewalInterval time.Duration
}

const (
	authorityOwnerDomainEnv = "BNPB_AUTHORITY_OWNER_DOMAIN_UUID"
	authorityNodeNameEnv    = "BNPB_K8S_NODE_NAME"
	authorityNodeDomainsEnv = "BNPB_AUTHORITY_NODE_DOMAINS"
)

var (
	authorityDomainPattern     = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	kubernetesNamespacePattern = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)
	kubernetesSubdomainPattern = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*$`)
)

func loadAuthorityConfig(lookup func(string) (string, bool)) (AuthorityConfig, error) {
	cfg := AuthorityConfig{RenewalInterval: 5 * time.Second}
	set := func(name string, dst *string) bool {
		value, ok := lookup(name)
		if ok {
			*dst = strings.TrimSpace(value)
		}
		return ok
	}
	var configured int
	if set("BNPB_AUTHORITY_OWNER_ENDPOINT", &cfg.OwnerEndpoint) && cfg.OwnerEndpoint != "" {
		if err := validateOwnerEndpoint(cfg.OwnerEndpoint); err != nil {
			return AuthorityConfig{}, err
		}
	}
	for _, field := range []struct {
		name string
		dst  *string
	}{
		{"BNPB_AUTHORITY_KUBERNETES_SERVER_URL", &cfg.Kubernetes.ServerURL},
		{"BNPB_AUTHORITY_KUBERNETES_NAMESPACE", &cfg.Kubernetes.Namespace},
		{"BNPB_AUTHORITY_KUBERNETES_CONFIGMAP", &cfg.Kubernetes.ConfigMap},
		{"BNPB_AUTHORITY_KUBERNETES_TOKEN_FILE", &cfg.Kubernetes.TokenFile},
		{"BNPB_AUTHORITY_KUBERNETES_CA_FILE", &cfg.Kubernetes.CAFile},
		{"BNPB_AUTHORITY_HOST_BASE_URL", &cfg.HostFence.BaseURL},
		{"BNPB_AUTHORITY_HOST_TOKEN_FILE", &cfg.HostFence.TokenFile},
		{"BNPB_AUTHORITY_HOST_CA_FILE", &cfg.HostFence.CAFile},
	} {
		if set(field.name, field.dst) {
			configured++
		}
	}
	ownerDomain, ownerDomainSet, err := loadAuthorityOwnerDomain(lookup)
	if err != nil {
		return AuthorityConfig{}, err
	}
	if ownerDomainSet {
		cfg.OwnerDomain = ownerDomain
		configured++
	}
	domains, domainsSet := lookup("BNPB_AUTHORITY_DOMAINS")
	if domainsSet {
		configured++
		for _, domain := range strings.Split(domains, ",") {
			cfg.Domains = append(cfg.Domains, strings.TrimSpace(domain))
		}
	}
	if renewal, ok := lookup("BNPB_AUTHORITY_RENEWAL_INTERVAL"); ok {
		configured++
		parsed, err := time.ParseDuration(strings.TrimSpace(renewal))
		if err != nil || parsed <= 0 || parsed > time.Minute {
			return AuthorityConfig{}, fmt.Errorf("BNPB_AUTHORITY_RENEWAL_INTERVAL: duration must be positive and at most 1m")
		}
		cfg.RenewalInterval = parsed
	}
	if configured == 0 {
		return cfg, nil
	}
	if configured != 10 && configured != 11 {
		return AuthorityConfig{}, fmt.Errorf("authority configuration: configure every Kubernetes and host-fence setting, domains and owner domain together")
	}
	if err := cfg.validate(); err != nil {
		return AuthorityConfig{}, err
	}
	return cfg, nil
}

func loadAuthorityOwnerDomain(lookup func(string) (string, bool)) (string, bool, error) {
	literal, literalSet := lookup(authorityOwnerDomainEnv)
	node, nodeSet := lookup(authorityNodeNameEnv)
	mapping, mappingSet := lookup(authorityNodeDomainsEnv)
	literal = strings.TrimSpace(literal)
	if !nodeSet && !mappingSet {
		return literal, literalSet, nil
	}
	if !validKubernetesSubdomain(node) {
		return "", false, fmt.Errorf("%s: required valid Kubernetes node name with %s", authorityNodeNameEnv, authorityNodeDomainsEnv)
	}
	domain, err := authorityDomainForNode(node, mapping)
	if err != nil {
		return "", false, err
	}
	if literalSet && !strings.EqualFold(literal, domain) {
		return "", false, fmt.Errorf("%s: conflicts with %s for the scheduled node", authorityOwnerDomainEnv, authorityNodeDomainsEnv)
	}
	return domain, true, nil
}

func authorityDomainForNode(node, mapping string) (string, error) {
	nodeDomains, err := parseAuthorityNodeDomains(mapping)
	if err != nil {
		return "", err
	}
	if domain, ok := nodeDomains[node]; ok {
		return domain, nil
	}
	return "", fmt.Errorf("%s: no mapping for scheduled node %q", authorityNodeDomainsEnv, node)
}

func parseAuthorityNodeDomains(mapping string) (map[string]string, error) {
	decoder := json.NewDecoder(strings.NewReader(mapping))
	if start, err := decoder.Token(); err != nil || start != json.Delim('{') {
		return nil, fmt.Errorf("%s: must be a JSON object mapping node names to domain UUIDs", authorityNodeDomainsEnv)
	}
	nodeDomains := make(map[string]string)
	assignedDomains := make(map[string]struct{})
	for decoder.More() {
		node, domain, err := decodeAuthorityNodeDomain(decoder)
		if err != nil {
			return nil, err
		}
		if _, exists := nodeDomains[node]; exists {
			return nil, fmt.Errorf("%s: duplicate node name %q", authorityNodeDomainsEnv, node)
		}
		domainKey := strings.ToLower(domain)
		if _, exists := assignedDomains[domainKey]; exists {
			return nil, fmt.Errorf("%s: duplicate domain UUID", authorityNodeDomainsEnv)
		}
		nodeDomains[node] = domain
		assignedDomains[domainKey] = struct{}{}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return nil, fmt.Errorf("%s: invalid JSON object", authorityNodeDomainsEnv)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("%s: unexpected data after JSON object", authorityNodeDomainsEnv)
	}
	return nodeDomains, nil
}

func decodeAuthorityNodeDomain(decoder *json.Decoder) (string, string, error) {
	key, err := decoder.Token()
	node, ok := key.(string)
	if err != nil || !ok || !validKubernetesSubdomain(node) {
		return "", "", fmt.Errorf("%s: invalid node name", authorityNodeDomainsEnv)
	}
	var domain string
	if err := decoder.Decode(&domain); err != nil || !authorityDomainPattern.MatchString(domain) {
		return "", "", fmt.Errorf("%s: invalid domain UUID for node %q", authorityNodeDomainsEnv, node)
	}
	return node, domain, nil
}

func validKubernetesSubdomain(name string) bool {
	return len(name) <= 253 && kubernetesSubdomainPattern.MatchString(name)
}

func (cfg AuthorityConfig) validate() error {
	if cfg.OwnerEndpoint != "" {
		if err := validateOwnerEndpoint(cfg.OwnerEndpoint); err != nil {
			return err
		}
	}
	if err := validateSecureOrigin("BNPB_AUTHORITY_KUBERNETES_SERVER_URL", cfg.Kubernetes.ServerURL); err != nil {
		return err
	}
	if err := validateSecureOrigin("BNPB_AUTHORITY_HOST_BASE_URL", cfg.HostFence.BaseURL); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"BNPB_AUTHORITY_KUBERNETES_NAMESPACE": cfg.Kubernetes.Namespace,
		"BNPB_AUTHORITY_KUBERNETES_CONFIGMAP": cfg.Kubernetes.ConfigMap,
	} {
		if value == "" || strings.IndexFunc(value, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
			return fmt.Errorf("%s: required nonempty value without whitespace/control characters", name)
		}
	}
	if len(cfg.Kubernetes.Namespace) > 63 || !kubernetesNamespacePattern.MatchString(cfg.Kubernetes.Namespace) {
		return fmt.Errorf("BNPB_AUTHORITY_KUBERNETES_NAMESPACE: invalid namespace name")
	}
	if !validKubernetesSubdomain(cfg.Kubernetes.ConfigMap) {
		return fmt.Errorf("BNPB_AUTHORITY_KUBERNETES_CONFIGMAP: invalid ConfigMap name")
	}
	if cfg.RenewalInterval <= 0 || cfg.RenewalInterval >= 10*time.Second {
		return fmt.Errorf("BNPB_AUTHORITY_RENEWAL_INTERVAL: duration must be positive and below the 10s fencing lead")
	}
	if len(cfg.Domains) == 0 || len(cfg.Domains) > 32 {
		return fmt.Errorf("BNPB_AUTHORITY_DOMAINS: must contain between 1 and 32 domain UUIDs")
	}
	for i, domain := range cfg.Domains {
		if !authorityDomainPattern.MatchString(domain) {
			return fmt.Errorf("BNPB_AUTHORITY_DOMAINS: invalid domain UUID")
		}
		for _, prior := range cfg.Domains[:i] {
			if strings.EqualFold(domain, prior) {
				return fmt.Errorf("BNPB_AUTHORITY_DOMAINS: duplicate domain UUID")
			}
		}
	}
	if !authorityDomainPattern.MatchString(cfg.OwnerDomain) {
		return fmt.Errorf("BNPB_AUTHORITY_OWNER_DOMAIN_UUID: invalid domain UUID")
	}
	foundOwnerDomain := false
	for _, domain := range cfg.Domains {
		if strings.EqualFold(domain, cfg.OwnerDomain) {
			foundOwnerDomain = true
			break
		}
	}
	if !foundOwnerDomain {
		return fmt.Errorf("BNPB_AUTHORITY_OWNER_DOMAIN_UUID: must be included in BNPB_AUTHORITY_DOMAINS")
	}
	for _, file := range []struct{ name, path string }{
		{"BNPB_AUTHORITY_KUBERNETES_TOKEN_FILE", cfg.Kubernetes.TokenFile},
		{"BNPB_AUTHORITY_KUBERNETES_CA_FILE", cfg.Kubernetes.CAFile},
		{"BNPB_AUTHORITY_HOST_TOKEN_FILE", cfg.HostFence.TokenFile},
		{"BNPB_AUTHORITY_HOST_CA_FILE", cfg.HostFence.CAFile},
	} {
		if err := validateCredentialFile(file.name, file.path, strings.Contains(file.name, "CA_FILE")); err != nil {
			return err
		}
	}
	return nil
}

func validateSecureOrigin(name, address string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return fmt.Errorf("%s: must be an HTTPS origin URL", name)
	}
	return validateURLPort(name, parsed)
}
func validateOwnerEndpoint(address string) error {
	parsed, err := url.Parse(address)
	if len(address) > 2048 || err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.Opaque != "" || (parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawPath != "" {
		return fmt.Errorf("BNPB_AUTHORITY_OWNER_ENDPOINT: must be a bounded HTTP(S) origin URL")
	}
	return validateURLPort("BNPB_AUTHORITY_OWNER_ENDPOINT", parsed)
}

func validateCredentialFile(name, path string, ca bool) error {
	if path == "" {
		return fmt.Errorf("%s: required file path", name)
	}
	contents, err := os.ReadFile(path)
	if err != nil || len(strings.TrimSpace(string(contents))) == 0 {
		return fmt.Errorf("%s: credential file is unavailable or empty", name)
	}
	if ca {
		remaining := contents
		for len(remaining) > 0 {
			block, rest := pem.Decode(remaining)
			if block == nil {
				break
			}
			remaining = rest
			if block.Type == "CERTIFICATE" {
				if _, err := x509.ParseCertificate(block.Bytes); err == nil {
					return nil
				}
			}
		}
		return fmt.Errorf("%s: must contain a valid PEM certificate", name)
	}
	token := strings.TrimSpace(string(contents))
	if strings.IndexFunc(token, func(character rune) bool { return character <= 0x20 || character >= 0x7f }) >= 0 {
		return fmt.Errorf("%s: must contain a valid bearer token", name)
	}
	return nil
}
func validateRoutedWriters(cfg Config) error {
	admin := [4]string{cfg.DatabaseURL, cfg.ShardURLs[0], cfg.ShardURLs[1], cfg.ShardURLs[2]}
	writers := [4]string{cfg.WriterURL, cfg.ShardWriterURLs[0], cfg.ShardWriterURLs[1], cfg.ShardWriterURLs[2]}
	names := [4]string{"BNPB_CANONICAL_WRITER_DATABASE_URL", "BNPB_SHARD_0_WRITER_DATABASE_URL", "BNPB_SHARD_1_WRITER_DATABASE_URL", "BNPB_SHARD_2_WRITER_DATABASE_URL"}
	for i, writer := range writers {
		if err := validateURL(names[i], writer, []string{"postgres", "postgresql"}); err != nil {
			return err
		}
		adminURL, _ := url.Parse(admin[i])
		writerURL, _ := url.Parse(writer)
		if !sameDatabaseTarget(adminURL, writerURL) {
			return fmt.Errorf("%s: must address the same database node, database, and schema as its migration URL", names[i])
		}
		if adminURL.User == nil || writerURL.User == nil || adminURL.User.Username() == writerURL.User.Username() {
			return fmt.Errorf("%s: must use credentials distinct from the migration administrator", names[i])
		}
		for prior := 0; prior < i; prior++ {
			priorURL, _ := url.Parse(admin[prior])
			if sameDatabaseTarget(adminURL, priorURL) {
				return fmt.Errorf("%s: routed databases must map to distinct database nodes and targets", names[i])
			}
		}
	}
	return nil
}

func sameDatabaseTarget(left, right *url.URL) bool {
	leftPort, rightPort := left.Port(), right.Port()
	if leftPort == "" {
		leftPort = "5432"
	}
	if rightPort == "" {
		rightPort = "5432"
	}
	if !strings.EqualFold(left.Hostname(), right.Hostname()) || leftPort != rightPort || left.EscapedPath() != right.EscapedPath() {
		return false
	}
	for _, schemaOption := range []string{"options", "search_path", "currentSchema", "schema"} {
		if left.Query().Get(schemaOption) != right.Query().Get(schemaOption) {
			return false
		}
	}
	return true
}
