package aggregator

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAuthorityConfigOwnerDomain(t *testing.T) {
	t.Parallel()
	const (
		domainA = "aaaaaaaa-0000-4000-8000-000000000001"
		domainB = "bbbbbbbb-0000-4000-8000-000000000002"
		mapping = `{"worker-1":"` + domainA + `","worker-2":"` + domainB + `"}`
		node    = "BNPB_K8S_NODE_NAME"
		domains = "BNPB_AUTHORITY_NODE_DOMAINS"
		literal = "BNPB_AUTHORITY_OWNER_DOMAIN_UUID"
	)
	baseline := authorityConfigFixture(t, domainA+","+domainB)
	for _, test := range []struct {
		name    string
		env     map[string]string
		want    string
		wantErr string
	}{
		{name: "scheduled node", env: map[string]string{node: "worker-2", domains: mapping}, want: domainB},
		{name: "local literal", env: map[string]string{literal: domainA}, want: domainA},
		{name: "matching literal", env: map[string]string{node: "worker-2", domains: mapping, literal: strings.ToUpper(domainB)}, want: domainB},
		{name: "conflicting literal", env: map[string]string{node: "worker-2", domains: mapping, literal: domainA}, wantErr: "conflicts"},
		{name: "missing map does not use literal", env: map[string]string{node: "worker-2", literal: domainA}, wantErr: domains},
		{name: "missing node", env: map[string]string{domains: mapping}, wantErr: node},
		{name: "unknown node", env: map[string]string{node: "worker-3", domains: mapping}, wantErr: "no mapping"},
		{name: "invalid node key", env: map[string]string{node: "worker-2", domains: `{"worker_1":"` + domainA + `","worker-2":"` + domainB + `"}`}, wantErr: "invalid node name"},
		{name: "invalid UUID", env: map[string]string{node: "worker-2", domains: `{"worker-1":null,"worker-2":"` + domainB + `"}`}, wantErr: "invalid domain UUID"},
		{name: "duplicate node key", env: map[string]string{node: "worker-2", domains: `{"worker-2":"` + domainA + `","worker-\u0032":"` + domainB + `"}`}, wantErr: "duplicate node name"},
		{name: "duplicate UUID", env: map[string]string{node: "worker-2", domains: `{"worker-1":"` + strings.ToUpper(domainB) + `","worker-2":"` + domainB + `"}`}, wantErr: "duplicate domain UUID"},
		{name: "wrong JSON type", env: map[string]string{node: "worker-2", domains: `[]`}, wantErr: domains},
		{name: "trailing JSON", env: map[string]string{node: "worker-2", domains: mapping + ` {}`}, wantErr: domains},
		{name: "mapped UUID not allowed", env: map[string]string{node: "worker-2", domains: mapping, "BNPB_AUTHORITY_DOMAINS": domainA}, wantErr: "must be included in BNPB_AUTHORITY_DOMAINS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := loadAuthorityConfig(func(name string) (string, bool) {
				if setting, ok := test.env[name]; ok {
					return setting, true
				}
				setting, ok := baseline[name]
				return setting, ok
			})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("loadAuthorityConfig error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || cfg.OwnerDomain != test.want {
				t.Fatalf("loadAuthorityConfig owner domain = %q, error = %v; want %q", cfg.OwnerDomain, err, test.want)
			}
		})
	}
}

func authorityConfigFixture(t *testing.T, domains string) map[string]string {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"BNPB_AUTHORITY_KUBERNETES_SERVER_URL": "https://kubernetes.example",
		"BNPB_AUTHORITY_KUBERNETES_NAMESPACE":  "test",
		"BNPB_AUTHORITY_KUBERNETES_CONFIGMAP":  "authority",
		"BNPB_AUTHORITY_KUBERNETES_TOKEN_FILE": tokenFile,
		"BNPB_AUTHORITY_KUBERNETES_CA_FILE":    caFile,
		"BNPB_AUTHORITY_HOST_BASE_URL":         "https://fence.example",
		"BNPB_AUTHORITY_HOST_TOKEN_FILE":       tokenFile,
		"BNPB_AUTHORITY_HOST_CA_FILE":          caFile,
		"BNPB_AUTHORITY_DOMAINS":               domains,
	}
}
