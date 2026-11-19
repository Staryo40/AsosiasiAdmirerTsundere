package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/catalogload"
)

func main() {
	var options catalogload.Options
	flag.StringVar(&options.Artifact, "artifact", "", "JSONL or JSON-array seismic artifact path")
	flag.StringVar(&options.BaseURL, "base-url", "", "Aggregator base URL")
	flag.StringVar(&options.TokenFile, "token-file", "", "dedicated catalog Bearer token file")
	flag.StringVar(&options.AuditTokenFile, "audit-token-file", "", "dedicated Audit Bearer token file")
	flag.StringVar(&options.ExpectedChecksum, "expected-checksum", "", "expected complete post-load BMKG Audit SHA-256")
	flag.StringVar(&options.ResourcesManifest, "resources-manifest", "", "JSON manifest containing reference_checksum and measured resources")
	flag.StringVar(&options.RunOutput, "run-output", "", "JSON run report path")
	flag.Parse()
	report, err := catalogload.Run(context.Background(), options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("loaded %d records; artifact sha256=%s; audit sha256=%s\n", report.Rows, report.ArtifactSHA256, report.AuditAfter.Checksum)
}
