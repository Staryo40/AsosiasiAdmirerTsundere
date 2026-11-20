package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/placementcompare"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "placement-compare: %s\n", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("placement-compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "", "path to raw seismic-event JSONL catalogue (required)")
	resourcesPath := flags.String("resources", "", "path to resource/topology manifest (required)")
	burstArea := flags.String("burst-area", "", "optional exact region_name to analyze as a same-region burst subset")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("invalid command-line arguments")
	}
	if flags.NArg() != 0 || *inputPath == "" || *resourcesPath == "" {
		return fmt.Errorf("-input and -resources are required; positional arguments are not accepted")
	}
	input, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input catalogue")
	}
	defer input.Close()
	resources, err := os.ReadFile(*resourcesPath)
	if err != nil || len(resources) == 0 {
		return fmt.Errorf("cannot read non-empty resource/topology manifest")
	}
	result, err := placementcompare.CompareJSONL(input, resources, *burstArea)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("cannot encode comparison result")
	}
	return nil
}
