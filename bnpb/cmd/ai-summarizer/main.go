package main

import (
	"os"

	aisummarizer "github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/ai-summarizer"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

func main() {
	os.Exit(service.BootstrapApplication("bnpb-ai-summarizer", "BNPB_AI_SUMMARIZER_HTTP_ADDR", ":8090", aisummarizer.Initialize))
}
