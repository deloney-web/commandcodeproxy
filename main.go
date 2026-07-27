package main

import (
	"flag"
	"fmt"

	"github.com/deloney-web/commandcodeproxy/internal/proxy"
	"github.com/deloney-web/commandcodeproxy/internal/server"
)

const appVersion = "v1.0.8"
const debugLogging = false

func main() {
	port := flag.String("port", "", "Port to run the server on (default: 55990)")
	host := flag.String("host", "", "Host to bind to (default: 127.0.0.1)")
	apiKey := flag.String("api-key", "", "API key for CommandCode (optional, can also be set via Authorization header)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(versionText())
		return
	}

	proxy := proxy.NewProxy(*apiKey)
	proxy.Debug = debugLogging

	srv := server.NewServer(proxy)
	srv.SetPort(*port)
	srv.SetHost(*host)

	printStartupInfo(srv)

	srv.Start()
}

func versionText() string {
	return appVersion
}
func printStartupInfo(srv *server.Server) {
	fmt.Println("")
	fmt.Println("========================================")
	fmt.Println("  CommandCode Proxy Server")
	fmt.Println("========================================")
	fmt.Println("")
	fmt.Printf("  Version:     %s\n", versionText())
	fmt.Printf("  Host:        %s\n", srv.GetHost())
	fmt.Printf("  Port:        %s\n", srv.GetPort())
	fmt.Println("  Base URL:    https://api.commandcode.ai")
	fmt.Println("")
	fmt.Println("  Endpoints:")
	fmt.Println("    POST /v1/chat/completions  (OpenAI-compatible)")
	fmt.Println("    POST /chat/completions     (OpenAI-compatible alias)")
	fmt.Println("    POST /v1/responses         (OpenAI Responses-compatible)")
	fmt.Println("    GET  /v1/models            (list models)")
	fmt.Println("    GET  /health               (health check)")
	fmt.Println("")
	fmt.Printf("  Server running on http://%s:%s\n", srv.GetHost(), srv.GetPort())
	fmt.Println("")
	fmt.Println("  Press Ctrl+C to stop")
	fmt.Println("========================================")
}
