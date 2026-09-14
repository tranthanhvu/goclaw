package config

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestLoadMCPAllowedHosts(t *testing.T) {
	for _, tt := range []struct {
		name    string
		content string
		env     string
		want    []string
	}{
		{
			name: "missing config defaults to no trusted hosts",
		},
		{
			name: "environment splits trims and drops empty entries",
			env:  " localhost, , 127.0.0.1,\t10.18.231.2, ",
			want: []string{"localhost", "127.0.0.1", "10.18.231.2"},
		},
		{
			name:    "JSON5 config allows explicit hosts",
			content: `{"gateway":{"mcp_allowed_hosts":["mcp.internal","10.18.231.2",]}}`,
			want:    []string{"mcp.internal", "10.18.231.2"},
		},
		{
			name:    "environment replaces config hosts",
			content: `{"gateway":{"mcp_allowed_hosts":["mcp.internal"]}}`,
			env:     " localhost, 127.0.0.1 ",
			want:    []string{"localhost", "127.0.0.1"},
		},
		{
			name:    "empty environment preserves config hosts",
			content: `{"gateway":{"mcp_allowed_hosts":["mcp.internal"]}}`,
			want:    []string{"mcp.internal"},
		},
		{
			name:    "nonempty environment with no hosts clears config hosts",
			content: `{"gateway":{"mcp_allowed_hosts":["mcp.internal"]}}`,
			env:     " , \t, ",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOCLAW_MCP_ALLOWED_HOSTS", tt.env)
			path := filepath.Join(t.TempDir(), "config.json5")
			if tt.content != "" {
				if err := os.WriteFile(path, []byte(tt.content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !slices.Equal(cfg.Gateway.MCPAllowedHosts, tt.want) {
				t.Fatalf("MCPAllowedHosts = %v, want %v", cfg.Gateway.MCPAllowedHosts, tt.want)
			}
		})
	}
}
