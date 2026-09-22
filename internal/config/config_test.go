package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const validConfig = `{
  "datasets": [
    {"name":"d","path":"d.csv","format":"csv","schema":{"fields":[
      {"name":"id","type":"int","required":true},
      {"name":"ts","type":"time","format":"2006-01-02"}
    ]}}
  ],
  "rules": [
    {"id":"r1","dataset":"d","kind":"uniqueness","severity":"error","field":"id"}
  ]
}`

func TestLoadValidAndDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Rules[0].Severity != model.SeverityError {
		t.Fatalf("severity default: %s", cfg.Rules[0].Severity)
	}
	if !filepath.IsAbs(cfg.Datasets[0].Path) {
		t.Fatalf("relative dataset path must resolve to absolute, got %s", cfg.Datasets[0].Path)
	}
}

func TestInvalidDeclarations(t *testing.T) {
	cases := map[string]string{
		"unknown dataset in rule": `{
		  "datasets":[{"name":"d","path":"d.csv","format":"csv","schema":{"fields":[{"name":"id","type":"int"}]}}],
		  "rules":[{"id":"r","dataset":"ghost","kind":"uniqueness","field":"id"}]}`,
		"unknown field in rule": `{
		  "datasets":[{"name":"d","path":"d.csv","format":"csv","schema":{"fields":[{"name":"id","type":"int"}]}}],
		  "rules":[{"id":"r","dataset":"d","kind":"uniqueness","field":"nope"}]}`,
		"unknown kind": `{
		  "datasets":[{"name":"d","path":"d.csv","format":"csv","schema":{"fields":[{"name":"id","type":"int"}]}}],
		  "rules":[{"id":"r","dataset":"d","kind":"magic","field":"id"}]}`,
		"bad format": `{
		  "datasets":[{"name":"d","path":"d.xml","format":"xml","schema":{"fields":[{"name":"id","type":"int"}]}}],
		  "rules":[{"id":"r","dataset":"d","kind":"uniqueness","field":"id"}]}`,
		"bad type": `{
		  "datasets":[{"name":"d","path":"d.csv","format":"csv","schema":{"fields":[{"name":"id","type":"bigint"}]}}],
		  "rules":[{"id":"r","dataset":"d","kind":"uniqueness","field":"id"}]}`,
		"time missing format": `{
		  "datasets":[{"name":"d","path":"d.csv","format":"csv","schema":{"fields":[{"name":"ts","type":"time"}]}}],
		  "rules":[{"id":"r","dataset":"d","kind":"freshness","field":"ts"}]}`,
		"duplicate rule id": `{
		  "datasets":[{"name":"d","path":"d.csv","format":"csv","schema":{"fields":[{"name":"id","type":"int"}]}}],
		  "rules":[
		    {"id":"r","dataset":"d","kind":"uniqueness","field":"id"},
		    {"id":"r","dataset":"d","kind":"completeness","field":"id"}]}`,
		"duplicate dataset": `{
		  "datasets":[
		    {"name":"d","path":"a.csv","format":"csv","schema":{"fields":[{"name":"id","type":"int"}]}},
		    {"name":"d","path":"b.csv","format":"csv","schema":{"fields":[{"name":"id","type":"int"}]}}],
		  "rules":[{"id":"r","dataset":"d","kind":"uniqueness","field":"id"}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, body)); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected error for missing config")
	}
}
