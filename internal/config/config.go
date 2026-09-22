// Package config loads declarative dataset and rule declarations.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// Config is the full declarative configuration.
type Config struct {
	Datasets []model.DatasetDecl `json:"datasets"`
	Rules    []model.Rule        `json:"rules"`
}

// Load reads and validates a JSON configuration file. Relative dataset paths
// resolve against the directory containing the config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base := filepath.Dir(path)
	for i := range cfg.Datasets {
		d := &cfg.Datasets[i]
		if !filepath.IsAbs(d.Path) {
			d.Path = filepath.Clean(filepath.Join(base, d.Path))
		}
	}
	return &cfg, nil
}

// Validate applies structural validation so an invalid declaration never runs.
func (c *Config) Validate() error {
	if len(c.Datasets) == 0 {
		return fmt.Errorf("config: at least one dataset is required")
	}
	dsNames := make(map[string]int, len(c.Datasets))
	fieldsByDataset := make(map[string]map[string]model.FieldType, len(c.Datasets))
	for i, d := range c.Datasets {
		if d.Name == "" {
			return fmt.Errorf("config: datasets[%d] has empty name", i)
		}
		if prev, ok := dsNames[d.Name]; ok {
			return fmt.Errorf("config: dataset %q declared more than once (indexes %d and %d)", d.Name, prev, i)
		}
		dsNames[d.Name] = i
		if d.Path == "" {
			return fmt.Errorf("config: dataset %q has empty path", d.Name)
		}
		if d.Format != "csv" && d.Format != "jsonl" {
			return fmt.Errorf("config: dataset %q has unsupported format %q (want csv|jsonl)", d.Name, d.Format)
		}
		if len(d.Schema.Fields) == 0 {
			return fmt.Errorf("config: dataset %q declares no fields", d.Name)
		}
		fields := make(map[string]model.FieldType, len(d.Schema.Fields))
		for _, f := range d.Schema.Fields {
			if f.Name == "" {
				return fmt.Errorf("config: dataset %q has a field with empty name", d.Name)
			}
			if _, dup := fields[f.Name]; dup {
				return fmt.Errorf("config: dataset %q declares field %q more than once", d.Name, f.Name)
			}
			if !validFieldType(f.Type) {
				return fmt.Errorf("config: dataset %q field %q has unsupported type %q", d.Name, f.Name, f.Type)
			}
			if f.Type == model.TypeTime && f.Format == "" {
				return fmt.Errorf("config: dataset %q time field %q requires a format", d.Name, f.Name)
			}
			fields[f.Name] = f.Type
		}
		fieldsByDataset[d.Name] = fields
	}
	if len(c.Rules) == 0 {
		return fmt.Errorf("config: at least one rule is required")
	}
	ruleIDs := make(map[string]int, len(c.Rules))
	for i, r := range c.Rules {
		if r.ID == "" {
			return fmt.Errorf("config: rules[%d] has empty id", i)
		}
		if prev, ok := ruleIDs[r.ID]; ok {
			return fmt.Errorf("config: rule %q declared more than once (indexes %d and %d)", r.ID, prev, i)
		}
		ruleIDs[r.ID] = i
		if _, ok := dsNames[r.Dataset]; !ok {
			return fmt.Errorf("config: rule %q references unknown dataset %q", r.ID, r.Dataset)
		}
		switch r.Kind {
		case model.KindCompleteness, model.KindUniqueness, model.KindConsistency, model.KindFreshness:
		default:
			return fmt.Errorf("config: rule %q has unknown kind %q", r.ID, r.Kind)
		}
		if r.Severity == "" {
			c.Rules[i].Severity = model.SeverityError
		} else if r.Severity != model.SeverityError && r.Severity != model.SeverityWarning {
			return fmt.Errorf("config: rule %q has unsupported severity %q", r.ID, r.Severity)
		}
		targets := r.Fields
		if r.Field != "" {
			targets = append(targets, r.Field)
		}
		for _, fname := range targets {
			if _, ok := fieldsByDataset[r.Dataset][fname]; !ok {
				return fmt.Errorf("config: rule %q references field %q absent from dataset %q schema", r.ID, fname, r.Dataset)
			}
		}
	}
	return nil
}

func validFieldType(t model.FieldType) bool {
	switch t {
	case model.TypeString, model.TypeInt, model.TypeFloat, model.TypeBool, model.TypeTime:
		return true
	}
	return false
}
