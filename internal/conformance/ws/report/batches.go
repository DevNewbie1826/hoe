package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type specification struct {
	Outdir  string            `json:"outdir"`
	Servers []json.RawMessage `json:"servers"`
	Cases   []string          `json:"cases"`
}

// Prepare partitions the exact case set into bounded clients. Autobahn retains
// every wire log until exit; independent clients prevent cumulative report
// memory from overwhelming small local Docker VMs. No case is excluded.
func Prepare(specFile, expectedFile, dir string) error {
	raw, err := os.ReadFile(specFile)
	if err != nil {
		return err
	}
	var spec specification
	if err = json.Unmarshal(raw, &spec); err != nil {
		return err
	}
	raw, err = os.ReadFile(expectedFile)
	if err != nil {
		return err
	}
	var cases []string
	if err = json.Unmarshal(raw, &cases); err != nil {
		return err
	}
	if len(cases) == 0 || len(spec.Servers) != 3 {
		return fmt.Errorf("invalid Autobahn inventory")
	}
	sort.Strings(cases)
	if err = os.Mkdir(dir, 0700); err != nil {
		return err
	}
	var manifest strings.Builder
	for agent, server := range spec.Servers {
		for start := 0; start < len(cases); start += 8 {
			batch := filepath.Join(dir, fmt.Sprintf("agent-%d-batch-%02d", agent, start/8))
			if err = os.MkdirAll(filepath.Join(batch, "reports"), 0700); err != nil {
				return err
			}
			s := specification{Outdir: "/reports", Servers: []json.RawMessage{server}, Cases: cases[start:min(start+8, len(cases))]}
			b, err := json.Marshal(s)
			if err != nil {
				return err
			}
			if err = os.WriteFile(filepath.Join(batch, "spec.json"), b, 0600); err != nil {
				return err
			}
			fmt.Fprintln(&manifest, batch)
		}
	}
	return os.WriteFile(filepath.Join(dir, "manifest.txt"), []byte(manifest.String()), 0600)
}

// Merge collects complete batch reports. Missing indices, details and duplicate
// cases fail before writing the aggregate index. Summarize then checks the full
// expected inventory and gorilla parity.
func Merge(batches, dir string) error {
	raw, err := os.ReadFile(filepath.Join(batches, "manifest.txt"))
	if err != nil {
		return err
	}
	index := make(map[string]map[string]result)
	for _, batch := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if batch == "" {
			return fmt.Errorf("empty batch manifest")
		}
		raw, err = os.ReadFile(filepath.Join(batch, "reports", "index.json"))
		if err != nil {
			return err
		}
		var partial map[string]map[string]result
		if err = json.Unmarshal(raw, &partial); err != nil {
			return err
		}
		for agent, cases := range partial {
			if index[agent] == nil {
				index[agent] = make(map[string]result)
			}
			for id, entry := range cases {
				if _, exists := index[agent][id]; exists {
					return fmt.Errorf("duplicate %s %s", agent, id)
				}
				name := entry.ReportFile
				if filepath.Base(name) != name || filepath.Ext(name) != ".json" {
					return fmt.Errorf("invalid report filename %q", name)
				}
				detail, err := os.ReadFile(filepath.Join(batch, "reports", name))
				if err != nil {
					return err
				}
				if err = os.WriteFile(filepath.Join(dir, name), detail, 0600); err != nil {
					return err
				}
				index[agent][id] = entry
			}
		}
	}
	raw, err = json.Marshal(index)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "index.json"), raw, 0600)
}
