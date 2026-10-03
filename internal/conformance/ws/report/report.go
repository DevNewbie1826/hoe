// Package report validates complete Autobahn runs and compares target counts.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

type result struct {
	Behavior      string `json:"behavior"`
	BehaviorClose string `json:"behaviorClose"`
	ReportFile    string `json:"reportfile"`
}

// Summarize validates exact executed case sets and all per-case JSON reports,
// then requires zero hoe failures and equal gorilla behavior counts.
func Summarize(dir, expectedFile string) (string, error) {
	b, err := os.ReadFile(expectedFile)
	if err != nil {
		return "", err
	}
	var expected []string
	if err = json.Unmarshal(b, &expected); err != nil {
		return "", err
	}
	if len(expected) == 0 {
		return "", fmt.Errorf("empty expected case set")
	}
	sort.Strings(expected)
	for i, id := range expected {
		if i > 0 && id == expected[i-1] {
			return "", fmt.Errorf("duplicate expected case %s", id)
		}
	}
	b, err = os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return "", err
	}
	var index map[string]map[string]result
	if err = json.Unmarshal(b, &index); err != nil {
		return "", err
	}
	agents := []string{"hoe/websocket", "gorilla-over-hoe", "gorilla-over-std"}
	if len(index) != len(agents) {
		return "", fmt.Errorf("expected %d agents, got %d", len(agents), len(index))
	}
	allCounts := make(map[string]map[string]int)
	var text strings.Builder
	for _, agent := range agents {
		cases, ok := index[agent]
		if !ok {
			return "", fmt.Errorf("missing agent %s", agent)
		}
		actual := make([]string, 0, len(cases))
		for id := range cases {
			actual = append(actual, id)
		}
		sort.Strings(actual)
		if !reflect.DeepEqual(actual, expected) {
			return "", fmt.Errorf("%s executed case set mismatch: got %d expected %d", agent, len(actual), len(expected))
		}
		counts := make(map[string]int)
		for _, id := range expected {
			entry := cases[id]
			switch entry.Behavior {
			case "OK", "NON-STRICT", "INFORMATIONAL", "UNIMPLEMENTED", "FAILED":
			default:
				return "", fmt.Errorf("%s %s missing/unknown behavior %q", agent, id, entry.Behavior)
			}
			if entry.ReportFile == "" {
				return "", fmt.Errorf("%s %s missing reportfile", agent, id)
			}
			name := entry.ReportFile
			if filepath.Base(name) != name || filepath.Ext(name) != ".json" {
				return "", fmt.Errorf("invalid report filename %s", name)
			}
			raw, e := os.ReadFile(filepath.Join(dir, name))
			if e != nil {
				return "", fmt.Errorf("%s %s: %w", agent, id, e)
			}
			var detail result
			if e = json.Unmarshal(raw, &detail); e != nil {
				return "", e
			}
			if detail.Behavior != entry.Behavior || detail.BehaviorClose != entry.BehaviorClose {
				return "", fmt.Errorf("%s %s report/index mismatch", agent, id)
			}
			counts[entry.Behavior]++
		}
		allCounts[agent] = counts
		fmt.Fprintf(&text, "%s TOTAL=%d OK=%d NON-STRICT=%d INFORMATIONAL=%d UNIMPLEMENTED=%d FAILED=%d\n", agent, len(expected), counts["OK"], counts["NON-STRICT"], counts["INFORMATIONAL"], counts["UNIMPLEMENTED"], counts["FAILED"])
	}
	if allCounts[agents[0]]["FAILED"] != 0 {
		return text.String(), fmt.Errorf("hoe/websocket has failed cases")
	}
	if !reflect.DeepEqual(allCounts[agents[1]], allCounts[agents[2]]) {
		return text.String(), fmt.Errorf("gorilla behavior counts differ")
	}
	return text.String(), nil
}
