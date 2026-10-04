package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRejectIncompleteReports(t *testing.T) {
	for _, scenario := range []string{"missing-index", "missing-detail", "missing-case", "wrong-case", "wrong-detail"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			expected := filepath.Join(dir, "expected.json")
			if err := os.WriteFile(expected, []byte(`["1.1.1","2.1.1"]`), 0600); err != nil {
				t.Fatal(err)
			}
			index := make(map[string]map[string]result)
			for _, agent := range []string{"hoe/websocket", "gorilla-over-hoe", "gorilla-over-std"} {
				index[agent] = make(map[string]result)
				for _, id := range []string{"1.1.1", "2.1.1"} {
					name := agent
					if agent == "hoe/websocket" {
						name = "hoe_websocket"
					}
					name += "_" + id + ".json"
					entry := result{Behavior: "OK", BehaviorClose: "OK", ReportFile: name}
					index[agent][id] = entry
					if scenario == "missing-detail" && agent == "hoe/websocket" && id == "2.1.1" {
						continue
					}
					detail := entry
					if scenario == "wrong-detail" && agent == "hoe/websocket" {
						detail.Behavior = "FAILED"
					}
					b, err := json.Marshal(detail)
					if err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if scenario == "missing-case" {
				delete(index["hoe/websocket"], "2.1.1")
			}
			if scenario == "wrong-case" {
				index["hoe/websocket"]["2.1.2"] = index["hoe/websocket"]["2.1.1"]
				delete(index["hoe/websocket"], "2.1.1")
			}
			if scenario != "missing-index" {
				b, err := json.Marshal(index)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(dir, "index.json"), b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Summarize(dir, expected); err == nil {
				t.Fatal("incomplete or inconsistent reports accepted")
			}
		})
	}
}
