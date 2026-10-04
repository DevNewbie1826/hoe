package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCompleteReportsAndParity(t *testing.T) {
	for _, scenario := range []string{"complete", "hoe-failure", "gorilla-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			expected := filepath.Join(dir, "expected.json")
			if err := os.WriteFile(expected, []byte(`["1.1.1"]`), 0600); err != nil {
				t.Fatal(err)
			}
			index := make(map[string]map[string]result)
			for i, agent := range []string{"hoe/websocket", "gorilla-over-hoe", "gorilla-over-std"} {
				behavior := "OK"
				if scenario == "hoe-failure" && i == 0 || scenario == "gorilla-mismatch" && i == 1 {
					behavior = "FAILED"
				}
				entry := result{Behavior: behavior, BehaviorClose: "OK", ReportFile: []string{"hoe.json", "gorilla-hoe.json", "gorilla-std.json"}[i]}
				index[agent] = map[string]result{"1.1.1": entry}
				b, err := json.Marshal(entry)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(dir, entry.ReportFile), b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			b, err := json.Marshal(index)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "index.json"), b, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = Summarize(dir, expected)
			if (err == nil) != (scenario == "complete") {
				t.Fatalf("scenario=%s err=%v", scenario, err)
			}
		})
	}
}
