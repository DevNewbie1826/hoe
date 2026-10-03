package websocket

import "testing"

func TestTextValidationAcrossChunks(t *testing.T) {
	cases := []struct {
		name   string
		chunks [][]byte
		valid  bool
	}{
		{"adjacent-runes-after-partial", [][]byte{{0xe2}, {0x82, 0xac, 0xe2, 0x82, 0xac}}, true},
		{"byte-at-a-time", [][]byte{{0xe2}, {0x82}, {0xac}}, true},
		{"ascii-after-partial", [][]byte{{0xe2}, {0x82, 0xac, 'a', 'b'}}, true},
		{"invalid-continuation", [][]byte{{0xe2}, {'x'}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var parser frameParser
			valid := true
			for _, p := range tc.chunks {
				if !parser.validText(p) {
					valid = false
					break
				}
			}
			if valid != tc.valid {
				t.Fatalf("valid=%t want=%t", valid, tc.valid)
			}
			if valid && len(parser.textTail) != 0 {
				t.Fatalf("unconsumed text %x", parser.textTail)
			}
		})
	}
}
