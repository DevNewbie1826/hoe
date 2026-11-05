//go:build linux || darwin

package conn

import (
	"testing"
	"unsafe"
)

// seamSpanBytes is the occupied in-struct span of the reader-management
// seam's six fields on 64-bit platforms: from the first seam field
// (readerDemand) to the end of the last (smallReaderReleases). It includes the
// 7 bytes of alignment padding the seam forces before smallReaderLoans.
//
// Field-by-field sizes on linux/arm64 (go1.27.3) and offsets in the real Conn:
//
//	readerDemand         atomic.Bool  4 @100-104
//	readerBytes          int          8 @104-112
//	bodyReadMax          int          8 @112-120
//	bodyReadsOn          bool         1 @120-121
//	(padding)                         7 @121-128
//	smallReaderLoans     int          8 @128-136
//	smallReaderReleases  int          8 @136-144
//
// Raw sum of the field sizes is 37 bytes; the contiguous span they occupy
// is 144-100 = 44 bytes. The net size increase over the pre-seam layout is
// 40 bytes: readerDemand fills four bytes of alignment padding that existed
// before the reader work (three preceding bools, then padding to headStart's
// 8-byte alignment), so only the five counter fields grow Conn.
const (
	seamSpanBytes   = 44
	seamGrowthBytes = 40
)

// TestConnReaderSeamSize pins the reader-management seam's exact per-Conn
// size. It anchors on the seam's own fields rather than unsafe.Sizeof(Conn{}),
// so field additions elsewhere in Conn do not invalidate it. seamGrowthBytes
// is the number to quote as the seam's per-Conn cost; seamSpanBytes is the
// occupied span it is derived from.
func TestConnReaderSeamSize(t *testing.T) {
	start := unsafe.Offsetof(Conn{}.readerDemand)
	end := unsafe.Offsetof(Conn{}.smallReaderReleases) + unsafe.Sizeof(Conn{}.smallReaderReleases)
	if span := end - start; span != seamSpanBytes {
		t.Fatalf("reader seam span = %d bytes (offset %d..%d), want %d", span, start, end, seamSpanBytes)
	}
	growth := unsafe.Offsetof(Conn{}.headStart) - end
	if growth != 0 {
		t.Fatalf("unexpected %d bytes between the seam's last field and headStart; the span anchor drifted", growth)
	}
	net := unsafe.Offsetof(Conn{}.headStart) - (start + unsafe.Sizeof(Conn{}.readerDemand))
	if net != seamGrowthBytes {
		t.Fatalf("reader seam net per-Conn growth = %d bytes, want %d", net, seamGrowthBytes)
	}

	// Pin the field sizes the span arithmetic above is derived from, so a
	// type change that happens to preserve the span is still visible.
	sizes := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"readerDemand", unsafe.Sizeof(Conn{}.readerDemand), 4},
		{"readerBytes", unsafe.Sizeof(Conn{}.readerBytes), 8},
		{"bodyReadMax", unsafe.Sizeof(Conn{}.bodyReadMax), 8},
		{"bodyReadsOn", unsafe.Sizeof(Conn{}.bodyReadsOn), 1},
		{"smallReaderLoans", unsafe.Sizeof(Conn{}.smallReaderLoans), 8},
		{"smallReaderReleases", unsafe.Sizeof(Conn{}.smallReaderReleases), 8},
	}
	for _, f := range sizes {
		if f.got != f.want {
			t.Errorf("Conn.%s size = %d bytes, want %d", f.name, f.got, f.want)
		}
	}

	t.Logf("reader seam occupies a 44-byte span (%d..%d), net per-Conn growth %d bytes, raw field sum 37 bytes", start, end, net)
}
