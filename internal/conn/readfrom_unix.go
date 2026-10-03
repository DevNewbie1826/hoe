//go:build linux || darwin

package conn

import (
	"errors"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
)

type writeOnly struct{ io.Writer }

// ReadFrom retains the standard file-copy special path over the loop socket.
// The codec calls it only after sniffing and flushing an unchunked response.
func (c *Conn) ReadFrom(src io.Reader) (int64, error) {
	raw := src
	var limit *io.LimitedReader
	if lr, ok := src.(*io.LimitedReader); ok {
		limit = lr
		raw = lr.R
	}
	file := false
	if _, ok := raw.(*os.File); ok {
		file = true
	} else if typ := reflect.TypeOf(raw); typ != nil {
		file = typ.PkgPath() == "os" && typ.Name() == "fileWithoutWriteTo"
	}
	if !file {
		b := bufpool.Get(32 << 10)
		defer bufpool.Put(b)
		return io.CopyBuffer(writeOnly{c}, src, b)
	}
	f := raw.(interface {
		Stat() (os.FileInfo, error)
		Seek(int64, int) (int64, error)
		Fd() uintptr
	})
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil || !info.Mode().IsRegular() {
		b := bufpool.Get(32 << 10)
		defer bufpool.Put(b)
		return io.CopyBuffer(writeOnly{c}, src, b)
	}
	left := max(int64(0), info.Size()-offset)
	if limit != nil {
		left = min(left, limit.N)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	var total int64
	interest := false
	defer func() {
		if interest {
			c.raw.SetInterest(true, false)
		}
	}()
	for left > 0 {
		t, _ := c.deadline("write")
		if !t.IsZero() && !time.Now().Before(t) {
			err = c.timeoutError("write")
			break
		}
		ready := c.raw.WriteReady()
		var n int
		n, err = c.raw.Sendfile(int(f.Fd()), &offset, int(min(left, 1<<20)))
		total += int64(n)
		left -= int64(n)
		if interrupted(err) {
			continue
		}
		if wouldBlock(err) {
			if !interest {
				err = c.raw.SetInterest(true, true)
				if err != nil {
					break
				}
				interest = true
			}
			err = c.wait("write", ready)
			if err != nil {
				break
			}
			continue
		}
		if err != nil || n == 0 {
			break
		}
	}
	if limit != nil {
		limit.N -= total
	}
	_, seekErr := f.Seek(offset, io.SeekStart)
	return total, errors.Join(err, seekErr)
}
