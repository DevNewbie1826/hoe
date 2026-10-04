//go:build linux || darwin

package conn

import (
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"runtime"
	"syscall"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	"github.com/DevNewbie1826/hoe/internal/sys"
)

type writeOnly struct{ io.Writer }

// ReadFrom retains the standard file-copy special path over the loop socket.
// The codec calls it only after sniffing and flushing an unchunked response.
func (c *Conn) ReadFrom(src io.Reader) (int64, error) {
	if err := c.PushPipeline(); err != nil {
		return 0, err
	}
	raw := src
	var limit *io.LimitedReader
	if lr, ok := src.(*io.LimitedReader); ok {
		limit = lr
		raw = lr.R
	}
	if runtime.GOOS == "linux" {
		tcp := false
		if _, ok := raw.(*net.TCPConn); ok {
			tcp = true
		} else if typ := reflect.TypeOf(raw); typ != nil {
			tcp = typ.PkgPath() == "net" && typ.Name() == "tcpConnWithoutWriteTo"
		}
		if tcp {
			return c.spliceFrom(raw.(syscall.Conn), limit)
		}
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

func (c *Conn) spliceFrom(src syscall.Conn, limit *io.LimitedReader) (total int64, err error) {
	left := int64(1<<63 - 1)
	if limit != nil {
		left = limit.N
		defer func() { limit.N -= total }()
	}
	if left <= 0 {
		return 0, nil
	}
	source, err := src.SyscallConn()
	if err != nil {
		return 0, err
	}
	pipe, err := sys.SplicePipe()
	if err != nil {
		return 0, err
	}
	defer syscall.Close(pipe[0])
	defer syscall.Close(pipe[1])
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	interest := false
	defer func() {
		if interest {
			c.raw.SetInterest(true, false)
		}
	}()
	for left > 0 {
		var count int
		var readErr error
		// RawConn arms runtime readiness before retrying an EAGAIN read;
		// it also keeps the source descriptor alive until this call ends.
		err = source.Read(func(fd uintptr) bool {
			for {
				count, readErr = sys.Splice(pipe[1], int(fd), int(min(left, 32<<10)))
				if interrupted(readErr) {
					continue
				}
				return !wouldBlock(readErr)
			}
		})
		if err != nil || readErr != nil {
			return total, errors.Join(err, readErr)
		}
		if count == 0 {
			return total, nil
		}
		for count > 0 {
			t, _ := c.deadline("write")
			if !t.IsZero() && !time.Now().Before(t) {
				return total, c.timeoutError("write")
			}
			ready := c.raw.WriteReady()
			n, err := c.raw.Splice(pipe[0], count)
			total += int64(n)
			left -= int64(n)
			count -= n
			if interrupted(err) {
				continue
			}
			if wouldBlock(err) {
				if !interest {
					if err := c.raw.SetInterest(true, true); err != nil {
						return total, err
					}
					interest = true
				}
				if err := c.wait("write", ready); err != nil {
					return total, err
				}
				continue
			}
			if err != nil {
				return total, err
			}
			if n == 0 {
				return total, io.ErrNoProgress
			}
		}
	}
	return total, nil
}
