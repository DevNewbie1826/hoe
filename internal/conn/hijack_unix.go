//go:build linux || darwin

package conn

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
)

type logWriter struct{}

func (logWriter) Write(p []byte) (int, error) { log.Print(string(p)); return len(p), nil }

// Hijack deregisters before net.FileConn duplicates the open file description.
// Returned reader bytes precede socket bytes and remain Buffered like net/http.
func (c *Conn) Hijack(in *bufio.Reader) (net.Conn, *bufio.ReadWriter, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	var pending []byte
	if n := in.Buffered(); n > 0 {
		p, _ := in.Peek(n)
		pending = append(pending, p...)
	}
	pending = append(pending, c.input[c.offset:]...)
	fd, err := c.raw.Detach()
	if err != nil {
		return nil, nil, err
	}
	c.releaseInputLocked()
	c.stopDeadlines()
	f := os.NewFile(uintptr(fd), "hoe-hijack")
	nc, err := net.FileConn(f)
	closeErr := f.Close()
	if err != nil {
		c.setState(http.StateClosed)
		c.engine.forget(c)
		return nil, nil, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		nc.Close()
		c.setState(http.StateClosed)
		c.engine.forget(c)
		return nil, nil, closeErr
	}
	c.raw.RelinquishWorker()
	c.setState(http.StateHijacked)
	c.engine.forget(c)
	var source io.Reader = nc
	if len(pending) > 0 {
		source = io.MultiReader(bytes.NewReader(pending), nc)
	}
	br := bufio.NewReaderSize(source, max(4096, len(pending)))
	if len(pending) > 0 {
		if _, err = br.Peek(len(pending)); err != nil {
			nc.Close()
			return nil, nil, err
		}
	}
	return nc, bufio.NewReadWriter(br, bufio.NewWriter(nc)), nil
}
