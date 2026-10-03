//go:build !linux && !darwin

package sys

// Read reports an unsupported syscall.
func Read(int, []byte) (int, error) { return 0, ErrUnsupported }

// Write reports an unsupported syscall.
func Write(int, []byte) (int, error) { return 0, ErrUnsupported }

// Writev reports an unsupported syscall.
func Writev(int, [][]byte) (int, error) { return 0, ErrUnsupported }

// Sendfile reports an unsupported syscall.
func Sendfile(int, int, *int64, int) (int, error) { return 0, ErrUnsupported }

// Splice reports an unsupported syscall.
func Splice(int, int, int) (int, error) { return 0, ErrUnsupported }

// SplicePipe reports an unsupported syscall.
func SplicePipe() ([2]int, error) { return [2]int{}, ErrUnsupported }
