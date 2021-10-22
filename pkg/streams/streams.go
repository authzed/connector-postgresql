package streams

// simple wrapper around IO for cobra commands
// inspired by / vendored from:
// https://github.com/kubernetes/kubernetes/blob/55e1d2f9a73a0c78fc0c266236c3e31261635b90/staging/src/k8s.io/cli-runtime/pkg/genericclioptions/io_options.go

import (
	"bytes"
	"io"
	"os"
)

// IO provides the standard names for iostreams.  This is useful for embedding and for unit testing.
// Inconsistent and different names make it hard to read and review code
type IO struct {
	// In think, os.Stdin
	In io.Reader
	// Out think, os.Stdout
	Out io.Writer
	// ErrOut think, os.Stderr
	ErrOut io.Writer
}

// NewTestIO returns a valid IO and in, out, errout buffers for unit tests
func NewTestIO() (IO, *bytes.Buffer, *bytes.Buffer, *bytes.Buffer) {
	in := &bytes.Buffer{}
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}

	return IO{
		In:     in,
		Out:    out,
		ErrOut: errOut,
	}, in, out, errOut
}

// NewStdIO returns a valid IO for stdin, stdout, stderr
func NewStdIO() IO {
	return IO{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}
}
