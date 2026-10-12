package errutil

import (
	"errors"
	"io"
	"testing"
)

func TestWrappingPreservesCause(test *testing.T) {
	wrapped := Wrapf(Wrap(io.EOF, "read"), "stream %d", 7)
	if !errors.Is(wrapped, io.EOF) || Cause(wrapped) != io.EOF {
		test.Fatalf("lost error identity: %v", wrapped)
	}
	if wrapped.Error() != "stream 7: read: EOF" {
		test.Fatalf("unexpected context: %v", wrapped)
	}
	if Wrap(nil, "read") != nil || Wrapf(nil, "%s", "read") != nil || Cause(nil) != nil {
		test.Fatal("nil errors must remain nil")
	}
}
