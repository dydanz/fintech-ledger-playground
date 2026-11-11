package console

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
)

// In compose a stopped upstream's name stops resolving: that's "refused"
// (can't connect), not "unknown".
func TestCallReason(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{&net.DNSError{Err: "no such host", Name: "api", IsNotFound: true}, "refused"},
		{fmt.Errorf("dial: %w", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), "refused"},
		{fmt.Errorf("get: %w", context.DeadlineExceeded), "timeout"},
		{&net.DNSError{Err: "i/o timeout", Name: "api", IsTimeout: true}, "timeout"},
		{errors.New("tls: handshake failure"), "unknown"},
	} {
		if got := callReason(c.err); got != c.want {
			t.Errorf("callReason(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
