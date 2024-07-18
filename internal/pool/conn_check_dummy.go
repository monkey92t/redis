//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !solaris && !illumos

package pool

import (
	"net"
)

func connCheck(_ net.Conn) error {
	return nil
}
