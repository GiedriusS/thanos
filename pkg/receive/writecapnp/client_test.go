package writecapnp

import (
	"net"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestTCPPool(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, l.Close())
	})

	pool, err := NewTCPPool(1, 1, func() (any, error) {
		addr, err := net.ResolveTCPAddr("tcp", l.Addr().String())
		if err != nil {
			return nil, err
		}
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to dial peer %s", l.Addr().String())
		}
		return conn, nil
	})
	require.NoError(t, err)

	conn, err := pool.Get()
	require.NoError(t, err)
	pool.Put(conn)

	conn, err = pool.Get()
	require.NoError(t, err)

	var _ = conn
	var _ = err

	require.NoError(t, pool.Destroy())
}
