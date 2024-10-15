// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"context"
	"fmt"
	"net"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// TCPPool common connection pool. Code is inspired by https://github.com/go-baa/pool.
type TCPPool struct {
	// New creates a new connection.
	New func() (any, error)
	// Ping checks that the connection is ok.
	Ping func(any) bool
	// Close closes the connection.
	Close func(any) error
	store chan any
	mu    sync.Mutex
}

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")
)

// NewTCPPool create a pool with capacity
func NewTCPPool(initCap, maxCap int, newFunc func() (any, error)) (*TCPPool, error) {
	if maxCap == 0 || initCap > maxCap {
		return nil, fmt.Errorf("invalid capacity settings")
	}
	p := new(TCPPool)
	p.store = make(chan interface{}, maxCap)
	if newFunc != nil {
		p.New = newFunc
	}
	for i := 0; i < initCap; i++ {
		v, err := p.create()
		if err != nil {
			return p, err
		}
		p.store <- v
	}
	return p, nil
}

// Len returns current connections in pool
func (p *TCPPool) Len() int {
	return len(p.store)
}

// Get returns a conn form store or create one
func (p *TCPPool) Get() (interface{}, error) {
	if p.store == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case v := <-p.store:
			if p.Ping != nil && !p.Ping(v) {
				continue
			}
			return v, nil
		default:
			return p.create()
		}
	}
}

// Put set back conn into store again.
func (p *TCPPool) Put(v interface{}) {
	select {
	case p.store <- v:
		return
	default:
		// pool is full, close passed connection
		if p.Close != nil {
			p.Close(v)
		}
		return
	}
}

// Destroy clear all connections
func (p *TCPPool) Destroy() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil {
		return nil
	}
	var errs []error
	close(p.store)
	for v := range p.store {
		if p.Close != nil {
			if err := p.Close(v); err != nil {
				errs = append(errs, err)
			}
		}
	}
	p.store = nil

	if len(errs) > 0 {
		return errors.Errorf("close errors: %v", errs)
	}
	return nil
}

func (p *TCPPool) create() (any, error) {
	if p.New == nil {
		return nil, fmt.Errorf("Pool.New is nil, can not create connection")
	}
	return p.New()
}

type Dialer interface {
	Dial() (net.Conn, error)
}

type TCPDialer struct {
	connPool *TCPPool
}

func NewTCPDialer(address string) (*TCPDialer, error) {
	tcpPool, err := NewTCPPool(
		1, 64, func() (any, error) {
			fmt.Println("creating a new conn")
			addr, err := net.ResolveTCPAddr("tcp", address)
			if err != nil {
				return nil, err
			}
			conn, err := net.DialTCP("tcp", nil, addr)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to dial peer %s", address)
			}

			return conn, nil
		})
	if err != nil {
		return nil, err
	}
	tcpPool.Close = func(any interface{}) error {
		if any == nil {
			return nil
		}
		return any.(*net.TCPConn).Close()
	}
	return &TCPDialer{connPool: tcpPool}, nil
}

func (t TCPDialer) Dial() (net.Conn, error) {
	conn, err := t.connPool.New()
	if err != nil {
		return nil, err
	}
	return conn.(*net.TCPConn), nil
}

type RemoteWriteClient struct {
	mu sync.Mutex

	dialer Dialer
	conn   *rpc.Conn

	writer Writer
	logger log.Logger
}

func NewRemoteWriteClient(dialer Dialer, logger log.Logger) *RemoteWriteClient {
	return &RemoteWriteClient{
		dialer: dialer,
		logger: logger,
	}
}

func (r *RemoteWriteClient) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, _ ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return r.writeWithReconnect(ctx, 2, in)
}

func (r *RemoteWriteClient) writeWithReconnect(ctx context.Context, numReconnects int, in *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	conn, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}

	arena := capnp.SingleSegment(nil)
	defer arena.Release()

	result, release := r.writer.Write(ctx, func(params Writer_write_Params) error {
		_, seg, err := capnp.NewMessage(arena)
		if err != nil {
			return err
		}
		wr, err := NewRootWriteRequest(seg)
		if err != nil {
			return err
		}
		if err := params.SetWr(wr); err != nil {
			return err
		}
		wr, err = params.Wr()
		if err != nil {
			return err
		}
		return BuildInto(wr, in.Tenant, in.Timeseries)
	})
	defer release()

	s, err := result.Struct()
	if err != nil {
		if capnp.IsDisconnected(err) {
			conn.Close()
			r.mu.Lock()
			r.conn = nil
			r.mu.Unlock()
		}
		if numReconnects > 0 {
			level.Warn(r.logger).Log("msg", "rpc failed, reconnecting")
			return r.writeWithReconnect(ctx, numReconnects-1, in)
		}
		return nil, errors.Wrap(err, "failed writing to peer")
	}
	defer r.put(conn)

	switch s.Error() {
	case WriteError_unavailable:
		return nil, status.Error(codes.Unavailable, "rpc failed")
	case WriteError_alreadyExists:
		return nil, status.Error(codes.AlreadyExists, "rpc failed")
	case WriteError_invalidArgument:
		return nil, status.Error(codes.InvalidArgument, "rpc failed")
	case WriteError_internal:
		return nil, status.Error(codes.Internal, "rpc failed")
	default:
		return &storepb.WriteResponse{}, nil
	}
}

func (r *RemoteWriteClient) connect(ctx context.Context) (net.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		return nil, nil
	}

	conn, err := r.dialer.Dial()
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial peer")
	}
	r.conn = rpc.NewConn(rpc.NewPackedStreamTransport(conn), nil)
	r.writer = Writer(r.conn.Bootstrap(ctx))
	return conn, nil
}

func (r *RemoteWriteClient) put(c net.Conn) {
	if r.dialer == nil {
		return
	}
	d, ok := r.dialer.(*TCPDialer)
	if !ok {
		return
	}
	d.connPool.Put(c)
}

func (r *RemoteWriteClient) Close() error {
	if r.dialer == nil {
		return nil
	}
	d, ok := r.dialer.(*TCPDialer)
	if !ok {
		return nil
	}
	return d.connPool.Destroy()
}
