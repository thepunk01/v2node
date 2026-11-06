package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	stdnet "net"
	"sync"
	"testing"
	"time"

	mieruclient "github.com/enfein/mieru/v3/apis/client"
	mierucommon "github.com/enfein/mieru/v3/apis/common"
	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/format"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
	"google.golang.org/protobuf/proto"
)

// echoDispatcher echoes everything back and records who used it.
type echoDispatcher struct {
	mu     sync.Mutex
	emails []string
	dests  []xnet.Destination
}

func (*echoDispatcher) Type() interface{} { return routing.DispatcherType() }
func (*echoDispatcher) Start() error      { return nil }
func (*echoDispatcher) Close() error      { return nil }

func (d *echoDispatcher) Dispatch(ctx context.Context, dest xnet.Destination) (*transport.Link, error) {
	in := session.InboundFromContext(ctx)
	d.mu.Lock()
	if in != nil && in.User != nil {
		d.emails = append(d.emails, in.User.Email)
	}
	d.dests = append(d.dests, dest)
	d.mu.Unlock()
	upR, upW := pipe.New()
	downR, downW := pipe.New()
	go func() {
		defer downW.Close()
		for {
			mb, err := upR.ReadMultiBuffer()
			if err != nil {
				return
			}
			if err := downW.WriteMultiBuffer(mb); err != nil {
				return
			}
		}
	}()
	return &transport.Link{Reader: downR, Writer: upW}, nil
}

func (*echoDispatcher) DispatchLink(context.Context, xnet.Destination, *transport.Link) error {
	return errors.New("not supported")
}

type testPolicy struct{}

func (testPolicy) Type() interface{}              { return policy.ManagerType() }
func (testPolicy) Start() error                   { return nil }
func (testPolicy) Close() error                   { return nil }
func (testPolicy) ForLevel(uint32) policy.Session { return policy.SessionDefault() }
func (testPolicy) ForSystem() policy.System       { return policy.System{} }

func freePort(t *testing.T, network string) int {
	t.Helper()
	if network == "udp" {
		c, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		return c.LocalAddr().(*stdnet.UDPAddr).Port
	}
	l, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*stdnet.TCPAddr).Port
}

func newTestClient(t *testing.T, user string, port int, transport string) mieruclient.Client {
	t.Helper()
	tp := mierupb.TransportProtocol_TCP.Enum()
	if transport == "UDP" {
		tp = mierupb.TransportProtocol_UDP.Enum()
	}
	c := mieruclient.NewClient()
	if err := c.Store(&mieruclient.ClientConfig{
		Profile: &mierupb.ClientProfile{
			ProfileName: proto.String("test"),
			User: &mierupb.User{
				Name:     proto.String(user),
				Password: proto.String(user),
			},
			Servers: []*mierupb.ServerEndpoint{{
				IpAddress: proto.String("127.0.0.1"),
				PortBindings: []*mierupb.PortBinding{{
					Port:     proto.Int32(int32(port)),
					Protocol: tp,
				}},
			}},
			Mtu: proto.Int32(1400),
		},
	}); err != nil {
		t.Fatalf("client Store: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("client Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	return c
}

func TestMieruInbound(t *testing.T) {
	for _, transport := range []string{"TCP", "UDP"} {
		t.Run(transport, func(t *testing.T) {
			network := "tcp"
			if transport == "UDP" {
				network = "udp"
			}
			port := freePort(t, network)
			const tag = "[test]-mieru:1"
			info := &panel.NodeInfo{
				Id:   1,
				Type: "mieru",
				Tag:  tag,
				Common: &panel.CommonNode{
					Protocol:   "mieru",
					ListenIP:   "127.0.0.1",
					ServerPort: port,
					Network:    network,
					Transport:  transport,
					PortBindings: []panel.MieruPortBinding{
						{Port: port, Protocol: transport},
					},
					NetworkSettings: []byte(`{"mtu":"1400"}`),
				},
			}
			d := &echoDispatcher{}
			m, err := newMieruInboundWith(tag, info, nil, d, testPolicy{})
			if err != nil {
				t.Fatalf("newMieruInboundWith: %v", err)
			}
			defer m.Close()

			alice := "11111111-1111-1111-1111-111111111111"
			bob := "22222222-2222-2222-2222-222222222222"
			n, err := m.AddUsers([]panel.UserInfo{{Id: 1, Uuid: alice}, {Id: 2, Uuid: bob}})
			if err != nil || n != 2 {
				t.Fatalf("AddUsers: n=%d err=%v", n, err)
			}

			c := newTestClient(t, alice, port, transport)

			// TCP CONNECT
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := c.DialContext(ctx, mierumodel.NetAddrSpec{
				AddrSpec: mierumodel.AddrSpec{FQDN: "example.com", Port: 443},
				Net:      "tcp",
			})
			if err != nil {
				t.Fatalf("DialContext tcp: %v", err)
			}
			msg := []byte("hello mieru via v2node")
			if _, err := conn.Write(msg); err != nil {
				t.Fatalf("write: %v", err)
			}
			got := make([]byte, len(msg))
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, msg) {
				t.Fatalf("echo mismatch: %q", got)
			}
			conn.Close()

			// UDP ASSOCIATE
			uconn, err := c.DialContext(ctx, mierumodel.NetAddrSpec{
				AddrSpec: mierumodel.AddrSpec{IP: stdnet.IPv4zero, Port: 0},
				Net:      "udp",
			})
			if err != nil {
				t.Fatalf("DialContext udp: %v", err)
			}
			pc := mierucommon.NewUDPAssociateWrapper(mierucommon.NewPacketOverStreamTunnel(uconn))
			target := &stdnet.UDPAddr{IP: stdnet.IPv4(1, 2, 3, 4), Port: 53}
			ping := []byte("ping over udp")
			if _, err := pc.WriteTo(ping, target); err != nil {
				t.Fatalf("udp write: %v", err)
			}
			ubuf := make([]byte, 2048)
			_ = uconn.SetReadDeadline(time.Now().Add(5 * time.Second))
			un, from, err := pc.ReadFrom(ubuf)
			if err != nil {
				t.Fatalf("udp read: %v", err)
			}
			if !bytes.Equal(ubuf[:un], ping) {
				t.Fatalf("udp echo mismatch: %q", ubuf[:un])
			}
			if ua, ok := from.(*stdnet.UDPAddr); !ok || !ua.IP.Equal(target.IP) || ua.Port != target.Port {
				t.Fatalf("udp reply from %v, want %v", from, target)
			}
			uconn.Close()

			want := format.UserTag(tag, alice)
			d.mu.Lock()
			if len(d.emails) < 2 {
				d.mu.Unlock()
				t.Fatalf("dispatcher saw %d connections, want 2", len(d.emails))
			}
			for _, e := range d.emails {
				if e != want {
					d.mu.Unlock()
					t.Fatalf("dispatcher user %q, want %q", e, want)
				}
			}
			if d.dests[0].Address.String() != "example.com" || d.dests[0].Port != 443 {
				d.mu.Unlock()
				t.Fatalf("tcp destination %v", d.dests[0])
			}
			d.mu.Unlock()

			// Removed users are refused.
			m.DelUsers([]panel.UserInfo{{Id: 1, Uuid: alice}})
			if m.hasUser(alice) || !m.hasUser(bob) {
				t.Fatalf("DelUsers did not update the user list")
			}
			c2 := newTestClient(t, alice, port, transport)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel2()
			if conn2, err := c2.DialContext(ctx2, mierumodel.NetAddrSpec{
				AddrSpec: mierumodel.AddrSpec{FQDN: "example.com", Port: 80},
				Net:      "tcp",
			}); err == nil {
				_ = conn2.SetDeadline(time.Now().Add(3 * time.Second))
				_, _ = conn2.Write([]byte("x"))
				one := make([]byte, 1)
				if _, err := conn2.Read(one); err == nil {
					t.Fatalf("removed user can still proxy traffic")
				}
				conn2.Close()
			}

			// Remaining user still works.
			c3 := newTestClient(t, bob, port, transport)
			ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel3()
			conn3, err := c3.DialContext(ctx3, mierumodel.NetAddrSpec{
				AddrSpec: mierumodel.AddrSpec{IP: stdnet.IPv4(9, 9, 9, 9), Port: 80},
				Net:      "tcp",
			})
			if err != nil {
				t.Fatalf("bob DialContext: %v", err)
			}
			if _, err := conn3.Write([]byte("bob")); err != nil {
				t.Fatalf("bob write: %v", err)
			}
			b3 := make([]byte, 3)
			_ = conn3.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.ReadFull(conn3, b3); err != nil || string(b3) != "bob" {
				t.Fatalf("bob echo: %q %v", b3, err)
			}
			conn3.Close()
		})
	}
}

func TestBuildMieruPortBindings(t *testing.T) {
	c := &panel.CommonNode{ServerPort: 2012, Network: "udp"}
	b, err := buildMieruPortBindings(c)
	if err != nil || len(b) != 1 || b[0].GetPort() != 2012 || b[0].GetProtocol() != mierupb.TransportProtocol_UDP {
		t.Fatalf("fallback binding: %v %v", b, err)
	}
	c = &panel.CommonNode{ServerPort: 2012, Transport: "TCP", PortBindings: []panel.MieruPortBinding{{PortRange: "3000-3010"}}}
	b, err = buildMieruPortBindings(c)
	if err != nil || len(b) != 1 || b[0].GetPortRange() != "3000-3010" || b[0].GetProtocol() != mierupb.TransportProtocol_TCP {
		t.Fatalf("range binding: %v %v", b, err)
	}
	c = &panel.CommonNode{PortBindings: []panel.MieruPortBinding{{PortRange: "bad"}}}
	if _, err := buildMieruPortBindings(c); err == nil {
		t.Fatalf("expected error for invalid range")
	}
}
