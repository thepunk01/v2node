package core

// Mieru inbound for v2node.
//
// The mieru protocol is served by the official mieru library
// (github.com/enfein/mieru/v3). Every proxied connection is handed to the
// v2node dispatcher with the user's email in the session, so traffic
// accounting, online IP / device limits, speed limits and routing rules work
// exactly like they do for the xray based protocols.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdnet "net"
	"strconv"
	"strings"
	"sync"
	"time"

	mierucommon "github.com/enfein/mieru/v3/apis/common"
	mieruconstant "github.com/enfein/mieru/v3/apis/constant"
	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mierutp "github.com/enfein/mieru/v3/apis/trafficpattern"
	"github.com/enfein/mieru/v3/pkg/appctl/appctlcommon"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	mierulog "github.com/enfein/mieru/v3/pkg/log"
	mieruprotocol "github.com/enfein/mieru/v3/pkg/protocol"
	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/format"
	"github.com/xtls/xray-core/common/buf"
	xctx "github.com/xtls/xray-core/common/ctx"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	udp_proto "github.com/xtls/xray-core/common/protocol/udp"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/common/task"
	xcore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/proxy/socks"
	"github.com/xtls/xray-core/transport/internet/udp"
	"google.golang.org/protobuf/proto"
)

const mieruDefaultMTU = 1400

var mieruLogOnce sync.Once

type mieruInbound struct {
	tag        string
	instance   *xcore.Instance
	dispatcher routing.Dispatcher
	policy     policy.Manager

	endpoints      []mieruprotocol.UnderlayProperties
	trafficPattern *mierutp.Config
	hintMandatory  bool
	tcpIdleTimeout time.Duration

	mu      sync.Mutex
	mux     *mieruprotocol.Mux
	users   map[string]*mierupb.User
	started bool
	closed  bool
}

type mieruNetworkSettings struct {
	MTU            any `json:"mtu"`
	TCPIdleTimeout any `json:"tcp_idle_timeout"`
}

func anyToInt(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(t))
		return i
	}
	return 0
}

func mieruTransport(s string) *mierupb.TransportProtocol {
	if strings.EqualFold(strings.TrimSpace(s), "udp") {
		return mierupb.TransportProtocol_UDP.Enum()
	}
	return mierupb.TransportProtocol_TCP.Enum()
}

func buildMieruPortBindings(c *panel.CommonNode) ([]*mierupb.PortBinding, error) {
	defaultTransport := c.Transport
	if defaultTransport == "" {
		defaultTransport = c.Network
	}
	var bindings []*mierupb.PortBinding
	for _, b := range c.PortBindings {
		proto3 := b.Protocol
		if proto3 == "" {
			proto3 = defaultTransport
		}
		pb := &mierupb.PortBinding{Protocol: mieruTransport(proto3)}
		if r := strings.TrimSpace(b.PortRange); r != "" {
			pb.PortRange = proto.String(r)
		} else if b.Port > 0 {
			pb.Port = proto.Int32(int32(b.Port))
		} else {
			continue
		}
		bindings = append(bindings, pb)
	}
	if len(bindings) == 0 {
		if c.ServerPort <= 0 {
			return nil, fmt.Errorf("mieru: server port is not set")
		}
		bindings = append(bindings, &mierupb.PortBinding{
			Port:     proto.Int32(int32(c.ServerPort)),
			Protocol: mieruTransport(defaultTransport),
		})
	}
	if _, err := appctlcommon.FlatPortBindings(bindings); err != nil {
		return nil, fmt.Errorf("mieru: invalid port bindings: %w", err)
	}
	return bindings, nil
}

func newMieruInbound(v *V2Core, tag string, info *panel.NodeInfo) (*mieruInbound, error) {
	pm, ok := v.Server.GetFeature(policy.ManagerType()).(policy.Manager)
	if !ok {
		return nil, fmt.Errorf("mieru: policy manager not found")
	}
	return newMieruInboundWith(tag, info, v.Server, v.dispatcher, pm)
}

func newMieruInboundWith(tag string, info *panel.NodeInfo, instance *xcore.Instance, d routing.Dispatcher, pm policy.Manager) (*mieruInbound, error) {
	if d == nil {
		return nil, fmt.Errorf("mieru: dispatcher is not ready")
	}
	mieruLogOnce.Do(func() {
		// Keep the mieru library quiet, v2node has its own logs.
		mierulog.SetFormatter(&mierulog.NilFormatter{})
	})
	c := info.Common
	if c == nil {
		return nil, fmt.Errorf("mieru: node info is empty")
	}
	ns := &mieruNetworkSettings{}
	if len(c.NetworkSettings) > 0 && string(c.NetworkSettings) != "null" {
		if err := json.Unmarshal(c.NetworkSettings, ns); err != nil {
			log.WithField("tag", tag).Warnf("mieru: ignore invalid network_settings: %v", err)
		}
	}
	mtu := anyToInt(ns.MTU)
	if mtu <= 0 {
		mtu = mieruDefaultMTU
	}
	bindings, err := buildMieruPortBindings(c)
	if err != nil {
		return nil, err
	}
	listenIP := strings.TrimSpace(c.ListenIP)
	if listenIP == "" {
		listenIP = "0.0.0.0"
	}
	endpoints, err := appctlcommon.AddrPortToUnderlayProperties(listenIP, bindings, mtu)
	if err != nil {
		return nil, fmt.Errorf("mieru: build listening endpoints: %w", err)
	}
	var pattern *mierupb.TrafficPattern
	if tp := strings.TrimSpace(c.TrafficPattern); tp != "" {
		pattern, err = mierutp.Decode(tp)
		if err != nil {
			return nil, fmt.Errorf("mieru: decode traffic pattern: %w", err)
		}
		if err := mierutp.Validate(pattern); err != nil {
			return nil, fmt.Errorf("mieru: invalid traffic pattern: %w", err)
		}
	}
	tpConfig, err := mierutp.NewConfig(pattern)
	if err != nil {
		return nil, fmt.Errorf("mieru: traffic pattern: %w", err)
	}
	var idle time.Duration
	if s := anyToInt(ns.TCPIdleTimeout); s > 0 {
		idle, err = appctlcommon.ParseServerTCPIdleTimeout(strconv.Itoa(s) + "s")
		if err != nil {
			return nil, fmt.Errorf("mieru: tcp idle timeout: %w", err)
		}
	}
	return &mieruInbound{
		tag:            tag,
		instance:       instance,
		dispatcher:     d,
		policy:         pm,
		endpoints:      endpoints,
		trafficPattern: tpConfig,
		hintMandatory:  c.UserHintIsMandatory,
		tcpIdleTimeout: idle,
		users:          make(map[string]*mierupb.User),
	}, nil
}

func (m *mieruInbound) usersSnapshot() map[string]*mierupb.User {
	users := make(map[string]*mierupb.User, len(m.users))
	for k, u := range m.users {
		users[k] = u
	}
	return users
}

// AddUsers registers users and starts the listener once there is at least
// one user (the mieru mux refuses to start without users).
func (m *mieruInbound) AddUsers(users []panel.UserInfo) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, fmt.Errorf("mieru inbound %s is closed", m.tag)
	}
	added := 0
	for i := range users {
		name := users[i].Uuid
		if name == "" {
			continue
		}
		m.users[name] = &mierupb.User{
			Name:     proto.String(name),
			Password: proto.String(name),
		}
		added++
	}
	if m.mux == nil {
		m.mux = mieruprotocol.NewMux(false)
		m.mux.SetEndpoints(m.endpoints).
			SetTrafficPattern(m.trafficPattern).
			SetServerUserHintIsMandatory(m.hintMandatory).
			SetServerTCPIdleTimeout(m.tcpIdleTimeout)
	}
	if len(m.users) == 0 {
		return added, nil
	}
	m.mux.SetServerUsers(m.usersSnapshot())
	if !m.started {
		if err := m.mux.Start(); err != nil {
			m.mux = nil
			return 0, fmt.Errorf("mieru: start listener: %w", err)
		}
		m.started = true
		go m.acceptLoop(m.mux)
		log.WithField("tag", m.tag).Infof("Mieru listening on %d endpoint(s)", len(m.endpoints))
	}
	return added, nil
}

func (m *mieruInbound) DelUsers(users []panel.UserInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range users {
		delete(m.users, users[i].Uuid)
	}
	if m.mux != nil && m.started && len(m.users) > 0 {
		m.mux.SetServerUsers(m.usersSnapshot())
	} else if m.mux != nil && m.started {
		// mieru does not accept an empty user list, use a random placeholder
		// so nobody can log in.
		placeholder := "v2node-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		m.mux.SetServerUsers(map[string]*mierupb.User{
			placeholder: {Name: proto.String(placeholder), Password: proto.String(placeholder + "-" + placeholder)},
		})
	}
}

func (m *mieruInbound) hasUser(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.users[name]
	return ok
}

func (m *mieruInbound) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.mux != nil {
		err := m.mux.Close()
		m.mux = nil
		return err
	}
	return nil
}

func (m *mieruInbound) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mieruInbound) acceptLoop(mux *mieruprotocol.Mux) {
	for {
		conn, err := mux.Accept()
		if err != nil {
			if m.isClosed() {
				return
			}
			log.WithField("tag", m.tag).Debugf("mieru accept error: %v", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go m.handleConn(conn)
	}
}

func safeDestination(addr stdnet.Addr, network xnet.Network) xnet.Destination {
	switch a := addr.(type) {
	case *stdnet.TCPAddr:
		return xnet.TCPDestination(xnet.IPAddress(a.IP), xnet.Port(a.Port))
	case *stdnet.UDPAddr:
		return xnet.UDPDestination(xnet.IPAddress(a.IP), xnet.Port(a.Port))
	}
	if addr != nil {
		if host, port, err := stdnet.SplitHostPort(addr.String()); err == nil {
			if p, err := xnet.PortFromString(port); err == nil {
				return xnet.Destination{Network: network, Address: xnet.ParseAddress(host), Port: p}
			}
		}
	}
	return xnet.Destination{Network: network, Address: xnet.AnyIP, Port: 0}
}

func mieruAddrToDestination(a mierumodel.AddrSpec, network xnet.Network) xnet.Destination {
	var address xnet.Address
	if a.FQDN != "" {
		address = xnet.DomainAddress(a.FQDN)
	} else {
		address = xnet.IPAddress(a.IP)
	}
	return xnet.Destination{Network: network, Address: address, Port: xnet.Port(a.Port)}
}

func (m *mieruInbound) handleConn(conn stdnet.Conn) {
	defer conn.Close()
	uc, ok := conn.(mierucommon.UserContext)
	if !ok {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	req := &mierumodel.Request{}
	if err := req.ReadFromSocks5(conn); err != nil {
		log.WithField("tag", m.tag).Debugf("mieru: read request: %v", err)
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	userName := uc.UserName()
	if userName == "" || !m.hasUser(userName) {
		return
	}
	reply := mieruconstant.Socks5ReplySuccess
	if req.Command != mieruconstant.Socks5ConnectCmd && req.Command != mieruconstant.Socks5UDPAssociateCmd {
		reply = mieruconstant.Socks5ReplyCommandNotSupported
	}
	resp := &mierumodel.Response{
		Reply:    reply,
		BindAddr: mierumodel.AddrSpec{IP: stdnet.IPv4zero, Port: 0},
	}
	if err := resp.WriteToSocks5(conn); err != nil || reply != mieruconstant.Socks5ReplySuccess {
		return
	}

	ctx := context.Background()
	if m.instance != nil {
		ctx = context.WithValue(ctx, xcore.XrayKey(1), m.instance)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = xctx.ContextWithID(ctx, session.NewID())
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	source := safeDestination(conn.RemoteAddr(), xnet.Network_TCP)
	ctx = session.ContextWithInbound(ctx, &session.Inbound{
		Source:        source,
		Local:         safeDestination(conn.LocalAddr(), xnet.Network_TCP),
		Tag:           m.tag,
		Name:          "mieru",
		User:          &protocol.MemoryUser{Email: format.UserTag(m.tag, userName), Level: 0},
		CanSpliceCopy: 3,
	})
	ctx = session.ContextWithContent(ctx, &session.Content{
		SniffingRequest: session.SniffingRequest{
			Enabled:                        true,
			OverrideDestinationForProtocol: []string{"http", "tls", "quic"},
		},
	})

	var err error
	switch req.Command {
	case mieruconstant.Socks5ConnectCmd:
		err = m.handleTCP(ctx, conn, mieruAddrToDestination(req.DstAddr, xnet.Network_TCP))
	case mieruconstant.Socks5UDPAssociateCmd:
		err = m.handleUDP(ctx, conn)
	}
	if err != nil {
		log.WithField("tag", m.tag).Debugf("mieru: connection ends: %v", err)
	}
}

func (m *mieruInbound) handleTCP(ctx context.Context, conn stdnet.Conn, dest xnet.Destination) error {
	if !dest.IsValid() || dest.Port == 0 {
		return fmt.Errorf("invalid destination %v", dest)
	}
	sessionPolicy := m.policy.ForLevel(0)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := signal.CancelAfterInactivity(ctx, cancel, sessionPolicy.Timeouts.ConnectionIdle)
	ctx = policy.ContextWithBufferPolicy(ctx, sessionPolicy.Buffer)

	link, err := m.dispatcher.Dispatch(ctx, dest)
	if err != nil {
		return fmt.Errorf("dispatch to %v: %w", dest, err)
	}
	clientReader := buf.NewReader(conn)
	clientWriter := buf.NewWriter(conn)

	requestDone := func() error {
		defer timer.SetTimeout(sessionPolicy.Timeouts.DownlinkOnly)
		return buf.Copy(clientReader, link.Writer, buf.UpdateActivity(timer))
	}
	responseDone := func() error {
		defer timer.SetTimeout(sessionPolicy.Timeouts.UplinkOnly)
		return buf.Copy(link.Reader, clientWriter, buf.UpdateActivity(timer))
	}
	requestDonePost := task.OnSuccess(requestDone, task.Close(link.Writer))
	if err := task.Run(ctx, requestDonePost, responseDone); err != nil {
		_ = interruptAny(link.Reader)
		_ = interruptAny(link.Writer)
		return err
	}
	return nil
}

func interruptAny(v any) error {
	if i, ok := v.(interface{ Interrupt() }); ok {
		i.Interrupt()
		return nil
	}
	if c, ok := v.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (m *mieruInbound) handleUDP(ctx context.Context, conn stdnet.Conn) error {
	sessionPolicy := m.policy.ForLevel(0)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := signal.CancelAfterInactivity(ctx, cancel, sessionPolicy.Timeouts.ConnectionIdle)
	defer timer.SetTimeout(0)

	pc := mierucommon.NewPacketOverStreamTunnel(conn)
	var writeMu sync.Mutex
	udpServer := udp.NewDispatcher(m.dispatcher, func(ctx context.Context, packet *udp_proto.Packet) {
		payload := packet.Payload
		defer payload.Release()
		from := packet.Source
		if payload.UDP != nil {
			from = *payload.UDP
		}
		msg, err := socks.EncodeUDPPacket(&protocol.RequestHeader{
			Address: from.Address,
			Port:    from.Port,
		}, payload.Bytes())
		if err != nil {
			return
		}
		defer msg.Release()
		writeMu.Lock()
		_, err = pc.WriteTo(msg.Bytes(), nil)
		writeMu.Unlock()
		if err != nil {
			cancel()
			return
		}
		timer.Update()
	})
	defer udpServer.RemoveRay()

	readBuf := make([]byte, 65535)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		n, _, err := pc.ReadFrom(readBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrClosedPipe {
				return nil
			}
			return err
		}
		if n == 0 {
			continue
		}
		timer.Update()
		b := buf.New()
		if n > int(b.Cap()) {
			b.Release()
			b = buf.NewWithSize(int32(n))
		}
		if _, err := b.Write(readBuf[:n]); err != nil {
			b.Release()
			continue
		}
		request, err := socks.DecodeUDPPacket(b)
		if err != nil || b.IsEmpty() {
			b.Release()
			continue
		}
		dest := request.Destination()
		b.UDP = &dest
		udpServer.Dispatch(protocol.ContextWithRequestHeader(ctx, request), dest, b)
	}
}
