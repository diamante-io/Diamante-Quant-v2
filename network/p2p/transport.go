package p2p

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"diamante/apperrors"
	"diamante/common"
	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// TransportConfig holds configuration for the transport layer
type TransportConfig struct {
	// Network settings
	ListenAddress  string `mapstructure:"listen_address"`
	Port           uint16 `mapstructure:"port"`
	MaxConnections int    `mapstructure:"max_connections"`

	// Timeouts
	ConnectTimeout    time.Duration `mapstructure:"connect_timeout"`
	HandshakeTimeout  time.Duration `mapstructure:"handshake_timeout"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`
	KeepAliveInterval time.Duration `mapstructure:"keepalive_interval"`

	// Message limits
	MaxMessageSize int `mapstructure:"max_message_size"`

	// TLS settings
	EnableTLS   bool   `mapstructure:"enable_tls"`
	TLSCertFile string `mapstructure:"tls_cert_file"`
	TLSKeyFile  string `mapstructure:"tls_key_file"`
	TLSCAFile   string `mapstructure:"tls_ca_file"`

	// Performance settings
	BufferSize      int  `mapstructure:"buffer_size"`
	EnableNagle     bool `mapstructure:"enable_nagle"`
	EnableKeepalive bool `mapstructure:"enable_keepalive"`
}

// DefaultTransportConfig returns default transport configuration
func DefaultTransportConfig() *TransportConfig {
	return &TransportConfig{
		ListenAddress:     "0.0.0.0",
		Port:              8545,
		MaxConnections:    1000,
		ConnectTimeout:    30 * time.Second,
		HandshakeTimeout:  10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      10 * time.Second,
		KeepAliveInterval: 30 * time.Second,
		MaxMessageSize:    MaxMessageSize,
		EnableTLS:         true,
		BufferSize:        64 * 1024, // 64KB
		EnableNagle:       false,     // Disable Nagle for low latency
		EnableKeepalive:   true,
	}
}

// ConnectionHandler defines the interface for handling new connections
type ConnectionHandler interface {
	HandleConnection(conn net.Conn, inbound bool) error
}

// ConnectionHandlerFunc is a function adapter for ConnectionHandler
type ConnectionHandlerFunc func(conn net.Conn, inbound bool) error

// HandleConnection implements ConnectionHandler
func (f ConnectionHandlerFunc) HandleConnection(conn net.Conn, inbound bool) error {
	return f(conn, inbound)
}

// Transport handles low-level network communication
type Transport struct {
	config *TransportConfig
	logger *logrus.Logger

	// TLS configuration
	tlsConfig *tls.Config

	// Network listener
	listener net.Listener

	// Connection management
	connections   map[string]net.Conn
	connectionsMu sync.RWMutex
	activeConns   int
	maxConns      int

	// Connection handler
	handler ConnectionHandler

	// Control channels
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	acceptDone chan struct{}

	// Statistics
	stats TransportStats

	// Connection pool for outbound connections
	connPool *connectionPool
}

// TransportStats holds transport layer statistics
type TransportStats struct {
	mu               sync.RWMutex
	InboundConns     uint64
	OutboundConns    uint64
	TotalConns       uint64
	ActiveConns      uint64
	BytesSent        uint64
	BytesReceived    uint64
	MessagesSent     uint64
	MessagesReceived uint64
	ConnectionErrors uint64
	HandshakeErrors  uint64
	TimeoutErrors    uint64
}

// GetStats returns a copy of the current statistics
func (ts *TransportStats) GetStats() map[string]interface{} {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return map[string]interface{}{
		"inbound_connections":  ts.InboundConns,
		"outbound_connections": ts.OutboundConns,
		"total_connections":    ts.TotalConns,
		"active_connections":   ts.ActiveConns,
		"bytes_sent":           ts.BytesSent,
		"bytes_received":       ts.BytesReceived,
		"messages_sent":        ts.MessagesSent,
		"messages_received":    ts.MessagesReceived,
		"connection_errors":    ts.ConnectionErrors,
		"handshake_errors":     ts.HandshakeErrors,
		"timeout_errors":       ts.TimeoutErrors,
	}
}

// NewTransport creates a new transport instance
func NewTransport(config *TransportConfig, tlsConfig *tls.Config, logger *logrus.Logger) *Transport {
	if config == nil {
		config = DefaultTransportConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	ctx, cancel := context.WithCancel(context.Background())

	transport := &Transport{
		config:      config,
		logger:      logger,
		tlsConfig:   tlsConfig,
		connections: make(map[string]net.Conn),
		maxConns:    config.MaxConnections,
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
		acceptDone:  make(chan struct{}),
		connPool:    newConnectionPool(config.MaxConnections/2, logger), // Half for outbound
	}

	return transport
}

// SetConnectionHandler sets the connection handler
func (t *Transport) SetConnectionHandler(handler ConnectionHandler) {
	t.handler = handler
}

// Listen starts listening for incoming connections
func (t *Transport) Listen() error {
	if t.listener != nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"transport is already listening")
	}

	address := net.JoinHostPort(t.config.ListenAddress, string(rune(t.config.Port)))

	var listener net.Listener
	var err error

	if t.config.EnableTLS && t.tlsConfig != nil {
		listener, err = tls.Listen("tcp", address, t.tlsConfig)
		if err != nil {
			return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to create TLS listener")
		}
		t.logger.WithField("address", address).Info("Started TLS listener")
	} else {
		listener, err = net.Listen("tcp", address)
		if err != nil {
			return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to create TCP listener")
		}
		t.logger.WithField("address", address).Info("Started TCP listener")
	}

	t.listener = listener

	// Start accepting connections
	go t.acceptLoop()

	return nil
}

// Connect establishes an outbound connection
func (t *Transport) Connect(address string) (net.Conn, error) {
	return t.ConnectWithContext(t.ctx, address)
}

// ConnectWithContext establishes an outbound connection with context
func (t *Transport) ConnectWithContext(ctx context.Context, address string) (net.Conn, error) {
	// Check connection limits
	t.connectionsMu.RLock()
	if t.activeConns >= t.maxConns {
		t.connectionsMu.RUnlock()
		t.stats.mu.Lock()
		t.stats.ConnectionErrors++
		t.stats.mu.Unlock()
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"maximum connections reached")
	}
	t.connectionsMu.RUnlock()

	// Create dialer with timeout
	dialer := &net.Dialer{
		Timeout:   t.config.ConnectTimeout,
		KeepAlive: t.config.KeepAliveInterval,
	}

	var conn net.Conn
	var err error

	if t.config.EnableTLS && t.tlsConfig != nil {
		// TLS connection
		conn, err = tls.DialWithDialer(dialer, "tcp", address, t.tlsConfig)
		if err != nil {
			t.stats.mu.Lock()
			t.stats.ConnectionErrors++
			t.stats.mu.Unlock()
			return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to establish TLS connection")
		}
	} else {
		// Plain TCP connection
		conn, err = dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			t.stats.mu.Lock()
			t.stats.ConnectionErrors++
			t.stats.mu.Unlock()
			return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to establish TCP connection")
		}
	}

	// Configure connection
	if err := t.configureConnection(conn); err != nil {
		conn.Close()
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to configure connection")
	}

	// Register connection
	t.registerConnection(conn, false)

	// Handle the connection if handler is set
	if t.handler != nil {
		go func() {
			if err := t.handler.HandleConnection(conn, false); err != nil {
				t.logger.WithError(err).Error("Connection handler failed")
				t.closeConnection(conn)
			}
		}()
	}

	t.logger.WithField("address", address).Debug("Established outbound connection")
	return conn, nil
}

// Close stops the transport and closes all connections
func (t *Transport) Close() error {
	t.logger.Info("Closing transport")

	// Cancel context to stop all operations
	t.cancel()

	// Close listener
	if t.listener != nil {
		t.listener.Close()
		// Wait for accept loop to finish
		<-t.acceptDone
	}

	// Close all connections
	t.connectionsMu.Lock()
	for _, conn := range t.connections {
		conn.Close()
	}
	t.connections = make(map[string]net.Conn)
	t.activeConns = 0
	t.connectionsMu.Unlock()

	// Close connection pool
	if t.connPool != nil {
		t.connPool.Close()
	}

	// Signal completion
	close(t.done)

	t.logger.Info("Transport closed")
	return nil
}

// SendMessage sends a message over a connection
func (t *Transport) SendMessage(conn net.Conn, data []byte) error {
	if conn == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"connection is nil")
	}

	if len(data) == 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"data is empty")
	}

	if len(data) > t.config.MaxMessageSize {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message too large")
	}

	// Set write timeout
	if err := conn.SetWriteDeadline(common.ConsensusNow().Add(t.config.WriteTimeout)); err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to set write deadline")
	}

	// Write data
	_, err := conn.Write(data)
	if err != nil {
		t.stats.mu.Lock()
		t.stats.ConnectionErrors++
		t.stats.mu.Unlock()
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to write data")
	}

	// Update statistics
	t.stats.mu.Lock()
	t.stats.BytesSent += uint64(len(data))
	t.stats.MessagesSent++
	t.stats.mu.Unlock()

	return nil
}

// ReceiveMessage receives a message from a connection
func (t *Transport) ReceiveMessage(conn net.Conn) ([]byte, error) {
	if conn == nil {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"connection is nil")
	}

	// Set read timeout
	if err := conn.SetReadDeadline(common.ConsensusNow().Add(t.config.ReadTimeout)); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to set read deadline")
	}

	// Read header first to get message length
	headerBuf := make([]byte, HeaderSize)
	_, err := io.ReadFull(conn, headerBuf)
	if err != nil {
		if err == io.EOF {
			return nil, err // Connection closed
		}
		t.stats.mu.Lock()
		t.stats.ConnectionErrors++
		t.stats.mu.Unlock()
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to read message header")
	}

	// Extract payload length from header
	if len(headerBuf) < 12 {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"invalid header size")
	}

	payloadLength := uint32(headerBuf[8])<<24 | uint32(headerBuf[9])<<16 |
		uint32(headerBuf[10])<<8 | uint32(headerBuf[11])

	// Validate payload length
	if payloadLength > uint32(t.config.MaxMessageSize-HeaderSize) {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"payload too large")
	}

	// Read payload if present
	var fullMessage []byte
	if payloadLength > 0 {
		payloadBuf := make([]byte, payloadLength)
		_, err = io.ReadFull(conn, payloadBuf)
		if err != nil {
			t.stats.mu.Lock()
			t.stats.ConnectionErrors++
			t.stats.mu.Unlock()
			return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to read message payload")
		}

		fullMessage = append(headerBuf, payloadBuf...)
	} else {
		fullMessage = headerBuf
	}

	// Update statistics
	t.stats.mu.Lock()
	t.stats.BytesReceived += uint64(len(fullMessage))
	t.stats.MessagesReceived++
	t.stats.mu.Unlock()

	return fullMessage, nil
}

// GetStats returns transport statistics
func (t *Transport) GetStats() map[string]interface{} {
	return t.stats.GetStats()
}

// GetActiveConnections returns the number of active connections
func (t *Transport) GetActiveConnections() int {
	t.connectionsMu.RLock()
	defer t.connectionsMu.RUnlock()
	return t.activeConns
}

// acceptLoop handles incoming connections
func (t *Transport) acceptLoop() {
	defer close(t.acceptDone)

	for {
		select {
		case <-t.ctx.Done():
			t.logger.Debug("Accept loop context cancelled")
			return
		default:
			conn, err := t.listener.Accept()
			if err != nil {
				select {
				case <-t.ctx.Done():
					// Expected error during shutdown
					return
				default:
					t.logger.WithError(err).Error("Failed to accept connection")
					t.stats.mu.Lock()
					t.stats.ConnectionErrors++
					t.stats.mu.Unlock()
					continue
				}
			}

			// Check connection limits
			t.connectionsMu.RLock()
			if t.activeConns >= t.maxConns {
				t.connectionsMu.RUnlock()
				t.logger.Warn("Maximum connections reached, rejecting connection")
				conn.Close()
				t.stats.mu.Lock()
				t.stats.ConnectionErrors++
				t.stats.mu.Unlock()
				continue
			}
			t.connectionsMu.RUnlock()

			// Configure connection
			if err := t.configureConnection(conn); err != nil {
				t.logger.WithError(err).Error("Failed to configure connection")
				conn.Close()
				continue
			}

			// Register connection
			t.registerConnection(conn, true)

			// Handle the connection
			go t.handleInboundConnection(conn)
		}
	}
}

// handleInboundConnection handles a new inbound connection
func (t *Transport) handleInboundConnection(conn net.Conn) {
	defer t.closeConnection(conn)

	t.logger.WithField("remote_addr", conn.RemoteAddr()).Debug("Handling inbound connection")

	if t.handler != nil {
		if err := t.handler.HandleConnection(conn, true); err != nil {
			t.logger.WithError(err).Error("Connection handler failed")
		}
	}
}

// configureConnection configures a network connection
func (t *Transport) configureConnection(conn net.Conn) error {
	// Configure TCP connection if applicable
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Set keep-alive
		if t.config.EnableKeepalive {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
					"failed to enable keep-alive")
			}

			if err := tcpConn.SetKeepAlivePeriod(t.config.KeepAliveInterval); err != nil {
				return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
					"failed to set keep-alive period")
			}
		}

		// Configure Nagle algorithm
		if err := tcpConn.SetNoDelay(!t.config.EnableNagle); err != nil {
			return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to configure Nagle algorithm")
		}
	}

	return nil
}

// registerConnection registers a new connection
func (t *Transport) registerConnection(conn net.Conn, inbound bool) {
	t.connectionsMu.Lock()
	defer t.connectionsMu.Unlock()

	connKey := conn.RemoteAddr().String()
	t.connections[connKey] = conn
	t.activeConns++

	// Update statistics
	t.stats.mu.Lock()
	t.stats.TotalConns++
	t.stats.ActiveConns = uint64(t.activeConns)
	if inbound {
		t.stats.InboundConns++
	} else {
		t.stats.OutboundConns++
	}
	t.stats.mu.Unlock()
}

// closeConnection closes and unregisters a connection
func (t *Transport) closeConnection(conn net.Conn) {
	if conn == nil {
		return
	}

	t.connectionsMu.Lock()
	defer t.connectionsMu.Unlock()

	connKey := conn.RemoteAddr().String()
	if _, exists := t.connections[connKey]; exists {
		delete(t.connections, connKey)
		t.activeConns--

		// Update statistics
		t.stats.mu.Lock()
		t.stats.ActiveConns = uint64(t.activeConns)
		t.stats.mu.Unlock()
	}

	conn.Close()
}

// connectionPool manages a pool of reusable connections
type connectionPool struct {
	pool    chan net.Conn
	factory func() (net.Conn, error)
	logger  *logrus.Logger
	closed  bool
	mu      sync.RWMutex
}

// newConnectionPool creates a new connection pool
func newConnectionPool(size int, logger *logrus.Logger) *connectionPool {
	return &connectionPool{
		pool:   make(chan net.Conn, size),
		logger: logger,
	}
}

// Get retrieves a connection from the pool
func (cp *connectionPool) Get() (net.Conn, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"connection pool is closed")
	}

	select {
	case conn := <-cp.pool:
		// Test connection before returning
		if cp.isConnectionValid(conn) {
			return conn, nil
		}
		// Connection is invalid, fall through to create new one
	default:
		// No connections available in pool
	}

	// Create new connection if factory is available
	if cp.factory != nil {
		return cp.factory()
	}

	return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
		"no connections available and no factory set")
}

// Put returns a connection to the pool
func (cp *connectionPool) Put(conn net.Conn) {
	if conn == nil {
		return
	}

	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		conn.Close()
		return
	}

	select {
	case cp.pool <- conn:
		// Connection added to pool
	default:
		// Pool is full, close the connection
		conn.Close()
	}
}

// SetFactory sets the connection factory function
func (cp *connectionPool) SetFactory(factory func() (net.Conn, error)) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.factory = factory
}

// Close closes the connection pool and all connections
func (cp *connectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return
	}

	cp.closed = true

	// Close all connections in the pool
	close(cp.pool)
	for conn := range cp.pool {
		conn.Close()
	}
}

// isConnectionValid tests if a connection is still valid
func (cp *connectionPool) isConnectionValid(conn net.Conn) bool {
	if conn == nil {
		return false
	}

	// Set a very short deadline to test the connection
	conn.SetReadDeadline(common.ConsensusNow().Add(1 * time.Millisecond))

	// Try to read one byte
	one := make([]byte, 1)
	_, err := conn.Read(one)

	// Reset deadline
	conn.SetReadDeadline(time.Time{})

	// If we get a timeout, the connection is likely valid
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Any other error means the connection is invalid
	return false
}

// checkConnectionHealth performs a simple connection health check
func (t *Transport) checkConnectionHealth(conn net.Conn) error {
	// Set a very short deadline for health check
	originalDeadline := time.Time{}
	if _, ok := conn.(*net.TCPConn); ok {
		// Try to read with minimal timeout to check if connection is alive
		conn.SetReadDeadline(consensus.ConsensusNow().Add(1 * time.Millisecond))
		defer conn.SetReadDeadline(originalDeadline)

		// Try to read one byte - this will fail immediately if connection is closed
		buf := make([]byte, 1)
		_, err := conn.Read(buf)

		// We expect this to timeout for healthy connections
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil // Connection is healthy
		}

		// If we get EOF or other connection errors, connection is unhealthy
		if err != nil {
			return fmt.Errorf("connection unhealthy: %w", err)
		}

		// If we actually read data, put the deadline back and return healthy
		return nil
	}

	return fmt.Errorf("unsupported connection type for health check")
}
