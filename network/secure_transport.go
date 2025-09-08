package network

import (
	stdtls "crypto/tls"
	"net"
)

// dialTLS establishes a TLS connection using the provided configuration.
func dialTLS(addr string, cfg *stdtls.Config) (net.Conn, error) {
	if cfg == nil {
		return net.Dial("tcp", addr)
	}
	dialCfg := cfg.Clone()
	if dialCfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err == nil {
			dialCfg.ServerName = host
		}
	}
	return stdtls.Dial("tcp", addr, dialCfg)
}

// listenTLS creates a listener that accepts TLS connections when cfg is not nil.
func listenTLS(addr string, cfg *stdtls.Config) (net.Listener, error) {
	if cfg == nil {
		return net.Listen("tcp", addr)
	}
	return stdtls.Listen("tcp", addr, cfg)
}
