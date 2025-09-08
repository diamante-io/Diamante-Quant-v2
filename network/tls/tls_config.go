package tls

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"strings"
	"time"
)

// Config mirrors TLS settings from the application configuration.
type Config struct {
	Enabled              bool
	NodeID               string
	CertFile             string
	KeyFile              string
	CAFile               string
	CertRotationInterval time.Duration
	MinVersion           string
	CipherSuites         []string
	ClientAuth           string
}

// Build constructs a *tls.Config based on the settings.
func (c *Config) Build() (*stdtls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}

	reload := func() (*stdtls.Certificate, error) {
		cert, err := stdtls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		return &cert, nil
	}

	first, err := reload()
	if err != nil {
		return nil, err
	}

	tlsCfg := &stdtls.Config{
		Certificates: []stdtls.Certificate{*first},
		MinVersion:   parseVersion(c.MinVersion),
	}
	tlsCfg.GetCertificate = func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		return reload()
	}
	tlsCfg.GetClientCertificate = func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
		return reload()
	}

	if c.CAFile != "" {
		caBytes, err := ioutil.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caBytes)
		tlsCfg.ClientCAs = pool
		tlsCfg.RootCAs = pool
	}

	if len(c.CipherSuites) > 0 {
		for _, cs := range c.CipherSuites {
			if v, ok := cipherMap[strings.TrimSpace(cs)]; ok {
				tlsCfg.CipherSuites = append(tlsCfg.CipherSuites, v)
			}
		}
	}

	cauth, ok := clientAuthMap[strings.TrimSpace(c.ClientAuth)]
	if ok {
		tlsCfg.ClientAuth = cauth
	} else {
		tlsCfg.ClientAuth = stdtls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

func parseVersion(v string) uint16 {
	switch strings.TrimSpace(v) {
	case "1.3":
		return stdtls.VersionTLS13
	case "1.2":
		return stdtls.VersionTLS12
	default:
		return stdtls.VersionTLS13
	}
}

var cipherMap = map[string]uint16{
	"TLS_AES_256_GCM_SHA384":       stdtls.TLS_AES_256_GCM_SHA384,
	"TLS_CHACHA20_POLY1305_SHA256": stdtls.TLS_CHACHA20_POLY1305_SHA256,
}

var clientAuthMap = map[string]stdtls.ClientAuthType{
	"RequireAndVerifyClientCert": stdtls.RequireAndVerifyClientCert,
	"RequireAnyClientCert":       stdtls.RequireAnyClientCert,
	"NoClientCert":               stdtls.NoClientCert,
}

// Validate ensures required fields are set when TLS is enabled.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return errors.New("tls cert_file and key_file must be specified")
	}
	return nil
}
