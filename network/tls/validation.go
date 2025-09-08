package tls

import (
	"crypto/x509"
	"errors"
)

// ValidateCert verifies that certBytes are signed by the provided CA.
func ValidateCert(certBytes []byte, ca *x509.Certificate) error {
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if ca != nil {
		roots.AddCert(ca)
	} else {
		return errors.New("no CA certificate provided")
	}
	opts := x509.VerifyOptions{Roots: roots}
	_, err = cert.Verify(opts)
	return err
}
