package common

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// LoadServerTLSConfig loads TLS configuration for the gRPC server.
func LoadServerTLSConfig(certPath, keyPath string, caCertPath *string) (credentials.TransportCredentials, error) {
	// Load server certificate and key
	serverCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}

	// If CA cert is provided, enable mutual TLS (mTLS)
	if caCertPath != nil && *caCertPath != "" {
		caCert, err := os.ReadFile(*caCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return credentials.NewTLS(tlsConfig), nil
}

// LoadClientTLSConfig loads TLS configuration for the gRPC client.
func LoadClientTLSConfig(caCertPath string, clientCertPath, clientKeyPath *string, serverName string) (credentials.TransportCredentials, error) {
	// Load CA certificate
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}

	// If client cert and key are provided, enable mutual TLS (mTLS)
	if clientCertPath != nil && clientKeyPath != nil && *clientCertPath != "" && *clientKeyPath != "" {
		clientCert, err := tls.LoadX509KeyPair(*clientCertPath, *clientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return credentials.NewTLS(tlsConfig), nil
}
