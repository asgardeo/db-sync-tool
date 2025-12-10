#!/bin/bash
# Generate TLS certificates for development/testing
# DO NOT use these certificates in production!

set -e

CERT_DIR="${1:-./certs}"
DAYS=365
CA_SUBJECT="/C=US/ST=Local/L=Local/O=Dev/CN=CDC-Sync-CA"
SERVER_SUBJECT="/C=US/ST=Local/L=Local/O=Dev/CN=localhost"
CLIENT_SUBJECT="/C=US/ST=Local/L=Local/O=Dev/CN=cdc-client"

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

echo "Generating CA private key..."
openssl genrsa -out ca.key 4096

echo "Generating CA certificate..."
openssl req -x509 -new -nodes -key ca.key -sha256 -days $DAYS \
    -out ca.crt -subj "$CA_SUBJECT"

echo "Generating server private key..."
openssl genrsa -out server.key 2048

echo "Generating server CSR..."
openssl req -new -key server.key -out server.csr -subj "$SERVER_SUBJECT"

echo "Generating server certificate..."
cat > server_ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = server
DNS.3 = cdc-server
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days $DAYS -sha256 -extfile server_ext.cnf

echo "Generating client private key..."
openssl genrsa -out client.key 2048

echo "Generating client CSR..."
openssl req -new -key client.key -out client.csr -subj "$CLIENT_SUBJECT"

echo "Generating client certificate..."
cat > client_ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client.crt -days $DAYS -sha256 -extfile client_ext.cnf

# Cleanup
rm -f *.csr *.cnf ca.srl

echo ""
echo "Certificates generated in $CERT_DIR:"
ls -la

echo ""
echo "Files:"
echo "  ca.crt      - CA certificate (distribute to both client and server)"
echo "  ca.key      - CA private key (keep secure, only needed for signing)"
echo "  server.crt  - Server certificate (for CDC server)"
echo "  server.key  - Server private key (for CDC server)"
echo "  client.crt  - Client certificate (for CDC client, mTLS)"
echo "  client.key  - Client private key (for CDC client, mTLS)"
