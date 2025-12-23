#!/usr/bin/env bash

# RustFS Enhanced Security Deployment Script
# This script demonstrates production-ready deployment with enhanced security features

set -e

# Configuration
RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-rustfs-secure}"
DATA_DIR="${DATA_DIR:-./data}"
CERTS_DIR="${CERTS_DIR:-./certs}"
CONSOLE_PORT="${CONSOLE_PORT:-9443}"
API_PORT="${API_PORT:-9000}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
    fi
    log "Docker is available"
}

# Generate TLS certificates for console
generate_certs() {
    if [[ ! -d "$CERTS_DIR" ]]; then
        mkdir -p "$CERTS_DIR"
        log "Created certificates directory: $CERTS_DIR"
    fi

    if [[ ! -f "$CERTS_DIR/console.crt" ]] || [[ ! -f "$CERTS_DIR/console.key" ]]; then
        log "Generating TLS certificates for console..."
        openssl req -x509 -newkey rsa:4096 \
            -keyout "$CERTS_DIR/console.key" \
            -out "$CERTS_DIR/console.crt" \
            -days 365 -nodes \
            -subj "/C=US/ST=CA/L=SF/O=RustFS/CN=localhost"
        
        chmod 600 "$CERTS_DIR/console.key"
        chmod 644 "$CERTS_DIR/console.crt"
        success "TLS certificates generated"
    else
        log "TLS certificates already exist"
    fi
}

# Create data directory
create_data_dir() {
    if [[ ! -d "$DATA_DIR" ]]; then
        mkdir -p "$DATA_DIR"
        log "Created data directory: $DATA_DIR"
    fi
}

# Generate secure credentials
generate_credentials() {
    if [[ -z "$RUSTFS_ACCESS_KEY" ]]; then
        export RUSTFS_ACCESS_KEY="admin-$(openssl rand -hex 8)"
        log "Generated access key: $RUSTFS_ACCESS_KEY"
    fi

    if [[ -z "$RUSTFS_SECRET_KEY" ]]; then
        export RUSTFS_SECRET_KEY="$(openssl rand -hex 32)"
        log "Generated secret key: [HIDDEN]"
    fi

    # Save credentials to .env file
    cat > .env << EOF
RUSTFS_ACCESS_KEY=$RUSTFS_ACCESS_KEY
RUSTFS_SECRET_KEY=$RUSTFS_SECRET_KEY
EOF
    chmod 600 .env
    success "Credentials saved to .env file"
}

# Stop existing container
stop_existing() {
    if docker ps -a --format "table {{.Names}}" | grep -q "^$CONTAINER_NAME\$"; then
        log "Stopping existing container: $CONTAINER_NAME"
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
    fi
}

# Deploy RustFS with enhanced security
deploy_rustfs() {
    log "Deploying RustFS with enhanced security..."
    
    docker run -d \
        --name "$CONTAINER_NAME" \
        --restart unless-stopped \
        -p "$CONSOLE_PORT:9001" \
        -p "$API_PORT:9000" \
        -v "$(pwd)/$DATA_DIR:/data" \
        -v "$(pwd)/$CERTS_DIR:/certs:ro" \
        -e RUSTFS_CONSOLE_TLS_ENABLE=true \
        -e RUSTFS_CONSOLE_TLS_CERT=/certs/console.crt \
        -e RUSTFS_CONSOLE_TLS_KEY=/certs/console.key \
        -e RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true \
        -e RUSTFS_CONSOLE_RATE_LIMIT_RPM=60 \
        -e RUSTFS_CONSOLE_AUTH_TIMEOUT=1800 \
        -e RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://localhost:$CONSOLE_PORT" \
        -e RUSTFS_CORS_ALLOWED_ORIGINS="http://localhost:$API_PORT" \
        -e RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
        -e RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
        -e RUSTFS_EXTERNAL_ADDRESS=":$API_PORT" \
        "$RUSTFS_IMAGE" /data

    # Wait for container to start
    sleep 5

    if docker ps --format "table {{.Names}}" | grep -q "^$CONTAINER_NAME\$"; then
        success "RustFS deployed successfully"
    else
        error "Failed to deploy RustFS"
    fi
}

# Check service health
check_health() {
    log "Checking service health..."
    
    # Check console health
    if curl -k -s "https://localhost:$CONSOLE_PORT/health" | jq -e '.status == "ok"' > /dev/null 2>&1; then
        success "Console service is healthy"
    else
        warn "Console service health check failed"
    fi

    # Check API health  
    if curl -s "http://localhost:$API_PORT/health" | jq -e '.status == "ok"' > /dev/null 2>&1; then
        success "API service is healthy"
    else
        warn "API service health check failed"
    fi
}

# Display access information
show_access_info() {
    echo
    echo "=========================================="
    echo "           RustFS Access Information"
    echo "=========================================="
    echo
    echo "🌐 Console (HTTPS): https://localhost:$CONSOLE_PORT/rustfs/console/"
    echo "🔧 API Endpoint:    http://localhost:$API_PORT"
    echo "🏥 Console Health:  https://localhost:$CONSOLE_PORT/health"
    echo "🏥 API Health:      http://localhost:$API_PORT/health"
    echo
    echo "🔐 Credentials:"
    echo "   Access Key: $RUSTFS_ACCESS_KEY"
    echo "   Secret Key: [Check .env file]"
    echo
    echo "📝 Logs: docker logs $CONTAINER_NAME"
    echo "🛑 Stop: docker stop $CONTAINER_NAME"
    echo
    echo "⚠️  Note: Console uses self-signed certificate"
    echo "   Accept the certificate warning in your browser"
    echo
}

# Main deployment flow
main() {
    log "Starting RustFS Enhanced Security Deployment"
    
    check_docker
    create_data_dir
    generate_certs
    generate_credentials
    stop_existing
    deploy_rustfs
    
    # Wait a bit for services to start
    sleep 10
    
    check_health
    show_access_info
    
    success "Deployment completed successfully!"
}

# Run main function
main "$@"