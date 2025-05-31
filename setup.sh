#!/bin/bash
# setup.sh - Complete setup script for Kedai Kopi Dynamic Query System
# ROBD Assignment - Spring 2025 | Group 3

set -e  # Exit on any error

echo "🌟 Kedai Kopi Dynamic Query System - Setup Script"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if running as root for Docker commands
check_docker_permissions() {
    if ! docker ps >/dev/null 2>&1; then
        print_warning "Docker requires sudo permissions"
        print_info "You may need to run: sudo usermod -aG docker $USER"
        print_info "Then logout and login again"
        return 1
    fi
    return 0
}

# Step 1: Check prerequisites
echo
print_info "Step 1: Checking prerequisites..."

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
python_major=$(echo $python_version | cut -d'.' -f1)
python_minor=$(echo $python_version | cut -d'.' -f2)

if [ "$python_major" -eq 3 ] && [ "$python_minor" -ge 11 ]; then
    print_status "Python $python_version (compatible with Cassandra driver)"
else
    print_error "Python 3.11+ required for Cassandra driver compatibility"
    print_info "Current version: $python_version"
    exit 1
fi

# Check Docker
if command -v docker >/dev/null 2>&1; then
    print_status "Docker installed"
else
    print_error "Docker not found. Please install Docker first."
    exit 1
fi

# Check Docker Compose
if command -v docker-compose >/dev/null 2>&1 || docker compose version >/dev/null 2>&1; then
    print_status "Docker Compose available"
else
    print_error "Docker Compose not found. Please install Docker Compose."
    exit 1
fi

# Check Docker permissions
if ! check_docker_permissions; then
    print_error "Cannot access Docker. Please check permissions."
    exit 1
fi

# Step 2: Create project structure
echo
print_info "Step 2: Creating project structure..."

# Create directories
mkdir -p src/core
mkdir -p src/interfaces
mkdir -p src/data_loaders
mkdir -p aggregator
mkdir -p init-scripts/mongodb
mkdir -p init-scripts/cassandra
mkdir -p data
mkdir -p logs

print_status "Project directories created"

# Step 3: Setup Python virtual environment
echo
print_info "Step 3: Setting up Python virtual environment..."

if [ ! -d "cafe_venv" ]; then
    python3 -m venv cafe_venv
    print_status "Virtual environment created"
else
    print_warning "Virtual environment already exists"
fi

# Activate virtual environment
source cafe_venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install main dependencies
print_info "Installing Python dependencies..."
pip install -r requirements.txt

print_status "Python dependencies installed"

# Step 4: Setup Docker services
echo
print_info "Step 4: Starting Docker services..."

# Stop any existing containers
print_info "Stopping any existing containers..."
docker-compose down 2>/dev/null || docker compose down 2>/dev/null || true

# Pull Docker images
print_info "Pulling Docker images..."
if command -v docker-compose >/dev/null 2>&1; then
    docker-compose pull
else
    docker compose pull
fi

# Start services
print_info "Starting database services..."
if command -v docker-compose >/dev/null 2>&1; then
    docker-compose up -d mongodb cassandra redis
else
    docker compose up -d mongodb cassandra redis
fi

print_status "Docker services started"

# Step 5: Wait for services to be ready
echo
print_info "Step 5: Waiting for services to be ready..."

# Wait for MongoDB
print_info "Waiting for MongoDB to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker exec mongodb mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
        print_status "MongoDB is ready"
        break
    fi
    sleep 2
    timeout=$((timeout-2))
done

if [ $timeout -le 0 ]; then
    print_error "MongoDB startup timeout"
    exit 1
fi

# Wait for Cassandra (takes longer to start)
print_info "Waiting for Cassandra to be ready (this may take a few minutes)..."
timeout=180
while [ $timeout -gt 0 ]; do
    if docker exec cassandra cqlsh -e "describe cluster" >/dev/null 2>&1; then
        print_status "Cassandra is ready"
        break
    fi
    sleep 5
    timeout=$((timeout-5))
    if [ $((timeout % 30)) -eq 0 ]; then
        print_info "Still waiting for Cassandra... ($timeout seconds remaining)"
    fi
done

if [ $timeout -le 0 ]; then
    print_error "Cassandra startup timeout"
    print_info "Cassandra might still be starting. You can check with:"
    print_info "docker logs cassandra"
    exit 1
fi

# Wait for Redis
print_info "Waiting for Redis to be ready..."
timeout=30
while [ $timeout -gt 0 ]; do
    if docker exec redis redis-cli ping >/dev/null 2>&1; then
        print_status "Redis is ready"
        break
    fi
    sleep 1
    timeout=$((timeout-1))
done

if [ $timeout -le 0 ]; then
    print_error "Redis startup timeout"
    exit 1
fi

# Step 6: Initialize databases
echo
print_info "Step 6: Initializing databases..."

# Test the system initialization
print_info "Testing system connectivity..."
python -c "
import sys
sys.path.append('src')
from core.database_manager import DatabaseManager

db_manager = DatabaseManager()
if db_manager.connect_all():
    print('✅ All database connections successful')
    status = db_manager.get_data_counts()
    print(f'📊 Current data status: {status}')
    db_manager.close_all_connections()
else:
    print('❌ Database connection failed')
    sys.exit(1)
"

print_status "Database initialization complete"

# Step 7: Start aggregator service (optional)
echo
print_info "Step 7: Starting aggregator service..."

if command -v docker-compose >/dev/null 2>&1; then
    docker-compose up -d aggregator
else
    docker compose up -d aggregator
fi

# Wait for aggregator to be ready
print_info "Waiting for aggregator service..."
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -f http://localhost:5000/health >/dev/null 2>&1; then
        print_status "Aggregator service is ready"
        break
    fi
    sleep 2
    timeout=$((timeout-2))
done

if [ $timeout -le 0 ]; then
    print_warning "Aggregator service might still be starting"
    print_info "Check status with: docker logs cafe-aggregator"
fi

# Step 8: Final verification
echo
print_info "Step 8: Final system verification..."

# Check all containers
print_info "Checking container status..."
if command -v docker-compose >/dev/null 2>&1; then
    docker-compose ps
else
    docker compose ps
fi

echo
print_status "🎉 Setup complete!"
echo
print_info "Next steps:"
echo "  1. Activate virtual environment: source cafe_venv/bin/activate"
echo "  2. Run the demo: python demo_main.py"
echo "  3. Or use the web API: http://localhost:5000/health"
echo
print_info "Useful commands:"
echo "  • Check logs: docker logs <container_name>"
echo "  • Stop services: docker-compose down"
echo "  • Restart services: docker-compose up -d"
echo
print_status "Ready for Monday's demo! 🚀"
