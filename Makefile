# Makefile for Kedai Kopi Dynamic Query System
# ROBD Assignment - Spring 2025 | Group 3

.PHONY: help setup start stop restart demo clean logs status test

# Default target
help:
	@echo "🌟 Kedai Kopi Dynamic Query System - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "Setup Commands:"
	@echo "  make setup     - Complete system setup (run this first)"
	@echo "  make start     - Start all Docker services"
	@echo "  make stop      - Stop all Docker services"
	@echo "  make restart   - Restart all services"
	@echo ""
	@echo "Demo Commands:"
	@echo "  make demo      - Run the interactive demo"
	@echo "  make api       - Test the REST API endpoints"
	@echo ""
	@echo "Maintenance:"
	@echo "  make logs      - Show container logs"
	@echo "  make status    - Check system status"
	@echo "  make clean     - Clean up containers and volumes"
	@echo "  make test      - Run system connectivity tests"
	@echo ""
	@echo "Quick Start: make setup && make demo"

# Complete system setup
setup:
	@echo "🚀 Running complete system setup..."
	@chmod +x setup.sh
	@./setup.sh

# Start Docker services
start:
	@echo "🐳 Starting Docker services..."
	@docker compose up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 30
	@make status

# Stop Docker services
stop:
	@echo "🛑 Stopping Docker services..."
	@docker compose down

# Restart services
restart: stop start

# Run the interactive demo
demo:
	@echo "🎯 Starting interactive demo..."
	@echo "📋 Make sure virtual environment is activated!"
	@echo "   Run: source cafe_venv/bin/activate"
	@echo ""
	@python demo_main.py

# Test API endpoints
api:
	@echo "🌐 Testing REST API endpoints..."
	@echo ""
	@echo "Health Check:"
	@curl -s http://localhost:5000/health | python -m json.tool || echo "❌ API not responding"
	@echo ""
	@echo "System Status:"
	@curl -s http://localhost:5000/api/data/status | python -m json.tool || echo "❌ Status endpoint failed"
	@echo ""
	@echo "Available Schemas:"
	@curl -s http://localhost:5000/api/schemas | python -m json.tool || echo "❌ Schema endpoint failed"

# Show container logs
logs:
	@echo "📋 Container Status:"
	@docker compose ps
	@echo ""
	@echo "📄 Recent Logs:"
	@echo "MongoDB:"
	@docker logs --tail 10 mongodb
	@echo ""
	@echo "Cassandra:"
	@docker logs --tail 10 cassandra
	@echo ""
	@echo "Redis:"
	@docker logs --tail 5 redis
	@echo ""
	@echo "Aggregator:"
	@docker logs --tail 10 cafe-aggregator

# Check system status
status:
	@echo "📊 System Status Check"
	@echo "====================="
	@echo ""
	@echo "🐳 Docker Containers:"
	@docker compose ps
	@echo ""
	@echo "🌐 Network Connectivity:"
	@echo -n "MongoDB (27017): "
	@nc -z localhost 27017 && echo "✅ Connected" || echo "❌ Not accessible"
	@echo -n "Cassandra (9042): "
	@nc -z localhost 9042 && echo "✅ Connected" || echo "❌ Not accessible"
	@echo -n "Redis (6379): "
	@nc -z localhost 6379 && echo "✅ Connected" || echo "❌ Not accessible"
	@echo -n "Aggregator (5000): "
	@nc -z localhost 5000 && echo "✅ Connected" || echo "❌ Not accessible"
	@echo ""
	@echo "💾 Disk Usage:"
	@docker system df
	@echo ""
	@echo "🔍 Quick Health Check:"
	@curl -s http://localhost:5000/health 2>/dev/null | grep -q "healthy" && echo "✅ API responding" || echo "❌ API not responding"

# Run connectivity tests
test:
	@echo "🧪 Running System Tests"
	@echo "======================"
	@echo ""
	@echo "Activating virtual environment and testing..."
	@bash -c "source cafe_venv/bin/activate && python -c '\
import sys; \
sys.path.append(\"src\"); \
from core.database_manager import DatabaseManager; \
print(\"🔌 Testing database connections...\"); \
db = DatabaseManager(); \
success = db.connect_all(); \
if success: \
    print(\"✅ All databases connected\"); \
    counts = db.get_data_counts(); \
    print(f\"📊 Data counts: {counts}\"); \
    db.close_all_connections(); \
    print(\"✅ Test completed successfully\"); \
else: \
    print(\"❌ Database connection failed\"); \
    exit(1) \
'"

# Load sample data
load-data:
	@echo "📥 Loading sample data..."
	@bash -c "source cafe_venv/bin/activate && python -c '\
import sys; \
sys.path.append(\"src\"); \
from core.database_manager import DatabaseManager; \
from data_loaders.data_loader import DataLoader; \
print(\"📊 Loading sample data...\"); \
db = DatabaseManager(); \
if db.connect_all(): \
    loader = DataLoader(db); \
    result = loader.load_all_sample_data(); \
    print(f\"📋 Load results: {result}\"); \
    db.close_all_connections(); \
    print(\"✅ Data loading completed\"); \
else: \
    print(\"❌ Could not connect to databases\"); \
'"

# Clean up everything
clean:
	@echo "🧹 Cleaning up system..."
	@echo "⚠️  This will remove all containers and data!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@docker compose down -v
	@docker system prune -f
	@echo "✅ Cleanup complete"

# Quick development setup
dev-setup:
	@echo "🛠️ Development setup..."
	@python -m venv cafe_venv
	@bash -c "source cafe_venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"
	@echo "✅ Virtual environment ready"
	@echo "💡 Run: source cafe_venv/bin/activate"

# Show useful information
info:
	@echo "📚 Kedai Kopi Dynamic Query System - Information"
	@echo "=============================================="
	@echo ""
	@echo "🎯 Project Purpose:"
	@echo "   Multi-database query optimization demonstration"
	@echo "   MongoDB (documents) + Cassandra (columns) + Redis (cache)"
	@echo ""
	@echo "🌐 Access Points:"
	@echo "   CLI Demo: python demo_main.py"
	@echo "   REST API: http://localhost:5000"
	@echo "   MongoDB Admin: http://localhost:8081 (with admin-tools profile)"
	@echo ""
	@echo "📁 Key Files:"
	@echo "   demo_main.py              - Main demo entry point"
	@echo "   src/interfaces/cli_interface.py - Interactive CLI"
	@echo "   aggregator/app.py         - REST API service"
	@echo "   docker-compose.yml        - Service definitions"
	@echo ""
	@echo "🎓 Assignment Requirements:"
	@echo "   ✅ 3 Query types (DB1, DB2, Combined)"
	@echo "   ✅ 2 Scenarios (Optimized vs Non-optimized)"
	@echo "   ✅ Different NoSQL types (Document + Column)"
	@echo "   ✅ Performance comparisons with real data"
	@echo ""
	@echo "📞 Quick Commands:"
	@echo "   make setup    - Complete setup"
	@echo "   make demo     - Run interactive demo"
	@echo "   make status   - Check system health"
	@echo "   make logs     - View container logs"

# Deployment helpers
deploy-check:
	@echo "🚀 Pre-deployment checklist..."
	@echo "Checking Python version..."
	@python3 --version | grep -E "3\.(11|12)" || (echo "❌ Need Python 3.11+"; exit 1)
	@echo "✅ Python version OK"
	@echo "Checking Docker..."
	@docker --version || (echo "❌ Docker not found"; exit 1)
	@echo "✅ Docker OK"
	@echo "Checking Docker Compose..."
	@docker compose version || docker-compose --version || (echo "❌ Docker Compose not found"; exit 1)
	@echo "✅ Docker Compose OK"
	@echo "Checking required files..."
	@test -f requirements.txt || (echo "❌ requirements.txt missing"; exit 1)
	@test -f demo_main.py || (echo "❌ demo_main.py missing"; exit 1)
	@test -f docker-compose.yml || (echo "❌ docker-compose.yml missing"; exit 1)
	@echo "✅ All files present"
	@echo "🎉 Ready for deployment!"
