#!/usr/bin/env python3
"""
Kedai Kopi Nusantara - Dynamic Multi-Database Query System
Main entry point for professor demonstrations

ROBD Assignment - Spring 2025
Group 3 - Multi-Database Architecture

This system demonstrates dynamic NoSQL query optimization across
MongoDB (document store) and Cassandra (column store) with
real performance comparisons.

Usage:
    python demo_main.py

Features:
- Handle ANY professor query combination
- Real-time performance optimization comparisons
- Educational explanations of NoSQL optimization strategies
- Professional demonstration interface
"""

import os
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_project_paths():
    """Setup Python paths for the project structure"""
    project_root = Path(__file__).parent
    src_path = project_root / 'src'

    # Add src directory to Python path
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    return project_root, src_path

def check_dependencies():
    """Check if all required dependencies are available"""
    required_packages = [
        'pymongo',
        'cassandra-driver',
        'redis'
    ]

    missing_packages = []
    for package in required_packages:
        try:
            if package == 'cassandra-driver':
                import cassandra
            else:
                __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print("❌ Missing required packages:")
        for package in missing_packages:
            print(f"   - {package}")
        print("\nInstall with: pip install " + " ".join(missing_packages))
        return False

    return True

def check_docker_services():
    """Check if Docker services are running"""
    print("🐳 Checking Docker services...")

    # This is a basic check - in a real implementation you might use docker-py
    try:
        import socket

        services = [
            ('MongoDB', 'localhost', 27017),
            ('Cassandra', 'localhost', 9042),
            ('Redis', 'localhost', 6379)
        ]

        all_running = True
        for service_name, host, port in services:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                print(f"   ✅ {service_name}: Running on {host}:{port}")
            else:
                print(f"   ❌ {service_name}: Not accessible on {host}:{port}")
                all_running = False

        if not all_running:
            print("\n💡 To start Docker services:")
            print("   sudo docker compose up -d")
            return False

        return True

    except Exception as e:
        print(f"   ⚠️ Could not check Docker services: {e}")
        print("   Please ensure Docker containers are running")
        return False

def load_sample_data():
    """Load sample data into databases if needed"""
    print("📥 Checking sample data...")
    print("⚠️  Data loading temporarily disabled to avoid import issues")
    print("   Use Option 4 in the main menu to manually load data")
    return True

def display_welcome_message():
    """Display welcome message with system information"""
    welcome = """
╔══════════════════════════════════════════════════════════════════╗
║          🌟 KEDAI KOPI NUSANTARA - DYNAMIC QUERY SYSTEM 🌟        ║
║                                                                  ║
║                    ROBD Assignment - Spring 2025                 ║
║                         Group 3 Submission                      ║
║                                                                  ║
║  📋 SYSTEM CAPABILITIES:                                         ║
║    • Handle ANY database query combination                       ║
║    • Real-time performance optimization comparisons             ║
║    • MongoDB (document) + Cassandra (column) + Redis (cache)    ║
║    • Educational NoSQL optimization demonstrations              ║
║                                                                  ║
║  🎯 PROFESSOR DEMO READY:                                        ║
║    • 3 Required query scenarios (DB1, DB2, Combined)           ║
║    • Dynamic query wizard for curveball questions              ║
║    • Professional performance analysis with concrete data       ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """
    print(welcome)

def main():
    """Main entry point for the demonstration system"""

    # Setup project structure
    project_root, src_path = setup_project_paths()

    # Display welcome
    display_welcome_message()

    # Check dependencies
    print("🔍 Checking system requirements...")
    if not check_dependencies():
        print("\n❌ Missing dependencies - please install required packages")
        sys.exit(1)

    # Check Docker services
    if not check_docker_services():
        print("\n❌ Docker services not running - please start containers")
        sys.exit(1)

    # Load sample data
    load_sample_data()

    # Start the CLI interface
    print("\n🚀 Starting Dynamic Query System...")
    print("   Ready for professor demonstration!")

    try:
        # Import with correct path handling
        sys.path.append('src')
        try:
            from interfaces.cli_interface import CLIInterface
        except ImportError:
            from src.interfaces.cli_interface import CLIInterface

        cli = CLIInterface()
        cli.main_menu()

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Please ensure all source files are in the correct structure")
        print("   Current working directory:", os.getcwd())
        print("   Looking for: src/interfaces/cli_interface.py")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n👋 System shutdown requested")
    except Exception as e:
        logger.exception("Unexpected error in main")
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        print("🔌 Cleaning up resources...")
        print("✅ System shutdown complete")

if __name__ == "__main__":
    main()
