# create_tables.py
"""
Direct execution script to create database tables
Run from DB folder: python create_tables.py
"""
import os
import sys

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
sys.path.insert(0, script_dir)

print("📍 Current directory:", os.getcwd())
print("📂 Files in current directory:", os.listdir('.'))

try:
    # Method 1: Direct exec
    print("\n🔧 Loading database configuration...")
    with open('database.py', 'r', encoding='utf-8') as f:
        exec(f.read(), globals())
    
    print("✅ Database config loaded")
    
    # Method 2: Load models
    print("🔧 Loading models...")
    with open('models.py', 'r', encoding='utf-8') as f:
        exec(f.read(), globals())
    
    print("✅ Models loaded")
    
    # Create tables
    print("\n🔧 Creating database tables...")
    Base.metadata.create_all(bind=engine)
    
    print("✅ Database tables created successfully!")
    print("\nCreated tables:")
    print("  - ga4_metrics")
    print("  - gsc_metrics")
    print("  - indexing_status")
    print("  - seo_reports")
    print("  - preprocessed_metrics")
    print("\n🚀 You can now run your Celery workers!")
    
except FileNotFoundError as e:
    print(f"❌ File not found: {e}")
    print("Make sure you're running from D:\\Final\\DB\\ folder")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()