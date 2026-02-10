# test_db_connection.py
"""
Test if database.py is configured correctly
Run this from DB folder: python test_db_connection.py
"""

try:
    print("🔍 Testing database.py...")
    from database import engine, Base, SessionLocal
    print("✅ Successfully imported from database.py")
    
    print("\n🔍 Testing database connection...")
    connection = engine.connect()
    print("✅ Database connection successful!")
    connection.close()
    
    print("\n🔍 Checking Base object...")
    print(f"Base type: {type(Base)}")
    print("✅ Base object exists")
    
    print("\n✅ All checks passed! You can now create tables.")
    
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure database.py exists in D:\\Final\\DB\\")
    print("2. Make sure database.py has: Base = declarative_base()")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nTroubleshooting:")
    print("1. Check if PostgreSQL is running")
    print("2. Check .env file has correct DB credentials")
    print("3. Check if database 'seo_reports' exists in PostgreSQL")