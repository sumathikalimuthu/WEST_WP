# debug_import.py
import os
import sys

print("=" * 50)
print("DEBUG: Checking Python Import System")
print("=" * 50)

print("\n1. Current working directory:")
print(f"   {os.getcwd()}")

print("\n2. This script's location:")
print(f"   {os.path.abspath(__file__)}")

print("\n3. Python sys.path (where Python looks for modules):")
for i, path in enumerate(sys.path):
    print(f"   [{i}] {path}")

print("\n4. Files in current directory:")
files = os.listdir('.')
for f in files:
    print(f"   - {f}")

print("\n5. Checking if database.py exists:")
if os.path.exists('database.py'):
    print("   ✅ database.py EXISTS in current directory")
    print(f"   📏 File size: {os.path.getsize('database.py')} bytes")
else:
    print("   ❌ database.py NOT FOUND")

print("\n6. Trying to import database:")
try:
    import database
    print("   ✅ SUCCESS! database module imported")
    print(f"   📍 Module location: {database.__file__}")
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    
print("\n" + "=" * 50)