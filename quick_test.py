"""Quick test for Fabric API 403 error"""
import os
from fabric_snowflake_sync import FabricApiClient

print("Testing Fabric API Connection...")
print("=" * 60)

client = FabricApiClient()

# Test authentication
print("\n1. Testing Authentication...")
auth_success = client.authenticate()

if auth_success:
    print("✅ Authentication successful!")
    
    # Test API access
    print("\n2. Testing API Access - Getting Semantic Models...")
    models = client.get_semantic_models()
    
    if models:
        print(f"✅ SUCCESS! Found {len(models)} models:")
        for model in models:
            print(f"   - {model.get('displayName', 'N/A')}")
    else:
        print("⚠️  No models found or API returned empty list")
        print("   This could mean:")
        print("   - No semantic models in workspace")
        print("   - API returned 403 (check permissions)")
        print("   - Check semantic_sync.log for details")
else:
    print("❌ Authentication failed!")
    print("   Check your credentials in .env file")
    
print("\n" + "=" * 60)
print("Check semantic_sync.log for detailed error messages")
