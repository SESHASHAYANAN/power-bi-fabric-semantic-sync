"""Direct test to see exact 403 error source"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Print loaded env vars
print("=== Environment Variables Loaded ===")
print(f"TENANT_ID: {os.getenv('FABRIC_TENANT_ID', 'NOT SET')}")
print(f"CLIENT_ID: {os.getenv('FABRIC_CLIENT_ID', 'NOT SET')}")
print(f"CLIENT_SECRET: {'SET' if os.getenv('FABRIC_CLIENT_SECRET') else 'NOT SET'}")
print(f"WORKSPACE_ID: {os.getenv('FABRIC_WORKSPACE_ID', 'NOT SET')}")

# Step 1: Authenticate
print("\n=== Step 1: Authentication ===")
auth_url = f"https://login.microsoftonline.com/{os.getenv('FABRIC_TENANT_ID')}/oauth2/v2.0/token"

payload = {
    "grant_type": "client_credentials",
    "client_id": os.getenv("FABRIC_CLIENT_ID"),
    "client_secret": os.getenv("FABRIC_CLIENT_SECRET"),
    "scope": "https://api.fabric.microsoft.com/.default",
}

try:
    resp = requests.post(auth_url, data=payload, timeout=30)
    print(f"Auth Response Status: {resp.status_code}")
    
    if resp.status_code == 200:
        token = resp.json().get("access_token")
        print("✅ Got access token!")
        
        # Step 2: Try API call
        print("\n=== Step 2: API Call ===")
        workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
        api_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticmodels"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        api_resp = requests.get(api_url, headers=headers, timeout=30)
        print(f"API Response Status: {api_resp.status_code}")
        print(f"API Response Body: {api_resp.text[:500]}")
        
        if api_resp.status_code == 403:
            print("\n❌ 403 ERROR - Possible causes:")
            print("1. Service Principal not added to Fabric workspace")
            print("2. Wrong workspace ID")
            print("3. API permissions not granted admin consent")
    else:
        print(f"❌ Auth failed: {resp.text}")
        
except Exception as e:
    print(f"❌ Error: {e}")
