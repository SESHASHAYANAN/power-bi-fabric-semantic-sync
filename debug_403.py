"""
Debug script to identify the source of 403 errors
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_fabric_auth():
    """Test Fabric API authentication"""
    print("=" * 60)
    print("TEST 1: Fabric API Authentication")
    print("=" * 60)
    
    tenant_id = os.getenv("FABRIC_TENANT_ID")
    client_id = os.getenv("FABRIC_CLIENT_ID")
    client_secret = os.getenv("FABRIC_CLIENT_SECRET")
    
    print(f"Tenant ID: {tenant_id}")
    print(f"Client ID: {client_id}")
    print(f"Client Secret: {'*' * 10}{client_secret[-4:] if client_secret else 'NOT SET'}")
    
    auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.fabric.microsoft.com/.default",
    }
    
    try:
        response = requests.post(
            auth_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        
        print(f"\nStatus Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: Authentication successful!")
            token_data = response.json()
            print(f"Token Type: {token_data.get('token_type')}")
            print(f"Expires In: {token_data.get('expires_in')} seconds")
            return token_data.get("access_token")
        else:
            print(f"❌ FAILED: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

def test_fabric_api(access_token):
    """Test Fabric API access"""
    if not access_token:
        print("\n❌ Skipping API test - no access token")
        return
    
    print("\n" + "=" * 60)
    print("TEST 2: Fabric API - Get Semantic Models")
    print("=" * 60)
    
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    print(f"Workspace ID: {workspace_id}")
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticmodels"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        print(f"\nStatus Code: {response.status_code}")
        
        if response.status_code == 200:
            models = response.json().get("value", [])
            print(f"✅ SUCCESS: Found {len(models)} semantic models")
            for model in models:
                print(f"  - {model.get('displayName', 'N/A')}")
        elif response.status_code == 403:
            print("❌ 403 FORBIDDEN - Permission denied!")
            print("Possible causes:")
            print("  1. Service Principal lacks 'Workspace.ReadWrite.All' permission")
            print("  2. Service Principal not added to Fabric workspace")
            print("  3. Incorrect workspace ID")
            print(f"\nResponse: {response.text}")
        elif response.status_code == 401:
            print("❌ 401 UNAUTHORIZED - Token invalid or expired")
            print(f"Response: {response.text}")
        else:
            print(f"❌ FAILED: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")

def test_backend_server():
    """Test backend server connectivity"""
    print("\n" + "=" * 60)
    print("TEST 3: Backend Server Health Check")
    print("=" * 60)
    
    backend_url = os.getenv("BACKEND_URL", "http://localhost:5000")
    
    try:
        response = requests.get(f"{backend_url}/api/health", timeout=5)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: Backend server is running")
            print(f"Response: {response.json()}")
        else:
            print(f"❌ FAILED: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Backend server not reachable")
        print("Make sure backend_server.py is running on port 5000")
    except Exception as e:
        print(f"❌ ERROR: {e}")

def test_file_upload():
    """Test file upload endpoint"""
    print("\n" + "=" * 60)
    print("TEST 4: File Upload Endpoint")
    print("=" * 60)
    
    backend_url = os.getenv("BACKEND_URL", "http://localhost:5000")
    
    # Create a simple test CSV
    import io
    csv_content = "name,value\ntest,123"
    files = {'file': ('test.csv', io.StringIO(csv_content), 'text/csv')}
    
    try:
        response = requests.post(f"{backend_url}/api/test-file", files=files, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: File upload working")
            print(f"Response: {response.json()}")
        elif response.status_code == 403:
            print("❌ 403 FORBIDDEN on file upload!")
            print("This suggests CORS or authentication issue with backend")
            print(f"Response: {response.text}")
        else:
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Backend server not reachable")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    print("\n🔍 Starting 403 Error Diagnostic\n")
    
    # Test 1: Fabric Authentication
    access_token = test_fabric_auth()
    
    # Test 2: Fabric API Access
    test_fabric_api(access_token)
    
    # Test 3: Backend Server
    test_backend_server()
    
    # Test 4: File Upload
    test_file_upload()
    
    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)
    print("\nNext Steps:")
    print("1. If Test 1 fails → Check Fabric credentials in .env")
    print("2. If Test 2 fails with 403 → Check Service Principal permissions")
    print("3. If Test 3 fails → Start backend server: python backend_server.py")
    print("4. If Test 4 fails → Check CORS settings in backend_server.py")
