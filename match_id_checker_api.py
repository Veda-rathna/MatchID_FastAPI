from fastapi import FastAPI, HTTPException, Query, status
from mongoengine import connect, Document, StringField, IntField, DateTimeField, FloatField, EmbeddedDocument, EmbeddedDocumentListField, DictField, BooleanField
from datetime import datetime, timedelta
import uvicorn
import os
import redis
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Match ID Checker API",
    description="API to check match ID status via MongoDB with API key authentication",
    version="1.0.0"
)

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_DATABASE_URL")
if MONGODB_URI is None:
    raise ValueError("MONGODB_DATABASE_URL is not set in environment variables")
connect(host=MONGODB_URI)

# Redis connection
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

# Define MongoDB models
class ClusterDetails(EmbeddedDocument):
    cluster_name = StringField(required=True)
    cluster_price = FloatField()
    timeline_days = IntField(min_value=1, max_value=30, default=30)
    api_key = StringField(required=True)
    match_id_type = StringField(default="admin_generated", choices=["admin_generated", "user_created"])
    trial_period = IntField(min_value=0, max_value=7, default=0)

class UserProfile(Document):
    user_id = StringField(required=True, unique=True)
    email = StringField(required=True)
    username = StringField(required=True)
    clusters = EmbeddedDocumentListField(ClusterDetails, default=[])
    bank_details = DictField()
    meta = {'collection': 'users'}

class MatchId(Document):
    match_id = StringField(required=True, unique=True)
    api_key = StringField(required=True)
    cluster_name = StringField(required=True)
    timestamp = DateTimeField(required=True)
    days_valid = IntField(required=True)
    is_trial = BooleanField(default=False)
    meta = {'collection': 'match_ids'}

def get_cache_key(api_key: str, match_id: str) -> str:
    return f"match_id:{api_key}:{match_id}"

@app.get("/check-match-id/", status_code=status.HTTP_200_OK)
async def check_match_id(
    api_key: str = Query(..., description="The API key associated with the cluster"),
    match_id: str = Query(..., description="The match ID to verify")
):
    """
    Check match ID status with Redis caching.
    Returns:
    - 200 OK: Paid Active
    - 201 Created: Trial Active
    - 404: API key not found
    - 410: Match ID expired
    - 422: Match ID not found
    - 500: Server error
    """
    if not api_key or not match_id:
        raise HTTPException(status_code=400, detail="API key and match ID are required")

    cache_key = get_cache_key(api_key, match_id)
    
    try:
        # Check Redis cache first
        cached_data = redis_client.get(cache_key)
        
        if cached_data is not None:
            data = json.loads(cached_data)
            print(f"Cache hit for {cache_key}")
            
            if data.get("is_active"):
                if data.get("is_trial"):
                    return {"status": "Trial Active"}, status.HTTP_201_CREATED
                else:
                    return {"status": "Paid Active"}, status.HTTP_200_OK
            else:
                match_id_obj = MatchId.objects(match_id=match_id, api_key=api_key).first()
                if match_id_obj:
                    expiry_date = match_id_obj.timestamp + timedelta(days=match_id_obj.days_valid)
                    is_active = datetime.now() < expiry_date
                    user = UserProfile.objects(clusters__api_key=api_key).first()
                    cluster = next((c for c in user.clusters if c.cluster_name == match_id_obj.cluster_name), None) if user else None
                    trial_period = cluster.trial_period if cluster else 0
                    trial_end_date = match_id_obj.timestamp + timedelta(days=trial_period) if trial_period > 0 else None
                    
                    if is_active != data.get("is_active") or match_id_obj.is_trial != data.get("is_trial"):
                        cache_data = {
                            "is_active": is_active,
                            "is_trial": match_id_obj.is_trial,
                            "status": "Trial Active" if is_active and match_id_obj.is_trial and datetime.now() <= trial_end_date else "Paid Active" if is_active else "Inactive"
                        }
                        redis_client.setex(cache_key, 3600, json.dumps(cache_data))
                        print(f"Cache updated for {cache_key}")
                    
                    if is_active:
                        if match_id_obj.is_trial and datetime.now() <= trial_end_date:
                            return {"status": "Trial Active"}, status.HTTP_201_CREATED
                        else:
                            return {"status": "Paid Active"}, status.HTTP_200_OK
                    else:
                        raise HTTPException(status_code=410, detail="Match ID expired")
                raise HTTPException(status_code=422, detail="Match ID not found")
        
        # Cache miss - fetch from MongoDB
        print(f"Cache miss for {cache_key}")
        
        # Check if API key exists
        user = UserProfile.objects(clusters__api_key=api_key).first()
        if not user:
            raise HTTPException(status_code=404, detail="API key not found")
        
        # Check match ID
        match_id_obj = MatchId.objects(match_id=match_id, api_key=api_key).first()
        if not match_id_obj:
            raise HTTPException(status_code=422, detail="Match ID not found")
        
        # Check if active
        expiry_date = match_id_obj.timestamp + timedelta(days=match_id_obj.days_valid)
        is_active = datetime.now() < expiry_date
        cluster = next((c for c in user.clusters if c.cluster_name == match_id_obj.cluster_name), None)
        trial_period = cluster.trial_period if cluster else 0
        trial_end_date = match_id_obj.timestamp + timedelta(days=trial_period) if trial_period > 0 else None
        
        # Store in cache (1 hour TTL)
        cache_data = {
            "is_active": is_active,
            "is_trial": match_id_obj.is_trial,
            "status": "Trial Active" if is_active and match_id_obj.is_trial and datetime.now() <= trial_end_date else "Paid Active" if is_active else "Inactive"
        }
        redis_client.setex(cache_key, 3600, json.dumps(cache_data))
        print(f"Cache set for {cache_key}")
        
        if is_active:
            if match_id_obj.is_trial and datetime.now() <= trial_end_date:
                return {"status": "Trial Active"}, status.HTTP_201_CREATED
            else:
                return {"status": "Paid Active"}, status.HTTP_200_OK
        else:
            raise HTTPException(status_code=410, detail="Match ID expired")
            
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health/")
async def health_check():
    """Simple health check endpoint"""
    try:
        MatchId.objects.count()
        redis_client.ping()
        return  # 200 OK
    except Exception:
        raise HTTPException(status_code=500, detail="Health check failed")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("match_id_checker_api:app", host="0.0.0.0", port=port, reload=True)