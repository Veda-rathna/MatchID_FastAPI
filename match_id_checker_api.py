from fastapi import FastAPI, HTTPException, Query
from mongoengine import connect, Document, StringField, IntField, DateTimeField, FloatField, EmbeddedDocument, EmbeddedDocumentListField, DictField
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
if not MONGODB_URI:
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
    cluster_timeline = StringField()
    api_key = StringField(required=True)

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
    meta = {'collection': 'match_ids'}

def get_cache_key(api_key: str, match_id: str) -> str:
    return f"match_id:{api_key}:{match_id}"

@app.get("/check-match-id/")
async def check_match_id(
    api_key: str = Query(..., description="The API key associated with the cluster"),
    match_id: str = Query(..., description="The match ID to verify")
):
    """
    Check match ID status with Redis caching and return only status codes.
    """
    if not api_key or not match_id:
        raise HTTPException(status_code=400)

    cache_key = get_cache_key(api_key, match_id)
    
    try:
        # Check Redis cache first
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            data = json.loads(cached_data)
            print(f"Cache hit for {cache_key}")
            
            if data["is_active"]:
                return  # 200 OK from cache
            else:
                # Check if status needs update
                match_id_obj = MatchId.objects(match_id=match_id, api_key=api_key).first()
                if match_id_obj:
                    expiry_date = match_id_obj.timestamp + timedelta(days=match_id_obj.days_valid)
                    is_active = datetime.now() < expiry_date
                    
                    if is_active != data["is_active"]:
                        # Update cache if status changed
                        cache_data = {"is_active": is_active}
                        redis_client.setex(cache_key, 3600, json.dumps(cache_data))
                        print(f"Cache updated for {cache_key}")
                    
                    if is_active:
                        return  # 200 OK
                    else:
                        raise HTTPException(status_code=410)
                raise HTTPException(status_code=422)
        
        # Cache miss - fetch from MongoDB
        print(f"Cache miss for {cache_key}")
        
        # Check if API key exists
        user = UserProfile.objects(clusters__api_key=api_key).first()
        if not user:
            raise HTTPException(status_code=404)
        
        # Check match ID
        match_id_obj = MatchId.objects(match_id=match_id, api_key=api_key).first()
        if not match_id_obj:
            raise HTTPException(status_code=422)
        
        # Check if active
        expiry_date = match_id_obj.timestamp + timedelta(days=match_id_obj.days_valid)
        is_active = datetime.now() < expiry_date
        
        # Store in cache (1 hour TTL)
        cache_data = {"is_active": is_active}
        redis_client.setex(cache_key, 3600, json.dumps(cache_data))
        print(f"Cache set for {cache_key}")
        
        if is_active:
            return  # 200 OK
        else:
            raise HTTPException(status_code=410)
            
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500)

@app.get("/health/")
async def health_check():
    """Simple health check endpoint"""
    try:
        MatchId.objects.count()
        redis_client.ping()
        return  # 200 OK
    except Exception:
        raise HTTPException(status_code=500)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("match_id_checker_api:app", host="0.0.0.0", port=port, reload=True)