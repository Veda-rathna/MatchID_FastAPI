from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse
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
    cluster_name = StringField(required=True)
    created_on = DateTimeField(required=True)
    last_paid_on = DateTimeField(default=None, null=True)
    valid_till = DateTimeField(default=None, null=True)
    is_trial = BooleanField(default=False)
    meta = {'collection': 'match_ids'}

def get_cache_key(api_key: str, match_id: str) -> str:
    return f"match_id:{api_key}:{match_id}"

def _get_cluster_from_user(cluster_name: str, api_key: str = None):
    """Retrieve cluster from UserProfile by cluster_name and optional api_key."""
    user = UserProfile.objects(clusters__cluster_name=cluster_name).first()
    if not user:
        return None, None
    cluster = next((c for c in user.clusters if c.cluster_name == cluster_name), None)
    if not cluster and api_key:
        # Fallback to Django Cluster model if MongoDB cluster not found
        from .models import Cluster  # Dynamically import Django model
        django_cluster = Cluster.objects.filter(cluster_name=cluster_name).first()
        if django_cluster:
            cluster = ClusterDetails(
                cluster_name=django_cluster.cluster_name,
                cluster_price=float(django_cluster.cluster_price),
                timeline_days=django_cluster.timeline_days,
                api_key=django_cluster.api_key,
                match_id_type='admin_generated',
                trial_period=django_cluster.trial_period
            )
    return user, cluster

def serialize_match_id(match_id_obj):
    """Serialize MatchId object to dictionary, excluding _id field"""
    if not match_id_obj:
        return None
    
    # Convert to dict and handle datetime objects
    match_id_dict = {
        "match_id": match_id_obj.match_id,
        "cluster_name": match_id_obj.cluster_name,
        "created_on": match_id_obj.created_on.isoformat() if match_id_obj.created_on else None,
        "last_paid_on": match_id_obj.last_paid_on.isoformat() if match_id_obj.last_paid_on else None,
        "valid_till": match_id_obj.valid_till.isoformat() if match_id_obj.valid_till else None,
        "is_trial": match_id_obj.is_trial
    }
    
    return match_id_dict

@app.get("/check-match-id/")
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
            
            # Determine status based on cached data
            is_active = False
            is_trial = data.get("is_trial", False)
            
            # Convert ISO strings back to datetime objects for comparison
            created_on = datetime.fromisoformat(data.get("created_on")) if data.get("created_on") else None
            valid_till = datetime.fromisoformat(data.get("valid_till")) if data.get("valid_till") else None
            
            # Get cluster details to determine trial period
            user, cluster = _get_cluster_from_user(data.get("cluster_name"), api_key)
            if not cluster or cluster.api_key != api_key:
                raise HTTPException(status_code=404, detail="Invalid API key")
            
            # Check if match ID is active
            if valid_till and datetime.now() < valid_till:
                is_active = True
            
            # Check if in trial period
            trial_period = cluster.trial_period if cluster else 0
            in_trial = False
            if is_trial and created_on and trial_period > 0:
                trial_end_date = created_on + timedelta(days=trial_period)
                in_trial = datetime.now() <= trial_end_date
            
            if is_active:
                if is_trial and in_trial:
                    return JSONResponse(content={"status": "Trial Active"}, status_code=status.HTTP_201_CREATED)
                else:
                    return JSONResponse(content={"status": "Paid Active"}, status_code=status.HTTP_200_OK)
            else:
                raise HTTPException(status_code=410, detail="Match ID expired")
        
        # Cache miss - fetch from MongoDB
        print(f"Cache miss for {cache_key}")
        
        # Check match ID
        match_id_obj = MatchId.objects(match_id=match_id).first()
        if not match_id_obj:
            raise HTTPException(status_code=422, detail="Match ID not found")
        
        # Validate API key against cluster
        user, cluster = _get_cluster_from_user(match_id_obj.cluster_name, api_key)
        if not cluster or cluster.api_key != api_key:
            raise HTTPException(status_code=404, detail="Invalid API key")
        
        # Check if active
        expiry_date = match_id_obj.valid_till if match_id_obj.valid_till else None
        is_active = expiry_date and datetime.now() < expiry_date
        trial_period = cluster.trial_period if cluster else 0
        in_trial = False
        if match_id_obj.is_trial and trial_period > 0:
            trial_end_date = match_id_obj.created_on + timedelta(days=trial_period)
            in_trial = datetime.now() <= trial_end_date
        
        # Store all fields except _id in cache (1 hour TTL)
        cache_data = serialize_match_id(match_id_obj)
        redis_client.setex(cache_key, 3600, json.dumps(cache_data))
        print(f"Cache set for {cache_key}")
        
        if is_active:
            if match_id_obj.is_trial and in_trial:
                return JSONResponse(content={"status": "Trial Active"}, status_code=status.HTTP_201_CREATED)
            else:
                return JSONResponse(content={"status": "Paid Active"}, status_code=status.HTTP_200_OK)
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