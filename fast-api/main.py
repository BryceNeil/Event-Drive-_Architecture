from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
import json

# Import consumer class
import consumers

app = FastAPI()

# Middleware allows app to successfully connect with backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=['https://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

redis = get_redis_connection(
    # Don't need to add username as it is a default username
    host="redis-15630.c258.us-east-1-4.ec2.cloud.redislabs.com",
    port=15630,
    password="QPCsyM7TBZaJZFmW1hs5MWGogrbNLlgF",
    decode_responses=True
)

# Deliver class creation which extends form hash model - This is an OBJECT
# Object has two properties - budget, and notes
class Delivery(HashModel):
    budget: int = 0
    notes: str = ''

    # To connect the object model to the redis database we use the Meta class
    class Meta: 
        database = redis


# Second object - EVENT OBJECT
# Has 3 attributes: delivery_id, type, data
class Event(HashModel):
    delivery_id: str = ''
    type: str
    data: str

    class Meta: 
        database = redis

@app.get('/deliveries/{pk}/status')
async def get_state(pk: str):
    state = None
    
    if state is not None:
        return json.loads(state)
    
    state = build_state(pk)
    return {}


def build_state(pk: str):
    pks = Event.all_pks()
    return pks
    all_events = [Event.get(pk) for pk in pks]
    events = [event for event in all_events if event.delivery_id == pk]
    state = {}

    for event in events:
        state = consumers.CONSUMERS[event.type](state, event)
    return events

# Get request
@app.get('/deliveries/{pk}/status')
async def get_state(pk: str):
    # Fetching the state from redis
    state = redis.get(f'delivery:{pk}') 

    # Debugging 
    print("DEBUG: pk is - ", pk)
    print("DEBUG: state is - ", state)
    if state is not None:   
        return json.loads(state) #loads in dictionary form instead of as list 
    
    # If it is None, it will return an empty object
    return {}

# Post request to this path in the DB
@app.post('/deliveries/create')
# Asynchronous funciton
async def create(request: Request):
    body = await request.json()
    delivery = Delivery(budget=body['data']['budget'], notes=body['data']['notes']).save()
    event = Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data'])).save()  # Convert form dictionary to string by using json.dumps()    
                                                                                                   # Must add .save() to create an event
    # Call create_delivery function from the consumers class
    state = consumers.create_delivery({}, event) # {} denotes empty sring as first parameter
    # Need to store the state in the redis cache 
    redis.set(f'delivery:{delivery.pk}', json.dumps(state))
    return state


@app.post('/event')
async def dispatch(request: Request):
    body = await request.json()
    delivery_id = body['delivery_id']
    event = Event(delivery_id=delivery_id, type=body['type'], data=json.dumps(body['data'])).save()  # Convert form dictionary to string by using json.dumps()    
    
    # Manipulating the state
    # STEP 1 - Get the state
    state = await get_state(delivery_id) # Calling Asynchronous function, must use await
    new_state = consumers.CONSUMERS[event.type](state, event)
    redis.set(f'delivery:{delivery_id}', json.dumps(state)) 
    return new_state