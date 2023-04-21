# Event file 
import json  
from fastapi import HTTPException

def create_delivery(state, event):
    # Get data
    data = json.loads(event.data) # Loads data from the main class
    return {
        "id": event.delivery_id,
        "budget": int(data["budget"]),
        "notes": data["notes"],
        "status": "ready",

    }

def start_delivery(state, event):
    # if status is already defined raises exeption when we try to modify it to same thing. If already in progress can't put it in progress again
    if state['status'] != 'ready':
        raise HTTPException(status_code=400, detail="Delivery already started")

    # state['status'] = 'active'    we can use this but we will use more elegant way
    return state | { # vertical line denotes a pipe - the pipe operator updates the status value in 
                     # create_delivery function to our new status value defined below
        "status": "active",

    }

def pickup_products(state, event):
    data = json.loads(event.data)
    new_budget = state["budget"] - int(data['purchase_price']) * int(data['quantity'])

    if new_budget < 0:
        raise HTTPException(status_code=400, detail="Not enough budget")

    return state | {
        "budget": new_budget,
        "purchase_price": int(data['purchase_price']),
        "quantity": int(data['quantity']),
        "status": "collected"
    }

def deliver_products(state, event):
    data = json.loads(event.data)
    new_budget = state["budget"] + int(data['sell_price']) * int(data['quantity'])
    new_quantity = state["quantity"] - int(data['quantity']) 

    if new_quantity < 0:
        raise HTTPException(status_code=400, detail="Not enough quantity")

    return state | {
        "budget": new_budget,
        "purchase_price": int(data['sell_price']),
        "quantity": int(data['quantity']), #new_quantity,
        "status": "completed"
    }
    
def increase_budget(state, event):
    data = json.loads(event.data)
    state['budget'] += int(data['budget'])
    return state

# Define constant so that in main class we have a dynamic way of calling a function based on the type
CONSUMERS = {
    "CREATE_DELIVERY": create_delivery,
    "START_DELIVERY": start_delivery,
    "PICKUP_PRODUCTS": pickup_products,
    "DELIVER_PRODUCTS": deliver_products,
    "INCREASE_BUDGET": increase_budget
}