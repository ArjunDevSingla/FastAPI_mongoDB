from fastapi import FastAPI, status
from pymongo import MongoClient
import connection
from pydantic import BaseModel
from typing import List

DB = "customer"
MSG_COLLECTION = "messages"

class Message(BaseModel):
    channel: str
    author: str
    text: str

app = FastAPI()

@app.get("/status")
def getStatus():
    return{"status": "running"}

@app.get("/channels", response_model=List[str])
def getChannels():
    with MongoClient("mongodb+srv://arjun1612:<password>@cluster0.aw4xic6.mongodb.net/?retryWrites=true&w=majority") as client:
        msg_collection = client[DB][MSG_COLLECTION]
        distinct_channel_list = msg_collection.distinct("channel")
        return distinct_channel_list

@app.get("/messages/{channel}", response_model=List[Message])
def getMessages(channel: str):
    """Get all messages for the specified channel."""
    with MongoClient("mongodb+srv://arjun1612:<password>@cluster0.aw4xic6.mongodb.net/?retryWrites=true&w=majority") as client:
        msg_collection = client[DB][MSG_COLLECTION]
        msg_list = msg_collection.find({"channel": channel})
        response_msg_list = []
        for msg in msg_list:
            response_msg_list.append(Message(**msg))
        return response_msg_list

@app.post("/post_message", status_code=status.HTTP_201_CREATED)
def postMessage(message: Message):
    with MongoClient("mongodb+srv://arjun1612:<password>@cluster0.aw4xic6.mongodb.net/?retryWrites=true&w=majority") as client:
        msg_collenction = client[DB][MSG_COLLECTION]
        result = msg_collenction.insert_one(message.dict())
        ack = result.acknowledged
        return {"insertion": ack}
