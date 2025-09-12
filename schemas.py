from pydantic import BaseModel
from typing import Dict, List
from decimal import Decimal
from datetime import datetime
from typing import Optional


class UserCreate(BaseModel):
    profile: str
    client_name: str
    user_name: str
    user_passwd: str
    full_name: str
    email_address: str

class UserLogin(BaseModel):
    user_name: str
    user_passwd: str

class UserOut(BaseModel):
    user_id: int
    profile: str
    client_name: str
    user_name: str

class UserList(BaseModel):
    user_id: int
    profile: str
    client_name: str
    user_name: str
    full_name: str
    email_address: str

class Partner(BaseModel):
    partner_name: str

class ExecRequest(BaseModel):
    client_name: str
    partner_name: str
    collector_type: str
    outbound_token: str

class ProcessRead(BaseModel):
    pid: int
    client_name: str
    partner_name: str
    script_name: str
    collector_type: str
    status: str

class Token(BaseModel):
    client_name: str
    password: str
    access_token: str
    token_type: str

class ClientLogin(BaseModel):
    client_name: str | None = None
    partner_name: str | None = None
    hashed_password: str | None = None

