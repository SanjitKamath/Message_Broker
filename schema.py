from pydantic import BaseModel, Field, EmailStr

class DataPacket(BaseModel):
    id: int = Field(..., description="Unique ID for the packet")
    sender: str = Field(..., min_length=3)
    content: str = Field(..., max_length=50)
    priority: int = Field(default=1, ge=1, le=10)