from fastapi import APIRouter
from pydantic import BaseModel
from app.service import buy_unnumbered, buy_numbered

router = APIRouter()

class BuyRequest(BaseModel):
    client_id: str
    request_id: str


@router.post("/buy")
def buy(req: BuyRequest):
    success = buy_unnumbered()

    return {
        "status": "success" if success else "fail"
    }


@router.post("/buy/{seat_id}")
def buy_seat(seat_id: int, req: BuyRequest):
    success = buy_numbered(seat_id, req.client_id)

    return {
        "status": "success" if success else "fail"
    }