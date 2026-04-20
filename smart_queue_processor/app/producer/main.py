from fastapi import FastAPI, Depends
from ..config import get_settings, Settings

app = FastAPI()

@app.get("/")
def read_root(settings: Settings = Depends(get_settings)):
    return {
        "message": f"{settings.app_name} producer is ready.",
        "status": "online"
    }


