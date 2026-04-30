from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    app_name: str = "Smart Queue Processor"
    redis_url: str = "redis://localhost:6379/0"
    redis_stream_group: str = "smart_processor_group"
    redis_consumer_name: str = "processor_1"
    max_retries: int = 3
    base_backoff_seconds: int = 2
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()
