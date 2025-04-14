from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    REDIS_URL: str = "redis://default:JPoDlzTpRCxckTFHbwZOfG3zgKfJznHV@redis-19858.crce182.ap-south-1-1.ec2.redns.redis-cloud.com:19858"
    apikey: str
    username: str
    pwd: str
    token: str
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
