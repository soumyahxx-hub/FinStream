"""Environment-backed API configuration."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "development"
    database_url: str | None = None
    api_host: str = "0.0.0.0"
    api_port: int = 8000


settings = Settings()
