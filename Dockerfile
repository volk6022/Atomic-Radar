FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Зависимости отдельным слоем: правка кода не должна тянуть переустановку пакетов.
COPY pyproject.toml ./
RUN pip install --no-cache-dir \
      "fastapi>=0.115" "uvicorn[standard]>=0.32" "sqlalchemy[asyncio]>=2.0" \
      "asyncpg>=0.30" "alembic>=1.14" "pydantic>=2.9" "pydantic-settings>=2.6" \
      "argon2-cffi>=23.1" "pyotp>=2.9" "itsdangerous>=2.2" "httpx>=0.27"

COPY app ./app
COPY scripts ./scripts

# Не root: сервис ходит в сеть и принимает запросы снаружи.
RUN useradd --system --uid 10001 radar && chown -R radar:radar /app
USER radar

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
