FROM python:3.7

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/

COPY api/database.py ./api/

CMD ["python", "-m", "scripts.migrate_csv"]