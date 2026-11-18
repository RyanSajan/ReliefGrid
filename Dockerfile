FROM python:3.12-slim
WORKDIR /code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY frontend ./frontend
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn","app.request_service:app","--host","0.0.0.0","--port","8000"]
