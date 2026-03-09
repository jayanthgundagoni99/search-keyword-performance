FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY code/search_keyword_performance/ ./search_keyword_performance/
COPY code/search_keyword_handler.py ./
COPY code/batch_handler.py ./

RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "-m", "search_keyword_performance"]
