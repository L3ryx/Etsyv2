FROM python:3.11-slim

WORKDIR /app

COPY render_requirements.txt .
RUN pip install --no-cache-dir -r render_requirements.txt

COPY app.py .
COPY scraper.py .
COPY templates/ templates/

EXPOSE 10000

CMD python app.py
