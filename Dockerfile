FROM python:3.9-slim

ADD main.py .
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", " banjka_overlord_bot.py"]
