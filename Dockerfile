FROM python:3.8-slim-buster

WORKDIR /app

COPY downloads ./downloads

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY main.py .

CMD [ "python3", "main.py" ]