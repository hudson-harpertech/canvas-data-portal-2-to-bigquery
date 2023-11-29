FROM python:3.8-slim-buster

WORKDIR /app

COPY downloads ./downloads

COPY requirements.txt ./
RUN pip install -r requirements.txt

RUN python -m pip install instructure-dap-client --upgrade --upgrade-strategy=eager

COPY main.py .

CMD [ "python3", "main.py" ]