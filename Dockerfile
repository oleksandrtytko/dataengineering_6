FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y vim software-properties-common python3.9 python3-pip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

WORKDIR app
COPY . /app

RUN pip3 install -r requirements.txt
