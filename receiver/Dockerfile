FROM python:3.7-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# working directory
WORKDIR /srv

# install dependencies
RUN pip install --upgrade pip
COPY requirements.txt /srv
RUN pip install --no-cache-dir -r requirements.txt

COPY receiver.py /srv
