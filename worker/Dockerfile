FROM python:3.7-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# working directory
WORKDIR /wrk

# install dependencies
RUN pip install --upgrade pip
COPY requirements.txt /wrk
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py /wrk

CMD ["python", "/wrk/worker.py", "1"]
