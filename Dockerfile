# your base image
FROM python:3.11-slim

# install dependencies for building mysqlclient
RUN apt-get update && apt-get install -y gcc default-libmysqlclient-dev pkg-config

# set working directory
WORKDIR /app

# copy your files
COPY . .

# install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# start your app
CMD ["python", "api.py"]
