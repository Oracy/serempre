# Use the Python base image
FROM python:3.9-slim-bullseye

# Set the working directory inside the container
WORKDIR /app

COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the code files to the container
COPY . .


# Define the default command to run the script
CMD ["python", "microservice.py"]

# docker run -e AWS_ACCESS_KEY_ID=<your-access-key-id> \
#            -e AWS_SECRET_ACCESS_KEY=<your-secret-access-key> \
#            -e AWS_DEFAULT_REGION=<your-region> \
#            oracymartos/serempre
# to test local, need to fillup those data.

# docker tag oracymartos/serempre:latest 794038211747.dkr.ecr.us-east-1.amazonaws.com/serempre/sqs-processor:latest
# docker push 794038211747.dkr.ecr.us-east-1.amazonaws.com/serempre/sqs-processor:latest
