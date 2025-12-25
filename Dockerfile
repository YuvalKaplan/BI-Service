# Start with a modern, official Python image on a stable Debian base
FROM python:3.13.7-slim

# Set an environment variable to signal the production environment
ENV ENV_TYPE=production

# Set up the working directory
WORKDIR /app

# Copy and install Python requirements
# This will now use the correct Python 3.11 and pip from the base image
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# This single command installs all necessary browser binaries and dependencies.
RUN playwright install --with-deps --only-shell

# Copy the rest of your application code
COPY . .

# Command to run your application
# CMD ["python", "service_web.py"]