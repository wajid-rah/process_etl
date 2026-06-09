# docker build -t postgres-python-app .


# Pulls Base image
FROM python:3.10-slim

# Create /app folder and Set working directory inside container
WORKDIR /app

# Copy requirements file from laptop into container
COPY requirements.txt .

# Install dependencies automatically
RUN pip install -r requirements.txt

# Copy Python script from laptop into container
COPY read_employees.py .

# Run the script when container starts
CMD ["python", "read_employees.py"]