# Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script into the container
COPY ccxt_exchanges.py dontshare_config.py main.py requirements.txt ./

# Install required Python packages
RUN pip install --no-chache-dir -r requirements.txt

# set cronjob to run daily at 3am
RUN echo "0 3 * * * /usr/local/bin/python /app/my_plot_script.py" > /etc/crontab


# Set the entry point for the container
CMD ["python", "main.py"]