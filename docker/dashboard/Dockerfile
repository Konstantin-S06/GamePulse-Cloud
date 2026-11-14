FROM python:3.11-slim

WORKDIR /opt/gamepulse

COPY requirements/dashboard.txt /tmp/requirements/dashboard.txt
RUN pip install --no-cache-dir -r /tmp/requirements/dashboard.txt

COPY dashboard ./dashboard
COPY src ./src
COPY sql ./sql

ENV PYTHONPATH=/opt/gamepulse/src:/opt/gamepulse

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]