FROM python:3.11.5

WORKDIR /app

COPY ETL.py /app/
COPY requirements.txt /app/
COPY .env /app/

RUN pip install -r requirements.txt

CMD ["python", "ETL.py"]