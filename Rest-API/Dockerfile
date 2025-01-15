FROM python:3.8.18-slim

WORKDIR /src

COPY . /src

RUN pip install -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]