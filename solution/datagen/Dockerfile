FROM python:3.12-slim

COPY ./requirements.txt ./requirements.txt

RUN pip install -r ./requirements.txt

WORKDIR /code

COPY ./code .

CMD ["python", "datagen.py"]
