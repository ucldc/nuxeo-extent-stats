FROM python:3.10

WORKDIR /nuxeo-extent-stats

COPY --chmod=744 extentstats.py .
COPY requirements.txt .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "extentstats.py"]