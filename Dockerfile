FROM public.ecr.aws/docker/library/python:3.9.19

WORKDIR /nuxeo-extent-stats

COPY --chmod=744 extentstats.py .
COPY requirements.txt .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "extentstats.py"]