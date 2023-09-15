FROM python:3.9

WORKDIR /nuxeo-extent-stats

COPY . .

RUN pip3 install -r requirements.txt

#ENTRYPOINT ["/usr/local/bin/python", "app/create_reports.py"]

# default argument to ENTRYPOINT
#CMD ["--all"]
