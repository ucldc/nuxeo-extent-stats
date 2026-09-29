FROM public.ecr.aws/docker/library/python:3.13

WORKDIR /nuxeo-extent-stats/av_runtime_stats

COPY --chmod=744 av_runtime_stats.py .
COPY requirements.txt .

RUN pip3 install -r requirements.txt

RUN apt update && apt install -y \
    mediainfo

ENTRYPOINT ["python", "av_runtime_stats.py"]