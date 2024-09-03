# nuxeo-extent-stats

Create extent stats reports for UCLDC content in Nuxeo

## Set up dev environment

### Build docker image

Clone this repo

```
git clone git@github.com:ucldc/nuxeo-extent-stats.git
cd nuxeo-extent-stats
```

Build the docker image

```
docker build -t nuxeoextent .
```

### Run docker image in container

First, copy `local.env` to `env.local`

```
cp local.env env.local
```

Then, populate `env.local` with the relevant values. `S3_BUCKET` is the name of the bucket to which the metadata and reports will be written.

If you do not provide an `=` then docker will read the value from your local environment. (See [docker documentation on --env-file](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file)). So if you've set the AWS auth env vars locally, then your env.local file might look something like this:

```
CAMPUSES=["UCB","UCD","UCI","UCLA","UCM","UCOP","UCR","UCSC","UCSD","UCSF"]

# set to True if you want to write files to local disk only
LOCAL=False

# set to True as a very hacky workaround for when Nuxeo API was broken for ElasticSearch endpoint
NUXEO_API_ES_ENDPOINT_BROKEN=False

NUXEO_TOKEN=xxxxxxx-xxxx-xxxx-xxxx-xxxxxxx
NUXEO_API=https://nuxeo.cdlib.org/nuxeo/site/api/v1
S3_BUCKET=nuxeo-extent-stats-2023

AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
```

Now, run the docker image in a container. Replace `/path/to/nuxeo-extent-stats` with your path to the `current_extent_stats` directory:

```
docker run --rm -it -d --name nuxeoextent -v /your/path/to/nuxeo-extent-stats:/app --env-file env.local nuxeoextent
```

The container will be removed on exit. Host directory `/your/path/to/nuxeo-extent-stats` will be mounted into the container. 

### Make changes to the code

Since your current working directory is mounted into the container, you can hack on the code on the host machine. Then when you're ready to run the code:


### Create reports

To create reports for all campuses:

```
docker exec nuxeoextent python app/create_reports.py --all
```

To create a report for a single campus, i.e. UCSD:

```
docker exec nuxeoextent python app/create_reports.py --campus UCSD
```

### TO DO

* set this up to run in Airflow or Fargate or somewhere else in the cloud
