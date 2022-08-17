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

The image you have just built should appear your list of docker images. To see a list of docker images:

```
docker image ls
```

You should see the python image and the nuxeoextent image listed, e.g.:

```
REPOSITORY                                      TAG       IMAGE ID       CREATED              SIZE
nuxeoextent                                     latest    9dd7bc4961ec   About a minute ago   1.01GB
python                                          3.9       81dbc1f514bf   4 days ago           916MB
```
### Run docker image in container

First, copy `local.env` to `env.local`

```
cp local.env env.local
```

Then, populate `env.local` with the relevant values. `S3_BUCKET` is the name of the bucket to which the metadata and reports will be written.

If you do not provide an `=` then docker will read the value from your local environment. (See [docker documentation on --env-file](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file)). So if you've set the AWS auth env vars locally, then your env.local file might look something like this:

```
NUXEO_TOKEN=xxxxxxx-xxxx-xxxx-xxxx-xxxxxxx
NUXEO_API_BASE=https://nuxeo.cdlib.org/nuxeo
NUXEO_API_PATH=site/api/v1
ES_ENDPOINT=https://example-url-elasticsearch-west-2.es.amazonaws.com
ES_INDEX=nuxeo
S3_BUCKET=nuxeo-extent-stats

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

### Fetch metadata

Right now you need to fetch the metadata from Nuxeo to S3 as a separate step *before* running the report creation.

To fetch metadata to S3 for all campuses:

```
docker exec nuxeoextent python fetch_metadata.py --all
```

### Create reports

Make sure you have fetched the metadata as per above *before* creating the reports.

To create reports for all campuses while the Nuxeo API is broken:

```
docker exec nuxeoextent-db python create_reports.py --all --es_api_broken
```

To create a report for a particular subfolder (4 levels of nesting max, e.g. /asset-library/UCX/sub1/sub2/sub3/sub4)

```
docker exec nuxeoextent-db python create_reports.py --path /asset-library/UCX/sub1/sub2 --es_api_broken
```

### TO DO

* set this up to run on AWS Fargate (once the Nuxeo API is fixed?)
