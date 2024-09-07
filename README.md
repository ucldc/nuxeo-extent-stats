# nuxeo-extent-stats

This repo contains code for creating Nuxeo extent stats reports.

**Basic layout**:

The script for generating the extent stats is `extentstats.py`.

There is a `Dockerfile` for creating an image in which to run `exentstats.py`.

The `sceptre` directory contains [sceptre](https://docs.sceptre-project.org) a CloudFormation template for creating a CodeBuild project and an ECS task in AWS.

The `run-extent-stats.py` runs the `extentstats.py` script in a container on Fargate.

**TODOs**: there are still several things that are on the roadmap to get automated around building and deploying.

## Run extent stats in Fargate

To run the extent stats generation in fargate (this is a manual process for now; planning on setting up in Airflow at some point):

Make sure your AWS credentials for the `pad-dsc-admin` AWS account are set in your environment.

Make sure that `NUXEO_TOKEN` environment is set in your environment. (You'll need to obtain a Nuxeo token for talking to the API if you don't already have one).

To run the reports for one campus:

```
python run-extent-stats-task.py --campus UCSD
```

To run the reports for all campuses (this will take a very long time! UCM and UCI take over 12 hours):

```
python run-extent-stats-task.py --all
```

The script will output the ARN of the ECS task that was launched, e.g.:

```
ECS task arn:aws:ecs:us-west-2:563907706919:task/nuxeo/f02c9bd725fe4ac99acd77bb12b8dc3e was started.
```

You can check on the status of the task in ECS. (Note: I'm not sure why the output from python doesn't immediately get written to CloudWatch logs).

Metadata and reports are written to the `nuxeo-extent-stats` S3 bucket in the `pad-dsc-admin` AWS account.

## Update Docker image

Make any updates and push to github (main branch). Then trigger a new build of the `nuxeo-extent-stats` CodeBuild project. This will build a new image and push it to ECR.

TODO: implement webhook to trigger build on push to main.


## Local development

You can use the `compose-dev.yaml` file to test out the docker files locally. Make sure your AWS env vars are set.

Login to ECR public so that you can pull the python image:

```
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
```

Then build and run the image:

```
docker compose -f compose-dev.yaml up
```

You can of course run `extentstats.py` locally (not in Docker). See `env.local.example` what env vars need to be set.

