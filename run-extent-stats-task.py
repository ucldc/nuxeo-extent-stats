import argparse

import boto3

def main(args):
    command = []
    if args.all:
        command = ["--all"]
        campus = "all"
    else:
        command = ["--campus", args.campus]
        campus = args.campus
    if args.version:
        command.extend(["--version", args.version])

    # assume we"re running this in the pad-dsc-admin account for now
    cluster = "nuxeo"
    subnets = ["subnet-b07689e9", "subnet-ee63cf99"] # Public subnets in the nuxeo VPC
    security_groups = ["sg-51064f34"] # default security group for nuxeo VPC
    
    ecs_client = boto3.client("ecs")
    response = ecs_client.run_task(
        cluster = cluster,
        capacityProviderStrategy=[
            {
                "capacityProvider": "FARGATE",
                "weight": 1,
                "base": 1
            },
        ],
        taskDefinition = "nuxeo-extent-stats-task-definition",
        count = 1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "ENABLED"
            }
        },
        platformVersion="LATEST",
        overrides = {
            "containerOverrides": [
                {
                    "name": "nuxeo-extent-stats",
                    "command": command
                }
            ]
        },
        tags=[
            {
                "key": "campus",
                "value": campus
            }
        ],
        enableECSManagedTags=True,
        enableExecuteCommand=True
    )
    task_arn = [task['taskArn'] for task in response['tasks']][0]
    
    print(f"ECS task {task_arn} was started.")
              
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument("--all", help="create reports for all campuses", action="store_true")
    top_folder.add_argument("--campus", help="single campus")
    parser.add_argument("--version", help="Metadata version. If not provided, metadata will be fetched from S3.")

    args = parser.parse_args()
    (main(args))