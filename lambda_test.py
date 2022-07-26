from lambda_function import lambda_handler
import json

ucb_test = {
    'campus': 'UCB',
    'path': '/asset-library/UCB'
}

lambda_handler(json.dumps(ucb_test), {})