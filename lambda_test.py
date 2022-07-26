from nuxeoextent import lambda_function
import json

ucb_test = {
  "campus": "UCB",
  "path": "/asset-library/UCB/UCB Bancroft Library/Codex Fernandez Leal",
  "uid": "0067abd5-accb-4768-b0a9-e1de17c7c0da"
    }

lambda_function.lambda_handler(json.dumps(ucb_test), {})