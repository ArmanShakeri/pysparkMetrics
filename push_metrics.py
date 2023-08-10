import requests

def push_metrics(data: str, host: str, job_name: str, topic: str) -> None:
    requests.post(
        "http://{h}/metrics/job/{j}/topic/{t}".format(
            h=host, j=job_name, t=topic
        ),
        data=data
    )