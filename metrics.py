from datetime import datetime

def extract_metrics(progress: dict) -> dict:
    stream_name = progress["name"]
    data = ""

    progress["timestamp"] = int(
        datetime.strptime(
            progress["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ'
        ).timestamp()
        * 1000
    )

    metrics = [
        "inputRowsPerSecond",
        "processedRowsPerSecond",
        "numInputRows",
        "timestamp",
        "batchId",
    ]

    for m in metrics:
        data += "{key} {value}\n".format(key=m, value=progress[m])

    lagMetrics = progress['sources'][0]['metrics']
    for m in lagMetrics:
        data += "{key} {value}\n".format(key=m, value=lagMetrics[m])

    durationMs = progress['durationMs']
    for m in durationMs:
        data += "{key} {value}\n".format(key=m, value=durationMs[m])

    startOffsets = progress['sources'][0]['startOffset']
    for t in startOffsets:
        topic = t
        for p in progress['sources'][0]['startOffset'][t]:
            partition = p
            data += "{k}{{partition=\"{p}\"}} {v}\n".format(p=p, k="startOffset",
                                                            v=progress['sources'][0]['startOffset'][t][p])
    latestOffsets = progress['sources'][0]['latestOffset']
    for t in latestOffsets:
        topic = t
        for p in progress['sources'][0]['latestOffset'][t]:
            partition = p
            data += "{k}{{partition=\"{p}\"}} {v}\n".format(p=p, k="latestOffset",
                                                            v=progress['sources'][0]['latestOffset'][t][p])
    return data



