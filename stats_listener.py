from pyspark.sql.streaming import StreamingQueryListener
import json,os
from metrics import extract_metrics
from push_metrics import push_metrics
import logging
from dotenv import load_dotenv

load_dotenv()
push_gateway = os.environ["Push_gateway_host"]
appname=os.environ["Spark_appname"]
kafka_topic=os.environ["Kafka_topic"]

class StatsListener(StreamingQueryListener):
    def onQueryStarted(self, queryStarted):
        # Implementation for query start (optional)
        pass

    def onQueryProgress(self, event):
        progress_json = event.progress.prettyJson
        progress_dict = json.loads(progress_json)
        try:
            data = extract_metrics(progress_dict)
            push_metrics(data, push_gateway, appname, kafka_topic)
        except Exception as e:
            logging.error(e.__str__())

    def onQueryTerminated(self, queryTerminated):
        # Implementation for query termination (optional)
        pass