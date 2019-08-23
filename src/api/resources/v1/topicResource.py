from flask_restful import Resource
from src.kafka import get_consumer

class TopicResource(Resource):
    def get(self, topic = None):
        consumer = get_consumer()
        if topic is not None:
            topic_exists = consumer.check_if_topic_exists(topic)
            if topic_exists is None:
                return {
                    "error": "An Internal Error Has occured, This has already been logged and the reported to the technical team"
                }, 500
            elif topic_exists == True:
                return "", 201
            else:
                return {
                    "message": f"Topic '{topic}' does not exist"
                }, 404
        else:
            topic_list = consumer.list_topics()
            if topic_list is None:
                return {
                    "error": "An Internal Error Has occured, This has already been logged and reported to the technical team"
                }, 500
            else:
                return topic_list, 200

       