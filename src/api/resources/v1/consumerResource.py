from flask_restful import Resource
from src.kafka import get_consumer
from flask import request
from confluent_kafka import KafkaException
from src.api.schemas import ConsumeMessageRequestSchema
from marshmallow import ValidationError

class ConsumerResource(Resource):
    consume_request_schema = ConsumeMessageRequestSchema()
    def get(self, topic):
        consumer = get_consumer()
        topic_exists = consumer.check_if_topic_exists(topic)
        if topic_exists is None:
            return {
                    "error": "An Internal Error Has occured, This has already been logged and the reported to the technical team"
            }, 500
        elif topic_exists == False:
            return {
                    "message": f"Topic '{topic}' does not exist"
            }, 404
        else:
            subscribed = consumer.subscribe_to_topic(topic)
            if subscribed == False:
                return {
                    "error": "An Internal Error Has occured, This has already been logged and the reported to the technical team"
                }, 500
            message = consumer.consume()
            if message is not None:
                return message, 200
            else:
                return {
                    "error": "No Messages or Messages cannot be consumed"
                }, 500
    def post(self):
        data = request.get_json()
        if data is None:
            return {
                "error": "This request must include a payload"
            }, 400
        try: 
            consume_request = self.consume_request_schema.load(data)
        except ValidationError as err:
            return {
                "error": err.messages
            }, 400
        consumer = get_consumer()
        topic = consume_request.topic
        topic_exists = consumer.check_if_topic_exists(topic)
        if topic_exists is None:
            return {
                    "error": "An Internal Error Has occured, This has already been logged and the reported to the technical team"
            }, 500
        elif topic_exists == False:
            return {
                    "message": f"Topic '{topic}' does not exist"
            }, 404
        else:
            subscribed = consumer.subscribe_to_topic(topic)
            if subscribed == False:
                return {
                    "error": "An Internal Error Has occured, This has already been logged and the reported to the technical team"
                }, 500
            message = consumer.consume()
            if message is not None:
                return message, 200
            else:
                return {
                    "error": "No Messages or Messages cannot be consumed"
                }, 500
        






