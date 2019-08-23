from flask import request
from flask_restful import Resource
from src.kafka import get_producer
from src.api.schemas import ProduceMessageRequestSchema
from marshmallow import ValidationError
from confluent_kafka.avro import SerializerError


class ProducerResource(Resource):
    message_request_schema = ProduceMessageRequestSchema()
    def post(self,topic = None):
        data = request.get_json()
        if data is None:
            return {
                "error": "Must provide payload for this request"
            }, 400
        producer = get_producer()
        
        if topic is not None:
            data["topic"] = topic

        try:
            message_request = self.message_request_schema.load(data)
        except ValidationError as err:
            return {
                "error": err.messages
            }, 400
        except Exception as err:
            return {
                "error": "Schema {} is invalid {}".format(
                    data.get("avro_schema"), err
                ) 
            }, 400


        error = producer.produce(
            message_request.topic,
            msg = message_request.message,
            schema = message_request.avro_schema
        )
        if isinstance(error, SerializerError):
            return {
                "error": f"Deserialization has failed for {str(message_request)} details: {error}"
            }, 400
        elif isinstance(error, Exception):
            return {
                "error": "An Internal Error Has occured, This has already been logged and reported to the technical team"
            }, 500
        
        return "", 201



                

        
        
        


