from marshmallow import Schema, fields, post_load
from confluent_kafka import avro
import json

class ProduceMessageRequest(Schema):
    def __init__(self, topic, avro_schema, message):
        self.topic = topic
        self.avro_schema = avro_schema
        self.message = message
    
    def __str__(self):
        return f"message: {self.message} avro_schema: {self.avro_schema} topic: {self.topic}"

class ProduceMessageRequestSchema(Schema):
    topic = fields.String(required=True)
    avro_schema = fields.Dict(required=True)
    message = fields.Dict(required=True)

    @post_load
    def return_produce_message_request(self, data, **kwargs):
        data["avro_schema"] = avro.loads(json.dumps(data["avro_schema"]))
        data["message"] = data["message"]
        return ProduceMessageRequest(**data)
