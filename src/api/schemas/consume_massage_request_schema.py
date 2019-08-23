from marshmallow import Schema, fields, post_load

class ConsumeMessageRequest():
    def __init__(self, topic):
        self.topic = topic

class ConsumeMessageRequestSchema(Schema):
    topic = fields.String(required=True)

    @post_load
    def return_consume_message_request(self, data, **kwargs):
        return ConsumeMessageRequest(**data)