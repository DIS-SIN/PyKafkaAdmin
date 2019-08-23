from .topicResource import TopicResource
from .producerResource import ProducerResource
from .consumerResource import ConsumerResource
from flask.blueprints import Blueprint
from flask_restful import Api


def register_v1_routes(app, current_version=False):
    if current_version:
        api_bp = Blueprint(name="current_api", import_name = __name__)
        api = Api(api_bp)
    else:
        api_bp = Blueprint(name="v1", import_name = __name__)
        api = Api(api_bp, prefix="/v1")

    api.add_resource(
        TopicResource, "/topics",
        "/topics/<string:topic>", endpoint = "topics"
    )

    api.add_resource(
       ProducerResource, "/produce", "/topics/<string:topic>/produce", endpoint = "producer"   
    )

    api.add_resource(
        ConsumerResource, "/consume", "/topics/<string:topic>/consume", endpoint = "consume"
    )
    app.register_blueprint(api_bp)

