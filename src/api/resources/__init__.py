from .v1 import register_v1_routes

def register_api_routes(app):
    register_v1_routes(app, True)