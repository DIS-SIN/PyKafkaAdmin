import logging
import os
from flask import Flask 

def create_app(environment = "production"):
    app = Flask(__name__)
    try:
        from .configs import default
        app.config.from_object(default)
    except ModuleNotFoundError:
        pass

    if environment == "production":
        try:
            from .configs import production
            app.config.from_pyfile(production)
        except ModuleNotFoundError:
            pass
        
        if app.config.get("LOGGING_CONFIG") is not None:
                logging.config.dictConfig(app.config["LOGGING_CONFIG"])
                app.config["LOGGING_ENABLED"] = True
        else:
            app.config["LOGGING_ENABLED"] = False
        
    else:
        try:
            from .configs import development
            app.config.from_object(development) 
        except ModuleNotFoundError:
            pass

        if app.config.get("LOGGING_CONFIG") is not None:
                logging.config.dictConfig(app.config["LOGGING_CONFIG"])
                app.config["LOGGING_ENABLED"] = True
        else:
            app.config["LOGGING_ENABLED"] = False
        
        if environment != "development":
            if app.config["LOGGING_ENABLED"]:
                logging.warn(
                    f"environment not explicitly set as development set as {environment}"
                )
            else:
                print(
                    f"WARNING: environment not explicilt set as development set as {environment}"
                )
        
    #TODO: Make sure configurations are in place 
    if app.config.get("BROKER_HOST") is None:
        broker_host = os.environ.get("PyADMINKAFKA_BROKER_HOST")
        if broker_host is None and environment == "production":
            raise ValueError(
                "BROKER_HOST was not provided, must either be set in config files or " +
                "as an environment variable PyADMINKAFKA_BROKER_HOST"
            )
        else:
            broker_host = "localhost:9092"
        app.config["BROKER_HOST"] = broker_host
    
    return app


