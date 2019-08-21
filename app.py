import os 
from src import create_app

environment = os.environ.get("FLASK_ENV") or "development"

application = app = create_app(environment)

if __name__ == "__main__":
    app.run()
