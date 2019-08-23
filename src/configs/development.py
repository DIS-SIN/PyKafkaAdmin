SECRET_KEY="development"
SEND_FILE_MAX_AGE_DEFAULT = 0
JSON_SORT_KEYS = False
LOGGING_CONFIG = {
      "version": 1,
      "formatters":{
          "default": {
              "class": "logging.Formatter",
              "format": "LEVEL: %(levelname)s TIME: %(asctime)s FILENAMEL %(filename)s MODULE: %(module)s MESSAGES: %(message)s \n"
          }
       },
       "handlers" : {
           "console": {
               "class": "logging.StreamHandler",
               "level": "NOTSET",
               "formatter": "default"
           },
           "file": {
               "class": "logging.FileHandler",
               "filename": "./src/logs/development.log",
               "level": "DEBUG",
               "formatter": "default"
           }
        },
        "loggers": {
            "": {
                "handlers": [
                   "console", "file"
                ],
                "level": "NOTSET"
            }
        }
    } 