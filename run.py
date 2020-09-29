# Run a test server.
from werkzeug.serving import WSGIRequestHandler
from materializationengine import create_app, create_celery
from materializationengine.celery_worker import celery_app
import os

HOME = os.path.expanduser("~")

application = create_app()
celery = create_celery(application, celery_app)

if __name__ == "__main__":

    WSGIRequestHandler.protocol_version = "HTTP/1.1"

    application.run(host='0.0.0.0',
                    port=8000,
                    debug=True,
                    threaded=True,
                    ssl_context='adhoc')
