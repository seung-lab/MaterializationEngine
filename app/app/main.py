import os
from app.factories.flask_app import create_app
from app.factories.celery_app import create_celery
from app.extensions import celery

app = create_app()
celery = create_celery(app, celery)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=80)
