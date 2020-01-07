from flask.cli import FlaskGroup
from app.main import create_app
from app import celery
from app.celery_app import create_celery
from app.database import db

application = create_app()
create_celery(application, celery)
cli = FlaskGroup(application)

@cli.command("create_db")
def create_db():
    db.drop_all()
    db.create_all()
    db.session.commit()



if __name__ == "__main__":
    cli()# application.run(host='0.0.0.0', port=80, debug=True)
