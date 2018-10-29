from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from materializationengine.models import AnalysisVersion


def setup_admin(app, db):
    admin = Admin(app, name="materializationengine")
    admin.add_view(ModelView(AnalysisVersion, db.session))
    return admin
