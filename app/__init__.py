import os

from flask import Flask

from .storage import init_db


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    os.makedirs(app.instance_path, exist_ok=True)
    app.config["DATABASE_PATH"] = os.path.join(app.instance_path, "sudoku.db")

    init_db(app.config["DATABASE_PATH"])

    from .routes import bp

    app.register_blueprint(bp)
    return app
