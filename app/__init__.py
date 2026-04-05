import os

from flask import Flask

from .storage import init_db


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    os.makedirs(app.instance_path, exist_ok=True)
    app.config["DATABASE_PATH"] = os.path.join(app.instance_path, "sudoku.db")

    init_db(app.config["DATABASE_PATH"])

    # Register custom Jinja2 filter for formatting elapsed seconds
    def format_elapsed_seconds(seconds):
        seconds = int(seconds or 0)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"
    
    app.jinja_env.filters['format_time'] = format_elapsed_seconds

    from .routes import bp

    app.register_blueprint(bp)
    return app
