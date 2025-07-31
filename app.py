from flask import Flask
import os
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Import routes
from routes.auth import auth_bp
from routes.upload import upload_bp
from routes.admin import admin_bp
from routes.qc import qc_bp
from routes.reports import reports_bp
from routes.download import download_bp
from routes.manager.manager_dashboard import manager_bp
from routes.image_viewer import image_viewer_bp

# Import utilities
from utils.helpers import update_allocation_table

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

# App configuration
app.secret_key = "9e4c1a1fc4764e77b91f4e9ba3f142b03b8f63773cfebf44cb2d1d9d24a1e0e1"

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(qc_bp)
app.register_blueprint(reports_bp)
app.register_blueprint(download_bp)
app.register_blueprint(manager_bp)
app.register_blueprint(image_viewer_bp)

# Background scheduler setup
scheduler = None
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_allocation_table, trigger="interval", minutes=15)
    scheduler.start()

def shutdown_scheduler():
    """Shutdown scheduler on app exit."""
    if scheduler is not None and scheduler.running:
        scheduler.shutdown()

if scheduler is not None:
    atexit.register(shutdown_scheduler)

if __name__ == "__main__":
    app.run(debug=True)
    
    
    
    
    
    
    

