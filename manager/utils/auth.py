from flask import session, redirect, url_for
from functools import wraps

def manager_required(f):
    """Decorator to ensure user is a manager."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session['user']['role'] != 'manager':
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function 