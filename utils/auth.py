from flask import session, redirect, url_for, jsonify, make_response
from functools import wraps

def no_cache(f):
    """Decorator to prevent caching."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        resp = make_response(f(*args, **kwargs))
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    return decorated_function

def login_required(f):
    """Decorator to require login for a route."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(required_role):
    """Decorator to require specific role for a route."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                return redirect(url_for('auth.login'))
            if session['user']['role'] != required_role:
                return jsonify({"error": "Unauthorized access"}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def api_login_required(f):
    """Decorator for API routes that require login (returns JSON error)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return jsonify({"error": "User not logged in"}), 401
        return f(*args, **kwargs)
    return decorated_function

def api_role_required(required_role):
    """Decorator for API routes that require specific role (returns JSON error)."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                return jsonify({"error": "User not logged in"}), 401
            if session['user']['role'] != required_role:
                return jsonify({"error": "Unauthorized access"}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def manager_required(f):
    """Decorator to ensure user is a manager."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session['user']['role'] != 'manager':
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function 