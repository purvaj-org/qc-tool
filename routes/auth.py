from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, make_response
import bcrypt
import datetime
from utils.database import get_db_connection
from utils.auth import no_cache

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/')
@no_cache
def index():
    print(f"Index route accessed: {datetime.datetime.now()}")
    return redirect(url_for('auth.login'))

@auth_bp.route('/login', methods=['GET', 'POST'])
@no_cache
def login():
    print(f"Login route accessed: {datetime.datetime.now()}")
    if request.method == 'POST':
        data = request.get_json()
        if not data:
            print("Login error: Invalid request data")
            return jsonify({'success': False, 'message': 'Invalid request data'}), 400

        loginid = data.get('username')
        password = data.get('password')

        if not loginid or not password:
            print("Login error: Username and password required")
            return jsonify({'success': False, 'message': 'Username and password required'}), 400

        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                sql = "SELECT unique_userid, loginid, user_role, passwords FROM user_table WHERE loginid = %s"
                cursor.execute(sql, (loginid,))
                user = cursor.fetchone()
            conn.close()

            if user:
                stored_password = user['passwords'].encode('utf-8')
                if bcrypt.checkpw(password.encode('utf-8'), stored_password):
                    user_role = user['user_role'].strip().lower()
                    session['user'] = {
                        'unique_userid': user['unique_userid'],
                        'username': user['loginid'],
                        'role': user_role
                    }
                    print(f"Login successful for {loginid}, role: {user_role}")
                    if user_role == 'admin':
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('admin.admin')})
                    elif user_role == 'vendor':
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('upload.upload')})
                    elif user_role == 'qc':
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('qc.qc')})
                    elif user_role == 'manager':
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('manager.manager_upload_history')})
                    else:
                        print(f"Login error: Unauthorized user role for {loginid}")
                        return jsonify({'success': False, 'message': 'Unauthorized user role'}), 403
                else:
                    print(f"Login error: Invalid password for {loginid}")
                    return jsonify({'success': False, 'message': 'Invalid username or password'}), 401
            else:
                print(f"Login error: User {loginid} not found")
                return jsonify({'success': False, 'message': 'Invalid username or password'}), 401
        except Exception as e:
            print(f"Login error: {str(e)}")
            return jsonify({'success': False, 'message': 'Server error'}), 500

    return render_template('login.html')

@auth_bp.route('/logout')
@no_cache
def logout():
    print(f"Logout route accessed: {datetime.datetime.now()}")
    session.clear()
    resp = make_response(redirect(url_for('auth.login')))
    resp.set_cookie('session', '', expires=0)
    return resp

@auth_bp.route('/check_session', methods=['GET'])
@no_cache
def check_session():
    print(f"Check session accessed: {datetime.datetime.now()}")
    try:
        if 'user' in session:
            return jsonify({"valid": True})
        return jsonify({"valid": False}), 401
    except Exception as e:
        print(f"Check session error: {str(e)}")
        return jsonify({"valid": False}), 401 