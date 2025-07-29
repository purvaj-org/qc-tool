from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response, send_file, make_response
import pymysql
import os
from functools import wraps
from s3_upload import upload_to_s3, s3_client, get_image_list_from_s3, SPACES_NAME
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import bcrypt
import pandas as pd
from io import BytesIO
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from manager import manager_bp

from datetime import datetime as dt  # Add this import at the top of your file
from logging.handlers import RotatingFileHandler
from botocore.exceptions import ClientError

import logging
from flask import jsonify, request, session

from flask import Flask, Blueprint, request, jsonify
import pymysql
import os
from urllib.parse import unquote


app = Flask(__name__)

app.register_blueprint(manager_bp)

app.secret_key = "9e4c1a1fc4764e77b91f4e9ba3f142b03b8f63773cfebf44cb2d1d9d24a1e0e1"
load_dotenv()
# Store upload progress (Use Redis in production)
upload_progress = {}

# Email configuration (replace with your SMTP details or use environment variables)
SMTP_SERVER = "smtp.sendgrid.net"
SMTP_PORT = 587
SMTP_USERNAME = os.getenv("SMTP_USERNAME")  # Use your SendGrid username
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD") # Use App Password for Gmail

# Decorator to prevent caching
def no_cache(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        resp = make_response(f(*args, **kwargs))
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    return decorated_function

# Database connection function
def get_db_connection():
    return pymysql.connect(
        host=os.getenv("db_host", "localhost"),
        port=int(os.getenv("db_port", 3306)),
        user=os.getenv("db_user"),
        password=os.getenv("db_password"),
        database=os.getenv("db_database"), 
        cursorclass=pymysql.cursors.DictCursor
    )

# Function to get admin email
def get_admin_email():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT email FROM user_table WHERE user_role = 'admin' LIMIT 1")
            admin = cursor.fetchone()
            return admin['email'] if admin else None
    finally:
        conn.close()

# Function to send email
def send_email(subject, body, to_email):
    if not to_email:
        print("Email error: No admin email found")
        return False
    try:
        msg = MIMEMultipart()
        msg['From'] = "noreply@purvaj.com"
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        print(f"Email sent to {to_email}: {subject}")
        return True
    except Exception as e:
        print(f"Email error: Failed to send email - {str(e)}")
        return False

def update_allocation_table():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT batch_id, upload_date, image_count, status 
                FROM allocation_table
            """)
            allocations = cursor.fetchall()

            for allocation in allocations:
                batch_id = allocation['batch_id']
                upload_date = allocation['upload_date']
                image_count = allocation['image_count']
                current_status = allocation['status']

                cursor.execute("""
                    SELECT image_id 
                    FROM image_table 
                    WHERE batch_id = %s AND date_uploaded = %s
                """, (batch_id, upload_date))
                image_ids = [row['image_id'] for row in cursor.fetchall()]

                if image_ids:
                    format_strings = ','.join(['%s'] * len(image_ids))
                    cursor.execute(f"""
                        SELECT status, COUNT(*) as count 
                        FROM qc_table 
                        WHERE batch_id = %s AND image_id IN ({format_strings})
                        GROUP BY status
                    """, [batch_id] + image_ids)
                    qc_counts = cursor.fetchall()

                    approved_count = 0
                    rejected_count = 0
                    for row in qc_counts:
                        if row['status'] == 'accepted':
                            approved_count = row['count']
                        elif row['status'] == 'rejected':
                            rejected_count = row['count']
                else:
                    approved_count = 0
                    rejected_count = 0

                total_processed = approved_count + rejected_count

                if current_status == 'revoked':
                    new_status = 'revoked'
                elif total_processed == 0:
                    new_status = 'Pending'
                elif total_processed < image_count:
                    new_status = 'In Progress'
                elif total_processed == image_count:
                    new_status = 'Completed'
                else:
                    new_status = 'Error'

                cursor.execute("""
                    UPDATE allocation_table 
                    SET approved_count = %s, rejected_count = %s, status = %s 
                    WHERE batch_id = %s AND upload_date = %s
                """, (approved_count, rejected_count, new_status, batch_id, upload_date))

            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating allocation_table: {e}")
    finally:
        conn.close()

scheduler = None
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_allocation_table, trigger="interval", minutes=15)
    scheduler.start()

@app.route('/')
@no_cache
def index():
    print(f"Index route accessed: {datetime.datetime.now()}")
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
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
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('admin')})
                    elif user_role == 'vendor':
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('upload')})
                    elif user_role == 'qc':
                        return jsonify({'success': True, 'session_id': user['unique_userid'], 'redirect': url_for('qc')})
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
@app.route('/upload', methods=['GET', 'POST'])
@no_cache
def upload():
    if 'user' not in session:
        print(f"Upload route accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('upload.html', session_id=session['user']['unique_userid'])

@app.route('/admin')
@no_cache
def admin():
    if 'user' not in session:
        print(f"Admin route accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('admin.html', session_id=session['user']['unique_userid'])

@app.route('/qc')
@no_cache
def qc():
    if 'user' not in session:
        print(f"QC route accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('qc.html', session_id=session['user']['unique_userid'])

@app.route('/logout')
@no_cache
def logout():
    print(f"Logout route accessed: {datetime.datetime.now()}")
    session.clear()
    resp = make_response(redirect(url_for('login')))
    resp.set_cookie('session', '', expires=0)
    return resp

@app.route("/get_vendor_data")
@no_cache
def get_vendor_data():
    if 'user' not in session:
        print(f"Get vendor data accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 401
    unique_userid = request.args.get("unique_userid")
    if not unique_userid:
        return jsonify({"error": "Missing unique_userid"}), 400
    
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT location FROM vendor_allocation_table WHERE unique_userid = %s", (unique_userid,))
        locations = [row["location"] for row in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT panda_name FROM vendor_allocation_table WHERE unique_userid = %s", (unique_userid,))
        pandas = [row["panda_name"] for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({"locations": locations, "pandas": pandas})

@app.route("/upload_images", methods=["POST"])
@no_cache
def upload_images():
    if "user" not in session:
        print(f"Upload images accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "User not logged in"}), 401

    session_id = session['user']['unique_userid']
    location = request.form.get("location")
    panda_name = request.form.get("panda-name")
    bahi_name = request.form.get("bahi-name")
    upload_type = request.form.get("upload-type")
    record_type = request.form.get("record-type")
    files = request.files.getlist("files")

    if not (location and panda_name and bahi_name and upload_type and files):
        return jsonify({"error": "Missing required fields"}), 400

    # Adjust batch_id based on upload_type
    if upload_type == "reupload":
        batch_id = f"{session_id}_{location}_{panda_name}_{bahi_name}_{record_type}_{upload_type}"
    else:
        batch_id = f"{session_id}_{location}_{panda_name}_{bahi_name}_{record_type}"

    uploaded_files = []
    failed_files = []
    total_files = len(files)

    upload_progress[session_id] = 0

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT NOW()")
            upload_timestamp = cursor.fetchone()["NOW()"]

            for index, file in enumerate(files, 1):
                filename = os.path.basename(file.filename)

                if filename.lower().endswith((".jpg", ".jpeg", ".png")):
                    # Adjust s3_path based on upload_type
                    if upload_type == "reupload":
                        s3_path = f"{session_id}/{location}/{panda_name}/{bahi_name}/{record_type}/{upload_type}/{filename}"
                    else:
                        s3_path = f"{session_id}/{location}/{panda_name}/{bahi_name}/{record_type}/{filename}"
                    
                    upload_result = upload_to_s3(file, s3_path)

                    if upload_result["success"]:
                        uploaded_files.append(filename)
                        cursor.execute("""
                            INSERT INTO image_table (batch_id, image_id, date_uploaded) 
                            VALUES (%s, %s, %s)
                        """, (batch_id, filename, upload_timestamp))
                    else:
                        failed_files.append({"file": filename, "error": upload_result["message"]})
                else:
                    failed_files.append({"file": filename, "error": "Invalid file format. Only .jpg, .jpeg, and .png files are allowed."})

                upload_progress[session_id] = int((index / total_files) * 100)

            if uploaded_files:
                cursor.execute("""
                    INSERT INTO pandas_upload_table 
                    (batch_id, unique_userid, upload_type, location, pandas_name, bahi_name, record_type, upload_date, image_count) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (batch_id, session_id, upload_type, location, panda_name, bahi_name, record_type, upload_timestamp, len(uploaded_files)))

                # Get vendor name for notification
                cursor.execute("SELECT name FROM user_table WHERE unique_userid = %s", (session_id,))
                vendor = cursor.fetchone()
                vendor_name = vendor['name'] if vendor else "Unknown"

        conn.commit()

        # Send email notification to admin for batch upload
        admin_email = get_admin_email()
        if uploaded_files:
            subject = f"New Batch Uploaded: {batch_id}"
            body = (
                f"A new batch has been uploaded and is ready for allocation.\n\n"
                f"Batch ID: {batch_id}\n"
                f"Image Count: {len(uploaded_files)}\n"
                f"Vendor ID: {session_id}\n"
                f"Vendor Name: {vendor_name}\n"
                f"Upload Date: {upload_timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            send_email(subject, body, admin_email)

    except Exception as e:
        conn.rollback()
        print(f"Upload images error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    upload_progress[session_id] = 100
    return jsonify({"message": "Upload completed", "uploaded_files": uploaded_files, "failed_files": failed_files})

@app.route("/upload_progress", methods=["GET"])
@no_cache
def upload_progress_status():
    session_id = session.get('user', {}).get('unique_userid')
    if not session_id or session_id not in upload_progress:
        return jsonify({"progress": 0})
    progress = upload_progress.get(session_id, 0)
    return jsonify({"progress": progress})

@app.route("/reset_progress", methods=["POST"])
@no_cache
def reset_progress():
    session_id = session.get('user', {}).get('unique_userid')
    if session_id in upload_progress:
        del upload_progress[session_id]
    return jsonify({"message": "Progress reset"})

@app.route('/upload_history')
@no_cache
def upload_history():
    if 'user' not in session:
        print(f"Upload history accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('upload_history.html', session_id=session['user']['unique_userid'])

@app.route('/get_upload_history', methods=['GET'])
@no_cache
def get_upload_history():
    if 'user' not in session:
        print(f"Get upload history accessed without session: {datetime.datetime.now()}")
        return jsonify({"history": [], "error": "User not logged in"}), 401

    unique_userid = session['user']['unique_userid']

    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            query = """
                SELECT upload_date, batch_id, location, pandas_name, 
                       bahi_name, upload_type, image_count
                FROM pandas_upload_table
                WHERE unique_userid = %s
                ORDER BY upload_date DESC
            """
            cursor.execute(query, (unique_userid,))
            uploads = cursor.fetchall()

            for upload in uploads:
                batch_id = upload['batch_id']

                # Count number of images with status = 'accepted'
                cursor.execute("""
                    SELECT COUNT(*) AS approved_count
                    FROM qc_table
                    WHERE batch_id = %s AND status = 'accepted'
                """, (batch_id,))
                approved_result = cursor.fetchone()
                upload['approved_count'] = approved_result['approved_count'] if approved_result else 0

                # Count number of images with status = 'rejected'
                cursor.execute("""
                    SELECT COUNT(*) AS rejected_count
                    FROM qc_table
                    WHERE batch_id = %s AND status = 'rejected'
                """, (batch_id,))
                rejected_result = cursor.fetchone()
                upload['rejected_count'] = rejected_result['rejected_count'] if rejected_result else 0

        conn.close()
        return jsonify({"history": uploads})

    except pymysql.MySQLError as e:
        print(f"Get upload history error: {str(e)}")
        return jsonify({"error": "Database error"}), 500

    except Exception as e:
        print(f"Get upload history error: {str(e)}")
        return jsonify({"error": "Unexpected server error"}), 500



@app.route('/ready_to_allocate')
@no_cache
def ready_to_allocate():
    if 'user' not in session:
        print(f"Ready to allocate accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('admin.html')

@app.route('/get_ready_to_allocate')
@no_cache
def get_ready_to_allocate():
    if 'user' not in session:
        print(f"Get ready to allocate accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "User not logged in"}), 401

    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM pandas_upload_table p WHERE NOT EXISTS (SELECT 1 FROM allocation_table a WHERE a.batch_id = p.batch_id AND a.upload_date = p.upload_date AND a.status != 'revoked');")
        batches = cursor.fetchall()

        vendor_mapping = {}
        cursor.execute("SELECT unique_userid, name FROM user_table")
        for row in cursor.fetchall():
            vendor_mapping[row["unique_userid"]] = row["name"]

        cursor.execute("SELECT unique_userid, name FROM user_table WHERE user_role = 'qc'")
        qc_users = cursor.fetchall()

    conn.close()

    for batch in batches:
        batch["vendor_name"] = vendor_mapping.get(batch["unique_userid"], "Unknown")
        batch["qc_users"] = qc_users

    return jsonify(batches)

@app.route('/allocate_qc', methods=['POST'])
@no_cache
def allocate_qc():
    if 'user' not in session:
        print(f"Allocate QC accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    data = request.json
    batch_id = data.get("batch_id")
    qc_user_id = data.get("qc_user")
    image_count = data.get("image_count")
    upload_date = data.get("upload_date")

    if not batch_id or not qc_user_id or not upload_date:
        return jsonify({"error": "Missing required fields"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT unique_userid FROM allocation_table 
                WHERE batch_id = %s AND upload_date = %s
            """, (batch_id, upload_date))
            row = cursor.fetchone()

            allocation_date = datetime.datetime.now()

            if row:
                existing_user = row['unique_userid']
                if existing_user != qc_user_id:
                    cursor.execute("""
                        UPDATE allocation_table
                        SET unique_userid = %s,
                            allocation_date = %s,
                            status = 'Pending'
                        WHERE batch_id = %s AND upload_date = %s
                    """, (qc_user_id, allocation_date, batch_id, upload_date))
            else:
                cursor.execute("""
                    INSERT INTO allocation_table (
                        batch_id, unique_userid, allocation_date, upload_date,
                        status, rejected_count, image_count, approved_count
                    ) VALUES (%s, %s, %s, %s, 'Pending', 0, %s, 0)
                """, (batch_id, qc_user_id, allocation_date, upload_date, image_count))

            # Get QC user name for notification
            cursor.execute("SELECT name FROM user_table WHERE unique_userid = %s", (qc_user_id,))
            qc_user = cursor.fetchone()
            qc_user_name = qc_user['name'] if qc_user else "Unknown"

        conn.commit()

        # Send email notification to admin for batch allocation
        admin_email = get_admin_email()
        subject = f"Batch Allocated: {batch_id}"
        body = (
            f"A batch has been allocated to a QC user.\n\n"
            f"Batch ID: {batch_id}\n"
            f"Image Count: {image_count}\n"
            f"Upload Date: {upload_date}\n"
            f"Allocation Date: {allocation_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"QC User ID: {qc_user_id}\n"
            f"QC User Name: {qc_user_name}\n"
        )
        send_email(subject, body, admin_email)

    except Exception as e:
        conn.rollback()
        print(f"Allocate QC error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify({"message": "Batch allocation processed successfully!"})

@app.route('/allocation_history')
@no_cache
def allocation_history():
    if 'user' not in session:
        print(f"Allocation history accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('allocation_history.html')

@app.route('/get_allocation_history', methods=['GET'])
@no_cache
def get_allocation_history():
    if 'user' not in session:
        print(f"Get allocation history accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT batch_id, unique_userid, allocation_date, status,
                   rejected_count, image_count, approved_count, upload_date
            FROM allocation_table
        """)
        history_data = cursor.fetchall()
    conn.close()

    return jsonify(history_data)

@app.route('/revoke_allocation', methods=['POST'])
@no_cache
def revoke_allocation():
    if 'user' not in session:
        print(f"Revoke allocation accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    data = request.json
    batch_id = data.get('batch_id')

    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("UPDATE allocation_table SET status='revoked' WHERE batch_id=%s", (batch_id,))
    conn.commit()
    conn.close()

    return jsonify({"message": f"Allocation revoked for batch {batch_id}."})

@app.route('/download_report/<batch_id>')
@no_cache
def download_report(batch_id):
    if 'user' not in session:
        print(f"Download report accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    return jsonify({"message": "Not implemented"})

@app.route('/qc_user')
@no_cache
def qc_user():
    if 'user' not in session:
        print(f"QC user accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('qc_user.html')

@app.route("/get_qc_users")
@no_cache
def get_qc_users():
    if 'user' not in session:
        print(f"Get QC users accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT unique_userid, name, email FROM user_table WHERE user_role = 'qc'")
        qc_users = cursor.fetchall()
    conn.close()
    
    return jsonify(qc_users)

@app.route("/add_qc_user", methods=["POST"])
@no_cache
def add_qc_user():
    if 'user' not in session:
        print(f"Add QC user accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid request data"}), 400
        
        uniqueuserid = data.get("uniqueuserid")
        name = data.get("name")
        loginid = data.get("loginid")
        password = data.get("passwords")
        email = data.get("email")

        if not (name and loginid and password and email):
            return jsonify({"error": "All fields are required"}), 400

        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_table (unique_userid, name, loginid, passwords, email, user_role)
                    VALUES (%s, %s, %s, %s, %s, 'qc')
                    """,
                    (uniqueuserid, name, loginid, hashed_password, email),
                )
            conn.commit()
        except pymysql.MySQLError as e:
            conn.rollback()
            print(f"Add QC user error: {str(e)}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        finally:
            conn.close()

        return jsonify({"message": "QC user added successfully!", "success": True})

    except Exception as e:
        print(f"Add QC user error: {str(e)}")
        return jsonify({"error": "Server error"}), 500

@app.route('/upload_user')
@no_cache
def upload_user():
    if 'user' not in session:
        print(f"Upload user accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('upload_user.html')

@app.route("/get_upload_users")
@no_cache
def get_upload_users():
    if 'user' not in session:
        print(f"Get upload users accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT unique_userid, name, email FROM user_table WHERE user_role = 'vendor'")
        upload_users = cursor.fetchall()
    conn.close()
    
    return jsonify(upload_users)

@app.route("/add_upload_user", methods=["POST"])
@no_cache
def add_upload_user():
    if 'user' not in session:
        print(f"Add upload user accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid request data"}), 400
        
        uniqueuserid = data.get("uniqueuserid")
        name = data.get("name")
        loginid = data.get("loginid")
        password = data.get("passwords")
        email = data.get("email")

        if not (name and loginid and password and email):
            return jsonify({"error": "All fields are required"}), 400

        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_table (unique_userid, name, loginid, passwords, email, user_role)
                    VALUES (%s, %s, %s, %s, %s, 'vendor')
                    """,
                    (uniqueuserid, name, loginid, hashed_password, email),
                )
            conn.commit()
        except pymysql.MySQLError as e:
            conn.rollback()
            print(f"Add upload user error: {str(e)}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        finally:
            conn.close()

        return jsonify({"message": "Upload user added successfully!", "success": True})

    except Exception as e:
        print(f"Add upload user error: {str(e)}")
        return jsonify({"error": "Server error"}), 500

@app.route("/get_qc_tasks", methods=["GET"])
@no_cache
def get_qc_tasks():
    if "user" not in session:
        print(f"Get QC tasks accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "User not logged in"}), 401

    session_id = session['user']['unique_userid']
    sort_by = request.args.get('sort_by', 'upload_date')  # Default sort by upload_date
    sort_order = request.args.get('sort_order', 'desc')  # Default descending order
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Base query
            base_query = """
                SELECT a.batch_id, a.image_count, a.upload_date, a.approved_count, a.rejected_count, a.allocation_date
                FROM allocation_table a
                WHERE a.unique_userid = %s
                AND NOT EXISTS (
                    SELECT 1
                    FROM batch_table b
                    WHERE b.batch_id = a.batch_id
                    AND b.status = 'Completed'
                )
            """
            
            # Add sorting based on parameters
            valid_sort_fields = {
                'upload_date': 'a.upload_date',
                'allocation_date': 'a.allocation_date', 
                'image_count': 'a.image_count',
                'batch_id': 'a.batch_id'
            }
            
            if sort_by in valid_sort_fields:
                sort_field = valid_sort_fields[sort_by]
                order = 'DESC' if sort_order.lower() == 'desc' else 'ASC'
                base_query += f" ORDER BY {sort_field} {order}"
            else:
                base_query += " ORDER BY a.upload_date DESC"  # Default fallback
            
            cursor.execute(base_query, (session_id,))
            tasks = cursor.fetchall()

        # Format upload_date and add status information
        formatted_tasks = []
        for task in tasks:
            formatted_task = dict(task)
            if isinstance(formatted_task['upload_date'], datetime.datetime):
                formatted_task['upload_date'] = formatted_task['upload_date'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(formatted_task.get('allocation_date'), datetime.datetime):
                formatted_task['allocation_date'] = formatted_task['allocation_date'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Calculate completion status
            approved_count = formatted_task.get('approved_count', 0) or 0
            rejected_count = formatted_task.get('rejected_count', 0) or 0
            total_processed = approved_count + rejected_count
            image_count = formatted_task.get('image_count', 0) or 0
            
            # Determine status based on processing
            if total_processed == 0:
                status = 'Pending'
            elif total_processed < image_count:
                status = 'In Progress'
            elif total_processed == image_count:
                status = 'Completed'
            else:
                status = 'Error'
            
            formatted_task['status'] = status
            formatted_task['approved_count'] = approved_count
            formatted_task['rejected_count'] = rejected_count
            formatted_tasks.append(formatted_task)

        return jsonify(formatted_tasks)
    
    except Exception as e:
        print(f"Get QC tasks error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
    finally:
        conn.close()

@app.route("/get_qc_tasks_filtered", methods=["POST"])
@no_cache
def get_qc_tasks_filtered():
    if "user" not in session:
        print(f"Get QC tasks filtered accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "User not logged in"}), 401

    session_id = session['user']['unique_userid']
    data = request.get_json()
    batch_id_filter = data.get('batch_id', '').strip()
    sort_by = data.get('sort_by', 'upload_date')
    sort_order = data.get('sort_order', 'desc')
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            base_query = """
                SELECT a.batch_id, a.image_count, a.upload_date, a.approved_count, a.rejected_count, a.allocation_date
                FROM allocation_table a
                WHERE a.unique_userid = %s
                AND NOT EXISTS (
                    SELECT 1
                    FROM batch_table b
                    WHERE b.batch_id = a.batch_id
                    AND b.status = 'Completed'
                )
            """
            
            params = [session_id]
            
            # Add batch_id filter if provided
            if batch_id_filter:
                base_query += " AND a.batch_id LIKE %s"
                params.append(f"%{batch_id_filter}%")
            
            # Add sorting
            valid_sort_fields = {
                'upload_date': 'a.upload_date',
                'allocation_date': 'a.allocation_date', 
                'image_count': 'a.image_count',
                'batch_id': 'a.batch_id'
            }
            
            if sort_by in valid_sort_fields:
                sort_field = valid_sort_fields[sort_by]
                order = 'DESC' if sort_order.lower() == 'desc' else 'ASC'
                base_query += f" ORDER BY {sort_field} {order}"
            else:
                base_query += " ORDER BY a.upload_date DESC"
            
            cursor.execute(base_query, params)
            tasks = cursor.fetchall()

        # Format upload_date and add status information
        formatted_tasks = []
        for task in tasks:
            formatted_task = dict(task)
            if isinstance(formatted_task['upload_date'], datetime.datetime):
                formatted_task['upload_date'] = formatted_task['upload_date'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(formatted_task.get('allocation_date'), datetime.datetime):
                formatted_task['allocation_date'] = formatted_task['allocation_date'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Calculate completion status
            approved_count = formatted_task.get('approved_count', 0) or 0
            rejected_count = formatted_task.get('rejected_count', 0) or 0
            total_processed = approved_count + rejected_count
            image_count = formatted_task.get('image_count', 0) or 0
            
            # Determine status based on processing
            if total_processed == 0:
                status = 'Pending'
            elif total_processed < image_count:
                status = 'In Progress'
            elif total_processed == image_count:
                status = 'Completed'
            else:
                status = 'Error'
            
            formatted_task['status'] = status
            formatted_task['approved_count'] = approved_count
            formatted_task['rejected_count'] = rejected_count
            formatted_tasks.append(formatted_task)

        return jsonify(formatted_tasks)
    
    except Exception as e:
        print(f"Get QC tasks filtered error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
    finally:
        conn.close()

@app.route("/viewer")
@no_cache
def viewer():
    if 'user' not in session:
        print(f"Viewer accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))

    batch_id = request.args.get("batch_id")
    upload_date = request.args.get("upload_date")

    if not batch_id or not upload_date:
        return "Missing batch ID or upload date", 400

    try:
        # Parse upload_date to ensure correct format
        try:
            upload_date_dt = datetime.datetime.strptime(upload_date, '%Y-%m-%d %H:%M:%S')
            formatted_upload_date = upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"Viewer error: Invalid upload_date format for batch_id '{batch_id}' - {datetime.datetime.now()}")
            return "Invalid upload_date format. Expected YYYY-MM-DD HH:MM:SS", 400

        # Step 1: Query image_table for image_ids with matching batch_id and upload_date
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT image_id
                FROM image_table
                WHERE batch_id = %s AND date_uploaded = %s
            """, (batch_id, formatted_upload_date))
            valid_image_ids = [row['image_id'] for row in cursor.fetchall()]

            # Step 2: Query qc_table for QC status
            cursor.execute("""
                SELECT image_id, status
                FROM qc_table
                WHERE batch_id = %s
            """, (batch_id,))
            qc_status = {row['image_id']: row['status'] for row in cursor.fetchall()}
        conn.close()

        # Step 3: Get image list from S3
        folder_path = batch_id.replace("_", "/") + "/"
        print(f"Viewer: Fetching images for folder_path '{folder_path}' - {datetime.datetime.now()}")
        s3_filenames = get_image_list_from_s3(folder_path)
        print(f"Viewer: Found {len(s3_filenames)} filenames from S3: {s3_filenames} - {datetime.datetime.now()}")

        # Step 4: Filter S3 filenames to include only those in image_table
        filtered_filenames = [
            filename for filename in s3_filenames
            if os.path.basename(filename) in valid_image_ids
        ]
        print(f"Viewer: Filtered to {len(filtered_filenames)} filenames matching image_table: {filtered_filenames} - {datetime.datetime.now()}")

        # Step 5: Render template with filtered image list
        return render_template(
            "image_viewer.html",
            batch_id=batch_id,
            image_filenames=filtered_filenames,
            qc_status=qc_status
        )

    except Exception as e:
        print(f"Viewer error for batch_id '{batch_id}': {str(e)} - {datetime.datetime.now()}")
        return "Internal Server Error", 500

@app.route('/qc_status_insert', methods=['POST'])
@no_cache
def qc_status_insert_route():
    if 'user' not in session:
        print(f"QC status insert accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    try:
        data = request.get_json()
        batch_id = data.get("batch_id")
        upload_date = data.get("upload_date")

        if not batch_id or not upload_date:
            return jsonify({"success": False, "message": "Missing batch_id or upload_date"}), 400

        try:
            upload_date_dt = datetime.datetime.strptime(upload_date, '%Y-%m-%d %H:%M:%S')
            formatted_upload_date = upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return jsonify({"success": False, "message": "Invalid upload_date format. Expected YYYY-MM-DD HH:MM:SS"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT image_count FROM pandas_upload_table
                    WHERE batch_id = %s AND upload_date = %s
                """, (batch_id, formatted_upload_date))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"success": False, "message": "Upload info not found in pandas_upload_table"}), 404

                expected_image_count = row['image_count']

                cursor.execute("""
                    SELECT image_id, status 
                    FROM qc_table 
                    WHERE batch_id = %s
                """, (batch_id,))
                qc_status = {r['image_id']: r['status'] for r in cursor.fetchall()}

                approved_count = sum(1 for status in qc_status.values() if status == 'accepted')
                rejected_count = sum(1 for status in qc_status.values() if status == 'rejected')
                total_qc_images = len(qc_status)

                if total_qc_images != expected_image_count:
                    return jsonify({
                        "success": False,
                        "message": f"Complete QC to make submission. Expected {expected_image_count} images, got {total_qc_images}."
                    }), 400

                s1 = "Completed"
                current_time = datetime.datetime.now()
                cursor.execute("""
                    INSERT INTO batch_table (batch_id, upload_date, status, date)
                    VALUES (%s, %s, %s, %s)
                """, (batch_id, formatted_upload_date, s1, current_time))

                cursor.execute("""
                    UPDATE allocation_table
                    SET status = %s
                    WHERE batch_id = %s AND upload_date = %s
                """, (s1, batch_id, formatted_upload_date))

                # Get additional details for notification
                cursor.execute("""
                    SELECT unique_userid FROM allocation_table
                    WHERE batch_id = %s AND upload_date = %s
                """, (batch_id, formatted_upload_date))
                allocation = cursor.fetchone()
                qc_user_id = allocation['unique_userid'] if allocation else "Unknown"

                cursor.execute("SELECT name FROM user_table WHERE unique_userid = %s", (qc_user_id,))
                qc_user = cursor.fetchone()
                qc_user_name = qc_user['name'] if qc_user else "Unknown"

                cursor.execute("""
                    SELECT unique_userid FROM pandas_upload_table
                    WHERE batch_id = %s AND upload_date = %s
                """, (batch_id, formatted_upload_date))
                upload = cursor.fetchone()
                vendor_id = upload['unique_userid'] if upload else "Unknown"

                cursor.execute("SELECT name FROM user_table WHERE unique_userid = %s", (vendor_id,))
                vendor = cursor.fetchone()
                vendor_name = vendor['name'] if vendor else "Unknown"

                conn.commit()

            # Send email notification to admin for QC completion
            admin_email = get_admin_email()
            subject = f"Batch QC Completed: {batch_id}"
            body = (
                f"A batch has been marked as QC completed.\n\n"
                f"Batch ID: {batch_id}\n"
                f"Image Count: {expected_image_count}\n"
                f"Approved Count: {approved_count}\n"
                f"Rejected Count: {rejected_count}\n"
                f"Upload Date: {formatted_upload_date}\n"
                f"Completion Date: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"QC User ID: {qc_user_id}\n"
                f"QC User Name: {qc_user_name}\n"
                f"Vendor ID: {vendor_id}\n"
                f"Vendor Name: {vendor_name}\n"
            )
            send_email(subject, body, admin_email)

            return jsonify({"success": True, "message": "QC status marked as complete."})

        except Exception as e:
            conn.rollback()
            print(f"QC status insert error: {str(e)}")
            return jsonify({"success": False, "message": str(e)}), 500

        finally:
            conn.close()

    except Exception as e:
        print(f"QC status insert error: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/update_qc_status', methods=['POST'])
@no_cache
def update_qc_status():
    if 'user' not in session:
        print(f"Update QC status accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    user = session['user']['unique_userid']
    data = request.get_json()
    
    batch_id = data.get('batch_id')
    image_id = data.get('image_id')
    status = data.get('status')
    remarks = data.get('remarks', 'ok' if status == 'accepted' else '')
    orientation_error = data.get('orientation_error', False)

    if not batch_id or not image_id or not status:
        return jsonify({"error": "Missing required fields"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM qc_table 
                WHERE batch_id = %s AND image_id = %s
            """, (batch_id, image_id))
            exists = cursor.fetchone()['count'] > 0

            current_time = datetime.datetime.now()

            if exists:
                cursor.execute("""
                    UPDATE qc_table 
                    SET status = %s, qc_date = %s, remarks = %s, orientation_error = %s
                    WHERE batch_id = %s AND image_id = %s
                """, (status, current_time, remarks, orientation_error, batch_id, image_id))
            else:
                cursor.execute("""
                    INSERT INTO qc_table (unique_userid, batch_id, image_id, status, qc_date, remarks, orientation_error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (user, batch_id, image_id, status, current_time, remarks, orientation_error))

        conn.commit()
        return jsonify({"message": "QC status updated successfully", "success": True})

    except Exception as e:
        conn.rollback()
        print(f"Update QC status error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/get_images')
@no_cache
def get_images():
    if 'user' not in session:
        print(f"Get images accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    batch_id = request.args.get('batch_id')
    upload_date = request.args.get('upload_date')

    if not batch_id:
        print(f"Get images error: Missing batch_id - {datetime.datetime.now()}")
        return jsonify({"error": "Missing batch_id"}), 400

    try:
        # Step 1: Query image_table for valid image_ids
        conn = get_db_connection()
        with conn.cursor() as cursor:
            if upload_date:
                try:
                    upload_date_dt = datetime.datetime.strptime(upload_date, '%Y-%m-%d %H:%M:%S')
                    formatted_upload_date = upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute("""
                        SELECT image_id
                        FROM image_table
                        WHERE batch_id = %s AND date_uploaded = %s
                    """, (batch_id, formatted_upload_date))
                except ValueError:
                    conn.close()
                    print(f"Get images error: Invalid upload_date format - {datetime.datetime.now()}")
                    return jsonify({"error": "Invalid upload_date format"}), 400
            else:
                cursor.execute("""
                    SELECT image_id
                    FROM image_table
                    WHERE batch_id = %s
                """, (batch_id,))
            valid_image_ids = [row['image_id'] for row in cursor.fetchall()]
        conn.close()

        # Step 2: Get images from S3
        prefix = batch_id.replace('_', '/') + '/'
        print(f"Get images: Listing objects with prefix '{prefix}' in bucket '{SPACES_NAME}' - {datetime.datetime.now()}")
        response = s3_client.list_objects_v2(Bucket=SPACES_NAME, Prefix=prefix)
        contents = response.get('Contents', [])
        images = []

        print(f"Get images: Found {len(contents)} objects for prefix '{prefix}' - {datetime.datetime.now()}")

        # Step 3: Filter and generate signed URLs
        for content in contents:
            key = content['Key']
            filename = os.path.basename(key)
            print(f"Get images: Examining key '{key}' - {datetime.datetime.now()}")
            if filename in valid_image_ids and key.lower().endswith(('.jpg', '.jpeg', '.png')):
                mime_type = 'image/png' if key.lower().endswith('.png') else 'image/jpeg'
                signed_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': SPACES_NAME,
                        'Key': key,
                        'ResponseContentDisposition': 'inline',
                        'ResponseContentType': mime_type
                    },
                    ExpiresIn=3600
                )
                images.append(signed_url)
                print(f"Get images: Added signed URL for '{key}' - {datetime.datetime.now()}")

        if not images:
            print(f"Get images: No images found for prefix '{prefix}' after filtering - {datetime.datetime.now()}")
            return jsonify({"message": f"No images found for batch_id '{batch_id}'", "images": []}), 200

        print(f"Get images: Returning {len(images)} image URLs - {datetime.datetime.now()}")
        return jsonify({"images": images})

    except Exception as e:
        print(f"Get images error: Failed to list objects for prefix '{prefix}': {str(e)} - {datetime.datetime.now()}")
        return jsonify({"error": f"Failed to retrieve images: {str(e)}", "images": []}), 500

@app.route('/debug_s3', methods=['GET'])
@no_cache
def debug_s3():
    if 'user' not in session:
        print(f"Debug S3 accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    try:
        response = s3_client.list_objects_v2(Bucket=SPACES_NAME)
        objects = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Debug S3: Found {len(objects)} objects in bucket '{SPACES_NAME}' - {datetime.datetime.now()}")
        return jsonify({"objects": objects, "count": len(objects)})
    except Exception as e:
        print(f"Debug S3 error: {str(e)} - {datetime.datetime.now()}")
        return jsonify({"error": str(e), "objects": []}), 500

@app.route('/qc_history')
@no_cache
def qc_history():
    if 'user' not in session:
        print(f"QC history accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('qc_history.html')

@app.route('/get_qc_history', methods=['GET'])
@no_cache
def get_qc_history():
    if 'user' not in session:
        print(f"Get QC history accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403

    unique_userid = session['user']['unique_userid']

    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    query = """
        SELECT a.batch_id, a.upload_date, b.status
        FROM allocation_table a
        INNER JOIN batch_table b
        ON a.batch_id = b.batch_id AND a.upload_date = b.upload_date
        WHERE a.unique_userid = %s
        AND b.status = 'Completed'
        ORDER BY a.upload_date DESC
    """
    cursor.execute(query, (unique_userid,))
    rows = cursor.fetchall()

    history_data = []
    for row in rows:
        history_data.append({
            "batch_id": row["batch_id"],
            "status": row["status"],
            "upload_date": str(row["upload_date"])
        })

    cursor.close()
    conn.close()

    return jsonify({"history": history_data})

@app.route('/download_report_allocation', methods=['GET'])
@no_cache
def download_report_allocation():
    if 'user' not in session:
        print(f"Download report allocation accessed without session: {datetime.datetime.now()}")
        return jsonify({'error': 'Unauthorized access'}), 403

    batch_id = request.args.get('batch_id')
    upload_date = request.args.get('upload_date')

    if not batch_id or not upload_date:
        return jsonify({'message': 'Missing batch_id or upload_date'}), 400

    try:
        # Parse upload_date
        try:
            parsed_upload_date = datetime.datetime.strptime(upload_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return jsonify({'message': 'Invalid upload_date format. Expected YYYY-MM-DD HH:MM:SS'}), 400

        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                # Get vendor information
                cursor.execute("""
                    SELECT unique_userid 
                    FROM pandas_upload_table 
                    WHERE batch_id = %s
                """, (batch_id,))
                vendor_result = cursor.fetchone()
                vendor_id = vendor_result['unique_userid'] if vendor_result else None

                vendor_name = None
                if vendor_id:
                    cursor.execute("""
                        SELECT name 
                        FROM user_table 
                        WHERE unique_userid = %s
                    """, (vendor_id,))
                    name_result = cursor.fetchone()
                    vendor_name = name_result['name'] if name_result else None

                # Get image IDs
                cursor.execute("""
                    SELECT image_id
                    FROM image_table
                    WHERE batch_id = %s AND date_uploaded = %s
                """, (batch_id, parsed_upload_date))
                image_rows = cursor.fetchall()

                if not image_rows:
                    return jsonify({'message': 'No images found for this batch and upload date'}), 200

                image_ids = [row['image_id'] for row in image_rows]

                # Get QC data
                format_placeholders = ','.join(['%s'] * len(image_ids))
                qc_query = f"""
                    SELECT image_id, status, remarks, orientation_error
                    FROM qc_table
                    WHERE batch_id = %s AND image_id IN ({format_placeholders})
                """
                cursor.execute(qc_query, [batch_id] + image_ids)
                qc_data = cursor.fetchall()

                if not qc_data:
                    return jsonify({'message': 'No QC data available for this batch'}), 200

                # Create DataFrame and Excel file
                df = pd.DataFrame(qc_data)
                df.insert(0, 'upload_date', upload_date)
                df.insert(0, 'batch_id', batch_id)
                df.insert(0, 'vendor_name', vendor_name or '')
                df.insert(0, 'vendor_id', vendor_id or '')

                df['orientation_error'] = df['orientation_error'].apply(lambda x: 'Yes' if x == 1 else 'No')

                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='QC_Report')

                output.seek(0)
                filename = f"{batch_id}_QC_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

                return send_file(
                    output,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name=filename
                )

        except Exception as e:
            print(f"Download report allocation error: {str(e)}")
            return jsonify({'message': 'Internal server error. Please try again later.'}), 500
        finally:
            connection.close()

    except Exception as e:
        print(f"Download report allocation error: {str(e)}")
        return jsonify({'message': 'Internal server error. Please try again later.'}), 500

@app.route('/check_session', methods=['GET'])
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

def shutdown_scheduler():
    if scheduler is not None and scheduler.running:
        scheduler.shutdown()

if scheduler is not None:
    atexit.register(shutdown_scheduler)
    
  
  
    
    
# Report.html
@app.route('/qc_report')
@app.route('/qc_report')
@no_cache
def qc_report():
    if 'user' not in session:
        print(f"QC report accessed without session: {datetime.now()}")
        return redirect(url_for('login'))
    print("Rendering qc_report.html")  # Debug log
    return render_template('qc_report.html')

@app.route('/get_qc_report', methods=['GET'])
@no_cache
def get_qc_report():
    if 'user' not in session:
        print(f"Get QC report accessed without session: {datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            query = """
                SELECT q.batch_id, q.image_id, q.status, q.qc_date, q.remarks, q.orientation_error,
                       q.unique_userid, u.name as qc_reviewer_name
                FROM qc_table q
                LEFT JOIN user_table u ON q.unique_userid = u.unique_userid
                ORDER BY q.qc_date DESC
            """
            print(f"Executing query: {query}")  # Debug log
            cursor.execute(query)
            qc_data = cursor.fetchall()
            print(f"QC data fetched: {len(qc_data)} rows")  # Debug log
            
            # Format the dates and orientation_error for JSON
            for row in qc_data:
                if row['qc_date'] is not None:
                    row['qc_date'] = row['qc_date'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    row['qc_date'] = 'N/A'
                row['orientation_error'] = 'Yes' if row['orientation_error'] else 'No'
                
        conn.close()
        print("QC report data sent successfully")  # Debug log
        return jsonify({"data": qc_data})
        
    except Exception as e:
        print(f"Get QC report error: {str(e)}")
        return jsonify({"error": f"Failed to fetch QC report: {str(e)}"}), 500

@app.route('/get_filter_data', methods=['GET'])
@no_cache
def get_filter_data():
    if 'user' not in session:
        print(f"Get filter data accessed without session: {datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Fetch unique users
            user_query = """
                SELECT DISTINCT u.unique_userid, u.name
                FROM user_table u
                JOIN qc_table q ON u.unique_userid = q.unique_userid
                ORDER BY u.name
            """
            print(f"Executing user query: {user_query}")  # Debug log
            cursor.execute(user_query)
            users = cursor.fetchall()
            print(f"Users fetched: {len(users)} users")  # Debug log
            
            # Fetch distinct dates with data
            date_query = """
                SELECT DISTINCT DATE(qc_date) as qc_date
                FROM qc_table
                WHERE qc_date IS NOT NULL
                ORDER BY qc_date DESC
            """
            print(f"Executing date query: {date_query}")  # Debug log
            cursor.execute(date_query)
            dates = cursor.fetchall()
            print(f"Dates fetched: {len(dates)} dates")  # Debug log
            
            # Format dates for JSON
            formatted_dates = [row['qc_date'].strftime('%Y-%m-%d') for row in dates if row['qc_date']]
            
        conn.close()
        print("Filter data sent successfully")  # Debug log
        return jsonify({"users": users, "dates": formatted_dates})
        
    except Exception as e:
        print(f"Get filter data error: {str(e)}")
        return jsonify({"error": f"Failed to fetch filter data: {str(e)}"}), 500
  
  
@app.route('/download_qc_batch_report/<batch_id>')
@no_cache
def download_qc_batch_report(batch_id):
    if 'user' not in session:
        print(f"Download QC batch report accessed without session: {datetime.datetime.now()}")
        return jsonify({'error': 'Unauthorized access'}), 403

    try:
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                # Get vendor information
                cursor.execute("""
                    SELECT unique_userid 
                    FROM pandas_upload_table 
                    WHERE batch_id = %s
                """, (batch_id,))
                vendor_result = cursor.fetchone()
                vendor_id = vendor_result['unique_userid'] if vendor_result else None

                vendor_name = None
                if vendor_id:
                    cursor.execute("""
                        SELECT name 
                        FROM user_table 
                        WHERE unique_userid = %s
                    """, (vendor_id,))
                    name_result = cursor.fetchone()
                    vendor_name = name_result['name'] if name_result else None

                # Get QC data for the batch
                cursor.execute("""
                    SELECT image_id, status, remarks, orientation_error, qc_date
                    FROM qc_table
                    WHERE batch_id = %s
                """, (batch_id,))
                qc_data = cursor.fetchall()

                if not qc_data:
                    return jsonify({'message': 'No QC data available for this batch'}), 200

                # Create DataFrame
                df = pd.DataFrame(qc_data)
                df.insert(0, 'batch_id', batch_id)
                df.insert(0, 'vendor_name', vendor_name or '')
                df.insert(0, 'vendor_id', vendor_id or '')

                # Decode image_id
                df['image_id'] = df['image_id'].apply(lambda x: unquote(x) if x else x)

                # Format orientation_error and qc_date
                df['orientation_error'] = df['orientation_error'].apply(lambda x: 'Yes' if x else 'No')
                df['qc_date'] = df['qc_date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if x else 'N/A')

                # Create Excel file in memory
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='QC_Batch_Report')

                output.seek(0)
                filename = f"{batch_id}_QC_Batch_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

                return send_file(
                    output,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name=filename
                )

        except Exception as e:
            print(f"Download QC batch report error: {str(e)}")
            return jsonify({'message': 'Internal server error. Please try again later.'}), 500
        finally:
            connection.close()

    except Exception as e:
        print(f"Download QC batch report error: {str(e)}")
        return jsonify({'message': 'Internal server error. Please try again later.'}), 500
  
  

  
@app.route('/get_image_list', methods=['GET'])

@app.route('/get_image_url', methods=['GET'])
@no_cache
def get_image_url():
    if 'user' not in session:
        print(f"Get image URL accessed without session: {dt.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        batch_id = request.args.get('batch_id')
        image_id = request.args.get('image_id')
        
        if not batch_id or not image_id:
            print(f"Missing batch_id or image_id: batch_id={batch_id}, image_id={image_id} - {dt.now()}")
            return jsonify({"error": "Missing batch_id or image_id"}), 400
        
        # Validate image_id extension
        if not image_id.lower().endswith(('.jpg', '.jpeg', '.png')):
            print(f"Invalid image extension: {image_id} - {dt.now()}")
            return jsonify({"error": "Invalid image file extension"}), 400
        
        # Validate image_id exists in image_table for the batch_id
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT image_id
                    FROM image_table
                    WHERE batch_id = %s AND image_id = %s
                """, (batch_id, image_id))
                result = cursor.fetchone()
            conn.close()
            
            if not result:
                print(f"Image ID '{image_id}' not found in image_table for batch_id '{batch_id}' - {dt.now()}")
                return jsonify({"error": f"Image ID '{image_id}' not found for batch '{batch_id}'"}), 404
        except Exception as e:
            print(f"Database error validating image_id '{image_id}' for batch_id '{batch_id}': {str(e)} - {dt.now()}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        
        # Construct S3 path
        folder_path = batch_id.replace('_', '/') + '/'
        s3_path = folder_path + image_id
        print(f"Constructed S3 path: {s3_path} - {dt.now()}")
        
        # Generate presigned URL
        try:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': SPACES_NAME, 'Key': s3_path},
                ExpiresIn=300  # URL expires in 5 minutes
            )
            print(f"Generated presigned URL for {s3_path}: {dt.now()}")
            return jsonify({"url": presigned_url})
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"S3 ClientError for {s3_path}: {error_code} - {error_message} - {dt.now()}")
            
            # If the key wasn't found, try an alternative path format
            if error_code == 'NoSuchKey':
                try:
                    # Try alternative path format
                    alternative_path = f"{batch_id}/{image_id}"
                    print(f"Trying alternative S3 path: {alternative_path} - {dt.now()}")
                    
                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': SPACES_NAME, 'Key': alternative_path},
                        ExpiresIn=300
                    )
                    print(f"Generated presigned URL for alternative path {alternative_path}: {dt.now()}")
                    return jsonify({"url": presigned_url})
                except ClientError as alt_e:
                    print(f"Alternative S3 path also failed: {alt_e.response['Error']['Code']} - {alt_e.response['Error']['Message']} - {dt.now()}")
            
            return jsonify({"error": f"S3 error: {error_code} - {error_message}"}), 500
        
    except Exception as e:
        print(f"Get image URL error: {str(e)} - {dt.now()}")
        return jsonify({"error": f"Failed to generate image URL: {str(e)}"}), 500


# Define Blueprint for QC routes
qc_bp = Blueprint('qc', __name__, url_prefix='/qc')  # Added url_prefix

@qc_bp.route('/update_image_status', methods=['POST'])
def update_image_status():
    # Check if the request is expecting JSON
    if not request.is_json:
        return jsonify({'error': 'Request must be JSON'}), 400
    
    try:
        # Get JSON data from the request
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Invalid JSON data'}), 400
            
        batch_id = data.get('batch_id')
        image_id = data.get('image_id')
        status = data.get('status')

        # Print received data for debugging
        print(f"Received data: batch_id={batch_id}, image_id={image_id}, status={status}")

        # Validate input
        if not batch_id or not image_id or not status:
            return jsonify({'error': 'Missing required fields (batch_id, image_id, status)'}), 400

        if status.lower() not in ['accepted', 'rejected']:
            return jsonify({'error': 'Invalid status value'}), 400

        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Check if record exists first
            check_query = """
                SELECT batch_id FROM qc_table 
                WHERE batch_id = %s AND image_id = %s
            """
            cursor.execute(check_query, (batch_id, image_id))
            record = cursor.fetchone()

            # Print check result for debugging
            print(f"Existing record check: {record}")

            if record:
                # Update only the status field of the existing record
                update_query = """
                    UPDATE qc_table
                    SET status = %s
                    WHERE batch_id = %s AND image_id = %s
                """
                cursor.execute(update_query, (
                    status.lower(),
                    batch_id,
                    image_id
                ))
                print(f"Updated record, rows affected: {cursor.rowcount}")
            else:
                # If no record exists, return an error (or insert if that's the desired behavior)
                return jsonify({'error': 'No record found for the given batch_id and image_id'}), 404

            # Commit the transaction
            conn.commit()
            
            return jsonify({'message': f'Image status updated to {status.lower()} successfully'}), 200
            
        except Exception as e:
            conn.rollback()
            print(f"Database operation error: {str(e)}")
            return jsonify({'error': f'Database operation error: {str(e)}'}), 500
        finally:
            # Close the database connection
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"Unexpected error in update_image_status: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500
# Make sure to register the blueprint in your main app

app.register_blueprint(qc_bp)

@app.route('/download_batches')
@no_cache
def download_batches():
    if 'user' not in session:
        print(f"Download batches accessed without session: {datetime.datetime.now()}")
        return redirect(url_for('login'))
    return render_template('download_batches.html')

@app.route('/api/download/batch-ids', methods=['GET'])
@no_cache
def get_download_batch_ids():
    if 'user' not in session:
        print(f"Get download batch IDs accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT batch_id 
                FROM pandas_upload_table 
                ORDER BY batch_id DESC
            """)
            batch_ids = [row['batch_id'] for row in cursor.fetchall()]
        conn.close()
        return jsonify({"batch_ids": batch_ids})
    except Exception as e:
        print(f"Error fetching batch IDs: {str(e)}")
        return jsonify({"error": "Failed to fetch batch IDs"}), 500

@app.route('/api/download/vendors', methods=['GET'])
@no_cache
def get_download_vendors():
    if 'user' not in session:
        print(f"Get download vendors accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT u.unique_userid, u.name 
                FROM user_table u
                INNER JOIN pandas_upload_table p ON u.unique_userid = p.unique_userid
                WHERE u.user_role = 'vendor'
                ORDER BY u.name
            """)
            vendors = cursor.fetchall()
        conn.close()
        return jsonify({"vendors": vendors})
    except Exception as e:
        print(f"Error fetching vendors: {str(e)}")
        return jsonify({"error": "Failed to fetch vendors"}), 500

@app.route('/api/download/search', methods=['POST'])
@no_cache
def search_download_batches():
    if 'user' not in session:
        print(f"Search download batches accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        data = request.get_json()
        batch_id = data.get('batch_id')
        status = data.get('status')
        vendor = data.get('vendor')
        
        if not batch_id:
            return jsonify({"error": "Batch ID is required"}), 400
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Base query to get batch information
            query = """
                SELECT p.batch_id, p.upload_date, p.image_count, p.unique_userid,
                       u.name as vendor_name,
                       COALESCE(a.approved_count, 0) as accepted_count,
                       COALESCE(a.rejected_count, 0) as rejected_count
                FROM pandas_upload_table p
                LEFT JOIN user_table u ON p.unique_userid = u.unique_userid
                LEFT JOIN allocation_table a ON p.batch_id = a.batch_id AND p.upload_date = a.upload_date
                WHERE p.batch_id = %s
            """
            params = [batch_id]
            
            if vendor:
                query += " AND p.unique_userid = %s"
                params.append(vendor)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Format dates for JSON
            for row in results:
                if isinstance(row.get('upload_date'), datetime.datetime):
                    row['upload_date'] = row['upload_date'].strftime('%Y-%m-%d %H:%M:%S')
        
        conn.close()
        return jsonify({"data": results})
        
    except Exception as e:
        print(f"Error searching download batches: {str(e)}")
        return jsonify({"error": "Failed to search batches"}), 500

@app.route('/api/download/batch', methods=['POST'])
@no_cache
def download_batch_zip():
    if 'user' not in session:
        print(f"Download batch ZIP accessed without session: {datetime.datetime.now()}")
        return jsonify({"error": "Unauthorized access"}), 403
    
    try:
        import zipfile
        import tempfile
        import requests
        from urllib.parse import urlparse
        
        data = request.get_json()
        batch_id = data.get('batch_id')
        upload_date = data.get('upload_date')
        status_filter = data.get('status', 'all')
        
        if not batch_id or not upload_date:
            return jsonify({"error": "Batch ID and upload date are required"}), 400
        
        # Parse upload date
        try:
            upload_date_dt = datetime.datetime.strptime(upload_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return jsonify({"error": "Invalid upload date format"}), 400
        
        conn = get_db_connection()
        
        # Get image IDs from image_table
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT image_id 
                FROM image_table 
                WHERE batch_id = %s AND date_uploaded = %s
            """, (batch_id, upload_date_dt))
            image_ids = [row['image_id'] for row in cursor.fetchall()]
        
        if not image_ids:
            conn.close()
            return jsonify({"error": "No images found for this batch"}), 404
        
        # Filter images based on status if specified
        filtered_image_ids = image_ids
        if status_filter in ['accepted', 'rejected']:
            with conn.cursor() as cursor:
                format_strings = ','.join(['%s'] * len(image_ids))
                cursor.execute(f"""
                    SELECT image_id 
                    FROM qc_table 
                    WHERE batch_id = %s AND image_id IN ({format_strings}) AND status = %s
                """, [batch_id] + image_ids + [status_filter])
                filtered_image_ids = [row['image_id'] for row in cursor.fetchall()]
        
        conn.close()
        
        if not filtered_image_ids:
            return jsonify({"error": f"No {status_filter} images found for this batch"}), 404
        
        # Create temporary ZIP file
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        temp_zip.close()
        
        try:
            with zipfile.ZipFile(temp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for image_id in filtered_image_ids:
                    try:
                        # Construct S3 path
                        s3_path = batch_id.replace('_', '/') + '/' + image_id
                        
                        # Generate presigned URL
                        presigned_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': SPACES_NAME, 'Key': s3_path},
                            ExpiresIn=300
                        )
                        
                        # Download image from S3
                        response = requests.get(presigned_url, timeout=30)
                        if response.status_code == 200:
                            # Add image to ZIP
                            zipf.writestr(image_id, response.content)
                        else:
                            print(f"Failed to download image {image_id}: HTTP {response.status_code}")
                    
                    except Exception as e:
                        print(f"Error downloading image {image_id}: {str(e)}")
                        continue
            
            # Send ZIP file
            filename = f"{batch_id}_{status_filter}_images_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            
            def remove_file(response):
                try:
                    os.unlink(temp_zip.name)
                except:
                    pass
                return response
            
            return send_file(
                temp_zip.name,
                mimetype='application/zip',
                as_attachment=True,
                download_name=filename
            )
        
        except Exception as e:
            # Clean up temp file on error
            try:
                os.unlink(temp_zip.name)
            except:
                pass
            raise e
            
    except Exception as e:
        print(f"Error creating batch ZIP: {str(e)}")
        return jsonify({"error": f"Failed to create ZIP file: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(debug=True)
    
    
    
    
    
    
    

