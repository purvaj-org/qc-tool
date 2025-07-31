from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
import bcrypt
import datetime
from utils.database import get_db_connection
from utils.auth import no_cache, login_required, api_login_required, api_role_required
from utils.email import send_email, get_admin_email
from utils.helpers import get_vendor_mapping, get_qc_users

admin_bp = Blueprint('admin', __name__)

@admin_bp.route('/admin')
@no_cache
@login_required
def admin():
    return render_template('admin.html', session_id=session['user']['unique_userid'])

@admin_bp.route('/ready_to_allocate')
@no_cache
@login_required
def ready_to_allocate():
    return render_template('admin.html')

@admin_bp.route('/get_ready_to_allocate')
@no_cache
@api_role_required('admin')
def get_ready_to_allocate():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM pandas_upload_table p WHERE NOT EXISTS (SELECT 1 FROM allocation_table a WHERE a.batch_id = p.batch_id AND a.upload_date = p.upload_date AND a.status != 'revoked');")
        batches = cursor.fetchall()

        vendor_mapping = get_vendor_mapping()
        qc_users = get_qc_users()

    conn.close()

    for batch in batches:
        batch["vendor_name"] = vendor_mapping.get(batch["unique_userid"], "Unknown")
        batch["qc_users"] = qc_users

    return jsonify(batches)

@admin_bp.route('/allocate_qc', methods=['POST'])
@no_cache
@api_role_required('admin')
def allocate_qc():
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

@admin_bp.route('/allocation_history')
@no_cache
@login_required
def allocation_history():
    return render_template('allocation_history.html')

@admin_bp.route('/get_allocation_history', methods=['GET'])
@no_cache
@api_role_required('admin')
def get_allocation_history():
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

@admin_bp.route('/revoke_allocation', methods=['POST'])
@no_cache
@api_role_required('admin')
def revoke_allocation():
    data = request.json
    batch_id = data.get('batch_id')

    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("UPDATE allocation_table SET status='revoked' WHERE batch_id=%s", (batch_id,))
    conn.commit()
    conn.close()

    return jsonify({"message": f"Allocation revoked for batch {batch_id}."})

# User Management Routes
@admin_bp.route('/qc_user')
@no_cache
@login_required
def qc_user():
    return render_template('qc_user.html')

@admin_bp.route("/get_qc_users")
@no_cache
@api_role_required('admin')
def get_qc_users_route():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT unique_userid, name, email FROM user_table WHERE user_role = 'qc'")
        qc_users = cursor.fetchall()
    conn.close()
    
    return jsonify(qc_users)

@admin_bp.route("/add_qc_user", methods=["POST"])
@no_cache
@api_role_required('admin')
def add_qc_user():
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
        except Exception as e:
            conn.rollback()
            print(f"Add QC user error: {str(e)}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        finally:
            conn.close()

        return jsonify({"message": "QC user added successfully!", "success": True})

    except Exception as e:
        print(f"Add QC user error: {str(e)}")
        return jsonify({"error": "Server error"}), 500

@admin_bp.route('/upload_user')
@no_cache
@login_required
def upload_user():
    return render_template('upload_user.html')

@admin_bp.route("/get_upload_users")
@no_cache
@api_role_required('admin')
def get_upload_users():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT unique_userid, name, email FROM user_table WHERE user_role = 'vendor'")
        upload_users = cursor.fetchall()
    conn.close()
    
    return jsonify(upload_users)

@admin_bp.route("/add_upload_user", methods=["POST"])
@no_cache
@api_role_required('admin')
def add_upload_user():
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
        except Exception as e:
            conn.rollback()
            print(f"Add upload user error: {str(e)}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        finally:
            conn.close()

        return jsonify({"message": "Upload user added successfully!", "success": True})

    except Exception as e:
        print(f"Add upload user error: {str(e)}")
        return jsonify({"error": "Server error"}), 500 