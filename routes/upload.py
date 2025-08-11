from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
import os
import datetime
import pymysql
from s3_upload import upload_to_s3
from utils.database import get_db_connection
from utils.auth import no_cache, login_required, api_login_required
from utils.email import send_email, get_admin_email

upload_bp = Blueprint('upload', __name__)

# Store upload progress (Use Redis in production)
upload_progress = {}

@upload_bp.route('/upload', methods=['GET', 'POST'])
@no_cache
@login_required
def upload():
    return render_template('upload.html', session_id=session['user']['unique_userid'])

@upload_bp.route("/get_vendor_data")
@no_cache
@api_login_required
def get_vendor_data():
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

@upload_bp.route("/check_batch_exists", methods=["POST"])
@no_cache
@api_login_required
def check_batch_exists():
    """Check if a batch already exists for Complete upload type."""
    session_id = session['user']['unique_userid']
    location = request.json.get("location")
    panda_name = request.json.get("panda_name")
    bahi_name = request.json.get("bahi_name")
    record_type = request.json.get("record_type")
    upload_type = request.json.get("upload_type")

    if not all([location, panda_name, bahi_name, record_type, upload_type]):
        return jsonify({"error": "Missing required fields"}), 400

    # Only check for Complete uploads
    if upload_type != "complete":
        return jsonify({"exists": False})

    # Generate batch_id using same logic as upload_images
    batch_id = f"{session_id}_{location}_{panda_name}_{bahi_name}_{record_type}"

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM pandas_upload_table 
                WHERE batch_id = %s
            """, (batch_id,))
            result = cursor.fetchone()
            exists = result['count'] > 0
        
        conn.close()
        return jsonify({"exists": exists, "batch_id": batch_id})

    except Exception as e:
        print(f"Check batch exists error: {str(e)}")
        return jsonify({"error": "Database error"}), 500

@upload_bp.route("/upload_images", methods=["POST"])
@no_cache
@api_login_required
def upload_images():
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
                    # S3 path is same for all upload types - reuploads overwrite originals
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

@upload_bp.route("/upload_progress", methods=["GET"])
@no_cache
def upload_progress_status():
    session_id = session.get('user', {}).get('unique_userid')
    if not session_id or session_id not in upload_progress:
        return jsonify({"progress": 0})
    progress = upload_progress.get(session_id, 0)
    return jsonify({"progress": progress})

@upload_bp.route("/reset_progress", methods=["POST"])
@no_cache
def reset_progress():
    session_id = session.get('user', {}).get('unique_userid')
    if session_id in upload_progress:
        del upload_progress[session_id]
    return jsonify({"message": "Progress reset"})

@upload_bp.route('/upload_history')
@no_cache
@login_required
def upload_history():
    return render_template('upload_history.html', session_id=session['user']['unique_userid'])

@upload_bp.route('/get_upload_history', methods=['GET'])
@no_cache
@api_login_required
def get_upload_history():
    unique_userid = session['user']['unique_userid']
    
    # Get pagination parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset = (page - 1) * per_page

    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Get total count for pagination
            count_query = """
                SELECT COUNT(*) as total
                FROM pandas_upload_table
                WHERE unique_userid = %s
            """
            cursor.execute(count_query, (unique_userid,))
            total_result = cursor.fetchone()
            total_records = total_result['total'] if total_result else 0
            
            # Get paginated data
            query = """
                SELECT upload_date, batch_id, location, pandas_name, 
                       bahi_name, upload_type, image_count
                FROM pandas_upload_table
                WHERE unique_userid = %s
                ORDER BY upload_date DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (unique_userid, per_page, offset))
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
        
        # Calculate pagination info
        total_pages = (total_records + per_page - 1) // per_page
        has_next = page < total_pages
        has_prev = page > 1
        
        return jsonify({
            "history": uploads,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev
            }
        })

    except pymysql.MySQLError as e:
        print(f"Get upload history error: {str(e)}")
        return jsonify({"error": "Database error"}), 500

    except Exception as e:
        print(f"Get upload history error: {str(e)}")
        return jsonify({"error": "Unexpected server error"}), 500

@upload_bp.route('/get_upload_history_filter_options', methods=['GET'])
@no_cache
@api_login_required
def get_upload_history_filter_options():
    """Get unique values for filter dropdowns"""
    unique_userid = session['user']['unique_userid']

    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Get unique locations
            cursor.execute("""
                SELECT DISTINCT location 
                FROM pandas_upload_table 
                WHERE unique_userid = %s AND location IS NOT NULL AND location != ''
                ORDER BY location
            """, (unique_userid,))
            locations = [row['location'] for row in cursor.fetchall()]

            # Get unique panda names
            cursor.execute("""
                SELECT DISTINCT pandas_name 
                FROM pandas_upload_table 
                WHERE unique_userid = %s AND pandas_name IS NOT NULL AND pandas_name != ''
                ORDER BY pandas_name
            """, (unique_userid,))
            panda_names = [row['pandas_name'] for row in cursor.fetchall()]

        conn.close()
        return jsonify({
            "success": True,
            "locations": locations,
            "panda_names": panda_names
        })

    except Exception as e:
        print(f"Get filter options error: {str(e)}")
        return jsonify({"success": False, "error": "Database error"}), 500 