from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
import datetime
import pymysql
import os
from datetime import datetime as dt
from botocore.exceptions import ClientError
from s3_upload import s3_client, get_image_list_from_s3, SPACES_NAME
from utils.database import get_db_connection
from utils.auth import no_cache, login_required, api_login_required, api_role_required
from utils.email import send_email, get_admin_email
from utils.helpers import format_datetime, validate_date_string

qc_bp = Blueprint('qc', __name__)

@qc_bp.route('/qc')
@no_cache
@login_required
def qc():
    return render_template('qc.html', session_id=session['user']['unique_userid'])

@qc_bp.route("/get_qc_tasks", methods=["GET"])
@no_cache
@api_role_required('qc')
def get_qc_tasks():
    session_id = session['user']['unique_userid']
    sort_by = request.args.get('sort_by', 'upload_date')
    sort_order = request.args.get('sort_order', 'desc')
    
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
            
            cursor.execute(base_query, (session_id,))
            tasks = cursor.fetchall()

        formatted_tasks = []
        for task in tasks:
            formatted_task = dict(task)
            formatted_task['upload_date'] = format_datetime(formatted_task['upload_date'])
            formatted_task['allocation_date'] = format_datetime(formatted_task.get('allocation_date'))
            
            # Calculate completion status
            approved_count = formatted_task.get('approved_count', 0) or 0
            rejected_count = formatted_task.get('rejected_count', 0) or 0
            total_processed = approved_count + rejected_count
            image_count = formatted_task.get('image_count', 0) or 0
            
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

@qc_bp.route("/get_qc_tasks_filtered", methods=["POST"])
@no_cache
@api_role_required('qc')
def get_qc_tasks_filtered():
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
            
            if batch_id_filter:
                base_query += " AND a.batch_id LIKE %s"
                params.append(f"%{batch_id_filter}%")
            
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

        formatted_tasks = []
        for task in tasks:
            formatted_task = dict(task)
            formatted_task['upload_date'] = format_datetime(formatted_task['upload_date'])
            formatted_task['allocation_date'] = format_datetime(formatted_task.get('allocation_date'))
            
            # Calculate completion status
            approved_count = formatted_task.get('approved_count', 0) or 0
            rejected_count = formatted_task.get('rejected_count', 0) or 0
            total_processed = approved_count + rejected_count
            image_count = formatted_task.get('image_count', 0) or 0
            
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

@qc_bp.route("/viewer")
@no_cache
@login_required
def viewer():
    batch_id = request.args.get("batch_id")
    upload_date = request.args.get("upload_date")

    if not batch_id or not upload_date:
        return "Missing batch ID or upload date", 400

    try:
        upload_date_dt = validate_date_string(upload_date)
        if not upload_date_dt:
            print(f"Viewer error: Invalid upload_date format for batch_id '{batch_id}' - {datetime.datetime.now()}")
            return "Invalid upload_date format. Expected YYYY-MM-DD HH:MM:SS", 400

        formatted_upload_date = upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT image_id
                FROM image_table
                WHERE batch_id = %s AND date_uploaded = %s
            """, (batch_id, formatted_upload_date))
            valid_image_ids = [row['image_id'] for row in cursor.fetchall()]

            cursor.execute("""
                SELECT image_id, status
                FROM qc_table
                WHERE batch_id = %s
            """, (batch_id,))
            qc_status = {row['image_id']: row['status'] for row in cursor.fetchall()}
        conn.close()

        folder_path = batch_id.replace("_", "/") + "/"
        print(f"Viewer: Fetching images for folder_path '{folder_path}' - {datetime.datetime.now()}")
        s3_filenames = get_image_list_from_s3(folder_path)
        print(f"Viewer: Found {len(s3_filenames)} filenames from S3: {s3_filenames} - {datetime.datetime.now()}")

        filtered_filenames = [
            filename for filename in s3_filenames
            if os.path.basename(filename) in valid_image_ids
        ]
        print(f"Viewer: Filtered to {len(filtered_filenames)} filenames matching image_table: {filtered_filenames} - {datetime.datetime.now()}")

        return render_template(
            "image_viewer.html",
            batch_id=batch_id,
            image_filenames=filtered_filenames,
            qc_status=qc_status
        )

    except Exception as e:
        print(f"Viewer error for batch_id '{batch_id}': {str(e)} - {datetime.datetime.now()}")
        return "Internal Server Error", 500

@qc_bp.route('/update_qc_status', methods=['POST'])
@no_cache
@api_login_required
def update_qc_status():
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

@qc_bp.route('/update_image_status', methods=['POST'])
@no_cache
def update_image_status():
    if not request.is_json:
        return jsonify({'error': 'Request must be JSON'}), 400
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'Invalid JSON data'}), 400
            
        batch_id = data.get('batch_id')
        image_id = data.get('image_id')
        status = data.get('status')

        print(f"Received data: batch_id={batch_id}, image_id={image_id}, status={status}")

        if not batch_id or not image_id or not status:
            return jsonify({'error': 'Missing required fields (batch_id, image_id, status)'}), 400

        if status.lower() not in ['accepted', 'rejected']:
            return jsonify({'error': 'Invalid status value'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            check_query = """
                SELECT batch_id FROM qc_table 
                WHERE batch_id = %s AND image_id = %s
            """
            cursor.execute(check_query, (batch_id, image_id))
            record = cursor.fetchone()

            print(f"Existing record check: {record}")

            if record:
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
                return jsonify({'error': 'No record found for the given batch_id and image_id'}), 404

            conn.commit()
            
            return jsonify({'message': f'Image status updated to {status.lower()} successfully'}), 200
            
        except Exception as e:
            conn.rollback()
            print(f"Database operation error: {str(e)}")
            return jsonify({'error': f'Database operation error: {str(e)}'}), 500
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"Unexpected error in update_image_status: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'An unexpected error occurred: {str(e)}'}), 500

@qc_bp.route('/get_image_url', methods=['GET'])
@no_cache
@api_login_required
def get_image_url():
    try:
        batch_id = request.args.get('batch_id')
        image_id = request.args.get('image_id')
        
        if not batch_id or not image_id:
            print(f"Missing batch_id or image_id: batch_id={batch_id}, image_id={image_id} - {dt.now()}")
            return jsonify({"error": "Missing batch_id or image_id"}), 400
        
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
                ExpiresIn=300
            )
            print(f"Generated presigned URL for {s3_path}: {dt.now()}")
            return jsonify({"url": presigned_url})
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"S3 ClientError for {s3_path}: {error_code} - {error_message} - {dt.now()}")
            
            if error_code == 'NoSuchKey':
                try:
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