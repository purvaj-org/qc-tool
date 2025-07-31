from flask import Blueprint, render_template, request, jsonify, session, send_file
import datetime
import pandas as pd
from io import BytesIO
import pymysql
import os
import zipfile
import tempfile
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote
from s3_upload import s3_client, SPACES_NAME
from utils.database import get_db_connection
from utils.auth import no_cache, login_required, api_login_required, api_role_required
from utils.email import send_email, get_admin_email
from utils.helpers import format_datetime, validate_date_string

reports_bp = Blueprint('reports', __name__)

# QC Status and Completion Routes
@reports_bp.route('/qc_status_insert', methods=['POST'])
@no_cache
@api_login_required
def qc_status_insert():
    try:
        data = request.get_json()
        batch_id = data.get("batch_id")
        upload_date = data.get("upload_date")

        if not batch_id or not upload_date:
            return jsonify({"success": False, "message": "Missing batch_id or upload_date"}), 400

        upload_date_dt = validate_date_string(upload_date)
        if not upload_date_dt:
            return jsonify({"success": False, "message": "Invalid upload_date format. Expected YYYY-MM-DD HH:MM:SS"}), 400

        formatted_upload_date = upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')

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

                current_time = datetime.datetime.now()
                cursor.execute("""
                    INSERT INTO batch_table (batch_id, upload_date, status, date)
                    VALUES (%s, %s, %s, %s)
                """, (batch_id, formatted_upload_date, "Completed", current_time))

                cursor.execute("""
                    UPDATE allocation_table
                    SET status = %s
                    WHERE batch_id = %s AND upload_date = %s
                """, ("Completed", batch_id, formatted_upload_date))

                # Get user details for notification
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

# QC History and Reports
@reports_bp.route('/qc_history')
@no_cache
@login_required
def qc_history():
    return render_template('qc_history.html')

@reports_bp.route('/get_qc_history', methods=['GET'])
@no_cache
@api_role_required('qc')
def get_qc_history():
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

@reports_bp.route('/qc_report')
@no_cache
@login_required
def qc_report():
    print("Rendering qc_report.html")
    return render_template('qc_report.html')

@reports_bp.route('/get_qc_report', methods=['GET'])
@no_cache
@api_login_required
def get_qc_report():
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
            print(f"Executing query: {query}")
            cursor.execute(query)
            qc_data = cursor.fetchall()
            print(f"QC data fetched: {len(qc_data)} rows")
            
            # Format the dates and orientation_error for JSON
            for row in qc_data:
                if row['qc_date'] is not None:
                    row['qc_date'] = row['qc_date'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    row['qc_date'] = 'N/A'
                row['orientation_error'] = 'Yes' if row['orientation_error'] else 'No'
                
        conn.close()
        print("QC report data sent successfully")
        return jsonify({"data": qc_data})
        
    except Exception as e:
        print(f"Get QC report error: {str(e)}")
        return jsonify({"error": f"Failed to fetch QC report: {str(e)}"}), 500

@reports_bp.route('/get_filter_data', methods=['GET'])
@no_cache
@api_login_required
def get_filter_data():
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
            print(f"Executing user query: {user_query}")
            cursor.execute(user_query)
            users = cursor.fetchall()
            print(f"Users fetched: {len(users)} users")
            
            # Fetch distinct dates with data
            date_query = """
                SELECT DISTINCT DATE(qc_date) as qc_date
                FROM qc_table
                WHERE qc_date IS NOT NULL
                ORDER BY qc_date DESC
            """
            print(f"Executing date query: {date_query}")
            cursor.execute(date_query)
            dates = cursor.fetchall()
            print(f"Dates fetched: {len(dates)} dates")
            
            # Format dates for JSON
            formatted_dates = [row['qc_date'].strftime('%Y-%m-%d') for row in dates if row['qc_date']]
            
        conn.close()
        print("Filter data sent successfully")
        return jsonify({"users": users, "dates": formatted_dates})
        
    except Exception as e:
        print(f"Get filter data error: {str(e)}")
        return jsonify({"error": f"Failed to fetch filter data: {str(e)}"}), 500

# Download Reports
@reports_bp.route('/download_report_allocation', methods=['GET'])
@no_cache
@api_login_required
def download_report_allocation():
    batch_id = request.args.get('batch_id')
    upload_date = request.args.get('upload_date')

    if not batch_id or not upload_date:
        return jsonify({'message': 'Missing batch_id or upload_date'}), 400

    try:
        parsed_upload_date = validate_date_string(upload_date)
        if not parsed_upload_date:
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

# Debug Routes
@reports_bp.route('/debug_s3', methods=['GET'])
@no_cache
@api_login_required
def debug_s3():
    try:
        response = s3_client.list_objects_v2(Bucket=SPACES_NAME)
        objects = [obj['Key'] for obj in response.get('Contents', [])]
        print(f"Debug S3: Found {len(objects)} objects in bucket '{SPACES_NAME}' - {datetime.datetime.now()}")
        return jsonify({"objects": objects, "count": len(objects)})
    except Exception as e:
        print(f"Debug S3 error: {str(e)} - {datetime.datetime.now()}")
        return jsonify({"error": str(e), "objects": []}), 500

@reports_bp.route('/get_images')
@no_cache
@api_login_required
def get_images():
    batch_id = request.args.get('batch_id')
    upload_date = request.args.get('upload_date')

    if not batch_id:
        print(f"Get images error: Missing batch_id - {datetime.datetime.now()}")
        return jsonify({"error": "Missing batch_id"}), 400

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            if upload_date:
                upload_date_dt = validate_date_string(upload_date)
                if not upload_date_dt:
                    conn.close()
                    print(f"Get images error: Invalid upload_date format - {datetime.datetime.now()}")
                    return jsonify({"error": "Invalid upload_date format"}), 400
                formatted_upload_date = upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("""
                    SELECT image_id
                    FROM image_table
                    WHERE batch_id = %s AND date_uploaded = %s
                """, (batch_id, formatted_upload_date))
            else:
                cursor.execute("""
                    SELECT image_id
                    FROM image_table
                    WHERE batch_id = %s
                """, (batch_id,))
            valid_image_ids = [row['image_id'] for row in cursor.fetchall()]
        conn.close()

        prefix = batch_id.replace('_', '/') + '/'
        print(f"Get images: Listing objects with prefix '{prefix}' in bucket '{SPACES_NAME}' - {datetime.datetime.now()}")
        response = s3_client.list_objects_v2(Bucket=SPACES_NAME, Prefix=prefix)
        contents = response.get('Contents', [])
        images = []

        print(f"Get images: Found {len(contents)} objects for prefix '{prefix}' - {datetime.datetime.now()}")

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