from flask import Blueprint, render_template, session, redirect, url_for, jsonify, request, send_file
from functools import wraps
import pymysql
import os
from dotenv import load_dotenv
from datetime import datetime, date
import logging
import pandas as pd
from io import BytesIO
from s3_upload import s3_client, SPACES_NAME
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Create a Blueprint for the manager
manager_bp = Blueprint('manager', __name__)

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

# Decorator to ensure user is a manager
def manager_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session['user']['role'] != 'manager':
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@manager_bp.route('/manager_dashboard')
@manager_required
def manager_dashboard():
    """Renders the manager's homepage."""
    return render_template('/manager/index_manager.html', username=session['user']['username'])

@manager_bp.route('/api/manager/filters')
@manager_required
def get_filters():
    """API endpoint to get data for search filters."""
    logger.info("Received request for /api/manager/filters")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get vendors
            cursor.execute("SELECT unique_userid, name FROM user_table WHERE user_role = 'vendor'")
            vendors = cursor.fetchall()

            # Get locations
            cursor.execute("SELECT DISTINCT location FROM pandas_upload_table WHERE location IS NOT NULL AND location != ''")
            locations_data = cursor.fetchall()
            locations = [row['location'] for row in locations_data]

            # Get pandas names
            cursor.execute("SELECT DISTINCT pandas_name FROM pandas_upload_table WHERE pandas_name IS NOT NULL AND pandas_name != ''")
            pandas_names_data = cursor.fetchall()
            pandas_names = [row['pandas_name'] for row in pandas_names_data]

        logger.info(f"Found {len(vendors)} vendors, {len(locations)} locations, and {len(pandas_names)} pandas names.")
        return jsonify({'success': True, 'vendors': vendors, 'locations': locations, 'pandas_names': pandas_names})
    except Exception as e:
        logger.error(f"Error fetching filters: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Could not fetch filter data'}), 500
    finally:
        if conn:
            conn.close()

# -------------------- DYNAMIC FILTER ENDPOINTS --------------------
@manager_bp.route('/api/manager/vendor/<int:vendor_id>/locations')
@manager_required
def get_vendor_locations(vendor_id):
    """Return distinct locations assigned to the given vendor."""
    logger.info("Request: vendor locations", extra={"vendor_id": vendor_id})
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT location
                FROM vendor_allocation_table
                WHERE unique_userid = %s AND location IS NOT NULL AND location != ''
                """,
                (vendor_id,)
            )
            locations = [row['location'] for row in cursor.fetchall()]
        return jsonify({"success": True, "locations": locations})
    except Exception as e:
        logger.error(f"Error fetching locations for vendor {vendor_id}: {e}", exc_info=True)
        return jsonify({"success": False, "error": "Could not fetch locations"}), 500
    finally:
        if conn:
            conn.close()

@manager_bp.route('/api/manager/vendor/<int:vendor_id>/location/<string:location>/pandas')
@manager_required
def get_vendor_location_pandas(vendor_id, location):
    """Return distinct panda names for the given vendor and location."""
    logger.info("Request: vendor location pandas", extra={"vendor_id": vendor_id, "location": location})
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT panda_name
                FROM vendor_allocation_table
                WHERE unique_userid = %s AND location = %s AND panda_name IS NOT NULL AND panda_name != ''
                """,
                (vendor_id, location)
            )
            pandas_names = [row['panda_name'] for row in cursor.fetchall()]
        return jsonify({"success": True, "pandas_names": pandas_names})
    except Exception as e:
        logger.error(f"Error fetching pandas names for vendor {vendor_id} at location {location}: {e}", exc_info=True)
        return jsonify({"success": False, "error": "Could not fetch pandas names"}), 500
    finally:
        if conn:
            conn.close()
# -------------------------------------------------------------------

@manager_bp.route('/manager_upload_history')
@manager_required
def manager_upload_history():
    """Renders the manager's upload history page."""
    return render_template('manager/upload_history_manager.html', username=session['user']['username'])

@manager_bp.route('/api/manager/upload_history/search', methods=['POST'])
@manager_required
def search_upload_history():
    """API endpoint to search for vendor upload history based on filters."""
    data = request.json
    logger.info(f"Received upload history search request with data: {data}")
    vendor_id = data.get('vendor_id')
    location = data.get('location')
    pandas_name = data.get('pandas_name')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    batch_id = data.get('batch_id')
    page = data.get('page', 1)
    per_page = 12  # Records per page

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            base_sql = """
                FROM pandas_upload_table p
                JOIN user_table u ON p.unique_userid = u.unique_userid
            """
            
            conditions = []
            params = []

            conditions.append("u.user_role = 'vendor'")

            if vendor_id:
                conditions.append("u.unique_userid = %s")
                params.append(vendor_id)
            
            if location:
                conditions.append("p.location = %s")
                params.append(location)

            if pandas_name:
                conditions.append("p.pandas_name = %s")
                params.append(pandas_name)

            if batch_id:
                conditions.append("p.batch_id = %s")
                params.append(batch_id)

            if start_date and end_date:
                conditions.append("p.upload_date BETWEEN %s AND %s")
                params.extend([start_date, end_date])

            # Fetch paginated data along with total count in one query
            select_columns = "u.name AS vendor_name, p.upload_date, p.batch_id, p.location, p.pandas_name, p.bahi_name, p.image_count"
            data_query = f"SELECT SQL_CALC_FOUND_ROWS {select_columns} {base_sql}"
            if conditions:
                data_query += " WHERE " + " AND ".join(conditions)
            offset = (page - 1) * per_page
            data_query += f" ORDER BY p.upload_date DESC LIMIT {per_page} OFFSET {offset}"

            logger.info(f"Executing SQL: {data_query} with params: {params}")
            cursor.execute(data_query, tuple(params))
            results = cursor.fetchall()

            # Get total count and total images in a second lightweight query
            cursor.execute("SELECT FOUND_ROWS() as total")
            total_records = cursor.fetchone()['total'] or 0
            total_pages = (total_records + per_page - 1) // per_page

            # Total images (optional) – run only if first page to reduce overhead
            total_images = None
            if page == 1:
                img_query = f"SELECT SUM(p.image_count) as total_images {base_sql}"
                if conditions:
                    img_query += " WHERE " + " AND ".join(conditions)
                cursor.execute(img_query, tuple(params))
                total_images = cursor.fetchone()['total_images'] or 0

            logger.info(f"Executing SQL: {data_query} with params: {params}")
            cursor.execute(data_query, tuple(params))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} results for page {page}.")

            for row in results:
                if isinstance(row.get('upload_date'), (date, datetime)):
                    row['upload_date'] = row['upload_date'].isoformat()

            return jsonify({
                'success': True, 
                'data': results,
                'total_pages': total_pages,
                'current_page': page,
                'total_records': total_records,
                'total_images': total_images
            })

    except Exception as e:
        logger.error(f"Error searching upload history: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'An internal error occurred: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()

@manager_bp.route('/api/manager/upload_history/export', methods=['POST'])
@manager_required
def export_upload_history():
    """Export filtered upload history to XLSX file."""
    data = request.json
    vendor_id = data.get('vendor_id')
    location = data.get('location')
    pandas_name = data.get('pandas_name')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    batch_id = data.get('batch_id')

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            base_sql = """
                FROM pandas_upload_table p
                JOIN user_table u ON p.unique_userid = u.unique_userid
            """
            conditions = ["u.user_role = 'vendor'"]
            params = []

            if vendor_id:
                conditions.append("u.unique_userid = %s")
                params.append(vendor_id)
            if location:
                conditions.append("p.location = %s")
                params.append(location)
            if pandas_name:
                conditions.append("p.pandas_name = %s")
                params.append(pandas_name)
            if batch_id:
                conditions.append("p.batch_id = %s")
                params.append(batch_id)
            if start_date and end_date:
                conditions.append("p.upload_date BETWEEN %s AND %s")
                params.extend([start_date, end_date])

            data_query = f"""SELECT u.name AS vendor_name, p.upload_date, p.batch_id, p.location, p.pandas_name, p.bahi_name, p.image_count {base_sql}"""
            if conditions:
                data_query += " WHERE " + " AND ".join(conditions)
            data_query += " ORDER BY p.upload_date DESC"

            logger.info(f"Export SQL: {data_query} with params {params}")
            cursor.execute(data_query, tuple(params))
            results = cursor.fetchall()

            # Convert dates to ISO format for DataFrame
            for row in results:
                if isinstance(row.get('upload_date'), (date, datetime)):
                    row['upload_date'] = row['upload_date'].isoformat()

            if not results:
                return jsonify({'success': False, 'error': 'No data to export for the selected filters.'}), 400

            df = pd.DataFrame(results)
            output = BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, download_name='upload_history.xlsx', as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        logger.error(f"Error exporting upload history: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Could not export upload history.'}), 500
    finally:
        if conn:
            conn.close()

@manager_bp.route('/api/manager/search', methods=['POST'])
@manager_required
def search_vendor_data():
    """API endpoint to search for vendor data based on filters."""
    data = request.json
    logger.info(f"Received search request with data: {data}")
    vendor_id = data.get('vendor_id')
    location = data.get('location')
    pandas_name = data.get('pandas_name')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    batch_id = data.get('batch_id')
    page = data.get('page', 1)
    per_page = 12  # Records per page

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Base query without QC join for speed
            base_sql = (
                "FROM pandas_upload_table p "
                "JOIN user_table u ON p.unique_userid = u.unique_userid "
            )
            
            conditions = []
            params = []

            conditions.append("u.user_role = 'vendor'")

            if vendor_id:
                conditions.append("u.unique_userid = %s")
                params.append(vendor_id)
            
            if location:
                conditions.append("p.location = %s")
                params.append(location)

            if pandas_name:
                conditions.append("p.pandas_name = %s")
                params.append(pandas_name)

            if batch_id:
                conditions.append("p.batch_id = %s")
                params.append(batch_id)

            if start_date and end_date:
                conditions.append("p.upload_date BETWEEN %s AND %s")
                params.extend([start_date, end_date])

            # Count total records for pagination
            count_query = f"SELECT COUNT(*) as total {base_sql}"
            if conditions:
                count_query += " WHERE " + " AND ".join(conditions)
            cursor.execute(count_query, tuple(params))
            count_result = cursor.fetchone()
            total_records = count_result['total'] or 0
            total_pages = (total_records + per_page - 1) // per_page

            # Fetch paginated data (step 1 – batches only)
            data_query = (
                "SELECT u.name AS vendor_name, "
                "p.upload_date, p.batch_id, p.location, p.pandas_name, p.bahi_name, p.image_count, "
                "COALESCE((SELECT approved_count FROM allocation_table a WHERE a.batch_id = p.batch_id AND a.upload_date = p.upload_date LIMIT 1), 0) AS approved_count, "
                "COALESCE((SELECT rejected_count FROM allocation_table a WHERE a.batch_id = p.batch_id AND a.upload_date = p.upload_date LIMIT 1), 0) AS rejected_count "
                f"{base_sql}"
            )
            if conditions:
                data_query += " WHERE " + " AND ".join(conditions)
            
            offset = (page - 1) * per_page
            data_query += f" ORDER BY p.upload_date DESC LIMIT {per_page} OFFSET {offset}"

            logger.info(f"Executing SQL: {data_query} with params: {params}")
            cursor.execute(data_query, tuple(params))
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} results for page {page}.")

            for row in results:
                if isinstance(row.get('upload_date'), (date, datetime)):
                    row['upload_date'] = row['upload_date'].isoformat()

            return jsonify({
                'success': True, 
                'data': results,
                'total_pages': total_pages,
                'current_page': page,
                'total_records': total_records
            })

    except Exception as e:
        logger.error(f"Error searching data: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'An internal error occurred: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()

# -------------------- BATCH DETAILS ENDPOINTS --------------------

@manager_bp.route('/api/manager/batch/<string:batch_id>/details')
@manager_required
def get_batch_details(batch_id):
    """Get detailed information about a specific batch."""
    logger.info(f"Fetching batch details for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get batch information with vendor details
            cursor.execute("""
                SELECT p.batch_id, p.unique_userid, p.upload_date, p.location, 
                       p.pandas_name, p.bahi_name, p.record_type, p.image_count,
                       u.name AS vendor_name, u.email AS vendor_email
                FROM pandas_upload_table p
                JOIN user_table u ON p.unique_userid = u.unique_userid
                WHERE p.batch_id = %s
                LIMIT 1
            """, (batch_id,))
            
            batch_data = cursor.fetchone()
            if not batch_data:
                logger.warning(f"Batch not found: {batch_id}")
                return jsonify({'success': False, 'error': 'Batch not found'}), 404
            
            # Get actual image count from image_table for accuracy
            cursor.execute("""
                SELECT COUNT(*) as actual_image_count
                FROM image_table
                WHERE batch_id = %s
            """, (batch_id,))
            
            actual_count_result = cursor.fetchone()
            actual_image_count = actual_count_result['actual_image_count'] if actual_count_result else 0
            
            # Get QC status summary
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_images,
                    SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) as accepted_count,
                    SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected_count,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_count
                FROM qc_table 
                WHERE batch_id = %s
            """, (batch_id,))
            
            qc_summary = cursor.fetchone()
            
            # Determine overall status
            if qc_summary and qc_summary['total_images'] > 0:
                if qc_summary['pending_count'] > 0:
                    status = "In Progress"
                elif qc_summary['rejected_count'] > 0:
                    status = "Partially Approved"
                else:
                    status = "Approved"
            else:
                status = "Pending QC"
            
            # Format the response
            batch_info = {
                'batch_id': batch_data['batch_id'],
                'vendor_name': batch_data['vendor_name'],
                'location': batch_data['location'],
                'pandas_name': batch_data['pandas_name'],
                'bahi_name': batch_data['bahi_name'],
                'upload_date': batch_data['upload_date'].isoformat() if batch_data['upload_date'] else None,
                'uploaded_by': batch_data['vendor_email'],
                'image_count': actual_image_count,  # Use actual count from image_table
                'stored_count': batch_data['image_count'],  # Keep stored count for reference
                'status': status,
                'qc_summary': {
                    'total': qc_summary['total_images'] if qc_summary else 0,
                    'accepted': qc_summary['accepted_count'] if qc_summary else 0,
                    'rejected': qc_summary['rejected_count'] if qc_summary else 0,
                    'pending': qc_summary['pending_count'] if qc_summary else 0
                }
            }
            
            logger.info(f"Successfully retrieved batch details for: {batch_id}")
            return jsonify({'success': True, 'batch': batch_info})
            
    except Exception as e:
        logger.error(f"Error fetching batch details: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Failed to fetch batch details'}), 500
    finally:
        if conn:
            conn.close()

@manager_bp.route('/api/manager/batch/<string:batch_id>/images')
@manager_required
def get_batch_images(batch_id):
    """Get list of images for a specific batch with thumbnail and full URLs."""
    logger.info(f"Fetching images for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get images from image_table
            cursor.execute("""
                SELECT image_id, date_uploaded
                FROM image_table
                WHERE batch_id = %s
                ORDER BY image_id
                LIMIT 100
            """, (batch_id,))
            
            image_records = cursor.fetchall()
            if not image_records:
                logger.info(f"No images found for batch: {batch_id}")
                return jsonify({'success': True, 'images': []})
            
            # Generate image URLs
            images = []
            folder_path = batch_id.replace('_', '/') + '/'
            
            for record in image_records:
                image_id = record['image_id']
                
                # Generate presigned URLs for both thumbnail and full size
                try:
                    # For thumbnails, we'll use the same URL for now
                    # In production, you might want separate thumbnail storage
                    s3_path = folder_path + image_id
                    
                    # Try primary path format first
                    try:
                        thumbnail_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': SPACES_NAME, 'Key': s3_path},
                            ExpiresIn=1800  # 30 minutes
                        )
                        full_url = thumbnail_url  # Same URL for both for now
                    except ClientError:
                        # Try alternative path format
                        alt_path = f"{batch_id}/{image_id}"
                        thumbnail_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': SPACES_NAME, 'Key': alt_path},
                            ExpiresIn=1800
                        )
                        full_url = thumbnail_url
                    
                    images.append({
                        'image_id': image_id,
                        'thumbnail_url': thumbnail_url,
                        'full_url': full_url
                    })
                    
                except ClientError as e:
                    logger.warning(f"Failed to generate URL for {image_id}: {e}")
                    # Add placeholder for failed images
                    images.append({
                        'image_id': image_id,
                        'thumbnail_url': '/static/img/image-placeholder.png',
                        'full_url': '/static/img/image-placeholder.png'
                    })
            
            logger.info(f"Successfully retrieved {len(images)} images for batch: {batch_id}")
            return jsonify({'success': True, 'images': images})
            
    except Exception as e:
        logger.error(f"Error fetching batch images: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Failed to fetch batch images'}), 500
    finally:
        if conn:
            conn.close()

@manager_bp.route('/api/manager/batch/<string:batch_id>/export', methods=['POST'])
@manager_required
def export_batch_details(batch_id):
    """Export detailed batch information to Excel."""
    logger.info(f"Exporting batch details for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get comprehensive batch and QC data
            cursor.execute("""
                SELECT 
                    p.batch_id,
                    u.name AS vendor_name,
                    p.upload_date,
                    p.location,
                    p.pandas_name,
                    p.bahi_name,
                    p.record_type,
                    p.image_count,
                    i.image_id,
                    COALESCE(q.status, 'Not Assigned') AS qc_status,
                    q.remarks,
                    q.unique_userid as qc_reviewer_id,
                    qc_user.name as qc_reviewer_name,
                    q.qc_date,
                    q.orientation_error
                FROM pandas_upload_table p
                JOIN user_table u ON p.unique_userid = u.unique_userid
                LEFT JOIN image_table i ON p.batch_id = i.batch_id
                LEFT JOIN qc_table q ON i.image_id = q.image_id AND i.batch_id = q.batch_id
                LEFT JOIN user_table qc_user ON q.unique_userid = qc_user.unique_userid
                WHERE p.batch_id = %s
                ORDER BY i.image_id
            """, (batch_id,))
            
            results = cursor.fetchall()
            if not results:
                logger.warning(f"No data found for batch export: {batch_id}")
                return jsonify({'success': False, 'error': 'Batch not found'}), 404
            
            # Create DataFrame for Excel export
            df = pd.DataFrame(results)
            
            # Format dates
            if 'upload_date' in df.columns:
                df['upload_date'] = df['upload_date'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and hasattr(x, 'strftime') else str(x)
                )
            if 'qc_date' in df.columns:
                df['qc_date'] = df['qc_date'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and hasattr(x, 'strftime') else ''
                )
            
                        # Create Excel file in memory
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Count actual images for accurate reporting
                actual_image_count = len([r for r in results if r['image_id'] is not None])
                
                # Batch summary sheet
                summary_data = {
                    'Batch ID': [results[0]['batch_id']],
                    'Vendor': [results[0]['vendor_name']],
                    'Upload Date': [results[0]['upload_date'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(results[0]['upload_date'], 'strftime') else str(results[0]['upload_date'])],
                    'Location': [results[0]['location']],
                    'Pandas Name': [results[0]['pandas_name']],
                    'Bahi Name': [results[0]['bahi_name']],
                    'Record Type': [results[0]['record_type']],
                    'Total Images': [actual_image_count],
                    'Stored Count': [results[0]['image_count']]  # Include for comparison
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Batch Summary', index=False)
                
                # Detailed QC data sheet
                qc_columns = ['image_id', 'qc_status', 'remarks', 'qc_reviewer_name', 'qc_date', 'orientation_error']
                qc_df = df[qc_columns].copy()
                qc_df.to_excel(writer, sheet_name='QC Details', index=False)
                
                # Summary statistics
                status_counts = df['qc_status'].value_counts().reset_index()
                status_counts.columns = ['Status', 'Count']
                status_counts.to_excel(writer, sheet_name='Status Summary', index=False)
            
            output.seek(0)
            
            # Create response
            filename = f"batch_{batch_id}_details.xlsx"
            logger.info(f"Successfully created Excel export for batch: {batch_id}")
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            
    except Exception as e:
        logger.error(f"Error exporting batch details: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Failed to export batch details'}), 500
    finally:
        if conn:
            conn.close()
