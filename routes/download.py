from flask import Blueprint, render_template, request, jsonify, session, send_file
import datetime
import zipfile
import tempfile
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3_upload import s3_client, SPACES_NAME
from utils.database import get_db_connection
from utils.auth import no_cache, login_required, api_login_required
from utils.helpers import validate_date_string

download_bp = Blueprint('download', __name__)

@download_bp.route('/download_batches')
@no_cache
@login_required
def download_batches():
    return render_template('download_batches.html')

@download_bp.route('/api/download/batch-ids', methods=['GET'])
@no_cache
@api_login_required
def get_download_batch_ids():
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

@download_bp.route('/api/download/vendors', methods=['GET'])
@no_cache
@api_login_required
def get_download_vendors():
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

@download_bp.route('/api/download/search', methods=['POST'])
@no_cache
@api_login_required
def search_download_batches():
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

@download_bp.route('/api/download/batch', methods=['POST'])
@no_cache
@api_login_required
def download_batch_zip():
    try:
        data = request.get_json()
        batch_id = data.get('batch_id')
        upload_date = data.get('upload_date')
        status_filter = data.get('status', 'all')
        
        if not batch_id or not upload_date:
            return jsonify({"error": "Batch ID and upload date are required"}), 400
        
        # Parse upload date
        upload_date_dt = validate_date_string(upload_date)
        if not upload_date_dt:
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
            # Create HTTP session with connection pooling for better performance
            http_session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20,
                pool_maxsize=20,
                max_retries=3
            )
            http_session.mount('http://', adapter)
            http_session.mount('https://', adapter)
            
            def download_image(image_id):
                try:
                    # Construct S3 path
                    s3_path = batch_id.replace('_', '/') + '/' + image_id
                    
                    # Generate presigned URL
                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': SPACES_NAME, 'Key': s3_path},
                        ExpiresIn=600  # Increased timeout for parallel downloads
                    )
                    
                    # Download image from S3
                    response = http_session.get(presigned_url, timeout=15)
                    if response.status_code == 200:
                        return image_id, response.content
                    else:
                        print(f"Failed to download image {image_id}: HTTP {response.status_code}")
                        return image_id, None
                
                except Exception as e:
                    print(f"Error downloading image {image_id}: {str(e)}")
                    return image_id, None
            
            with zipfile.ZipFile(temp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Download images in parallel (max 10 concurrent downloads)
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_image = {executor.submit(download_image, image_id): image_id 
                                     for image_id in filtered_image_ids}
                    
                    for future in as_completed(future_to_image):
                        image_id, content = future.result()
                        if content:
                            zipf.writestr(image_id, content)
            
            # Send ZIP file
            filename = f"{batch_id}_{status_filter}_images_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            
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