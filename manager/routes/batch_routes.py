from flask import Blueprint, jsonify, request, send_file
from datetime import datetime, date
import logging
import pandas as pd
from io import BytesIO
from s3_upload import s3_client, SPACES_NAME
from botocore.exceptions import ClientError
from ..utils.database import get_db_connection
from ..utils.auth import manager_required

logger = logging.getLogger(__name__)

batch_bp = Blueprint('batch', __name__)

# -------------------- BATCH DETAILS ENDPOINTS --------------------

@batch_bp.route('/api/manager/batch/<string:batch_id>/details')
@manager_required
def get_batch_details(batch_id):
    """Get detailed information about a specific batch."""
    logger.info(f"Fetching batch details for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # here batch information with vendor details
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
            
            cursor.execute("""
                SELECT COUNT(*) as actual_image_count
                FROM image_table
                WHERE batch_id = %s
            """, (batch_id,))
            
            actual_count_result = cursor.fetchone()
            actual_image_count = actual_count_result['actual_image_count'] if actual_count_result else 0
            
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
            
            if qc_summary and qc_summary['total_images'] > 0:
                if qc_summary['pending_count'] > 0:
                    status = "In Progress"
                elif qc_summary['rejected_count'] > 0:
                    status = "Partially Approved"
                else:
                    status = "Approved"
            else:
                status = "Pending QC"
            
            batch_info = {
                'batch_id': batch_data['batch_id'],
                'vendor_name': batch_data['vendor_name'],
                'location': batch_data['location'],
                'pandas_name': batch_data['pandas_name'],
                'bahi_name': batch_data['bahi_name'],
                'upload_date': batch_data['upload_date'].isoformat() if batch_data['upload_date'] else None,
                'uploaded_by': batch_data['vendor_email'],
                'image_count': actual_image_count,
                'stored_count': batch_data['image_count'],
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

@batch_bp.route('/api/manager/batch/<string:batch_id>/images')
@manager_required
def get_batch_images(batch_id):
    """Get list of images for a specific batch with thumbnail and full URLs."""
    logger.info(f"Fetching images for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
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
            
            images = []
            folder_path = batch_id.replace('_', '/') + '/'
            
            for record in image_records:
                image_id = record['image_id']
                
                try:
                    s3_path = folder_path + image_id
                    
                    try:
                        thumbnail_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': SPACES_NAME, 'Key': s3_path},
                            ExpiresIn=1800  
                        )
                        full_url = thumbnail_url
                    except ClientError:
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

@batch_bp.route('/api/manager/batch/<string:batch_id>/export', methods=['POST'])
@manager_required
def export_batch_details(batch_id):
    """Export detailed batch information to Excel."""
    logger.info(f"Exporting batch details for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
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
            
            df = pd.DataFrame(results)
            
            if 'upload_date' in df.columns:
                df['upload_date'] = df['upload_date'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and hasattr(x, 'strftime') else str(x)
                )
            if 'qc_date' in df.columns:
                df['qc_date'] = df['qc_date'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and hasattr(x, 'strftime') else ''
                )
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                actual_image_count = len([r for r in results if r['image_id'] is not None])
                
                summary_data = {
                    'Batch ID': [results[0]['batch_id']],
                    'Vendor': [results[0]['vendor_name']],
                    'Upload Date': [results[0]['upload_date'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(results[0]['upload_date'], 'strftime') else str(results[0]['upload_date'])],
                    'Location': [results[0]['location']],
                    'Pandas Name': [results[0]['pandas_name']],
                    'Bahi Name': [results[0]['bahi_name']],
                    'Record Type': [results[0]['record_type']],
                    'Total Images': [actual_image_count],
                    'Stored Count': [results[0]['image_count']] 
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Batch Summary', index=False)
                
                qc_columns = ['image_id', 'qc_status', 'remarks', 'qc_reviewer_name', 'qc_date', 'orientation_error']
                qc_df = df[qc_columns].copy()
                qc_df.to_excel(writer, sheet_name='QC Details', index=False)
                
                status_counts = df['qc_status'].value_counts().reset_index()
                status_counts.columns = ['Status', 'Count']
                status_counts.to_excel(writer, sheet_name='Status Summary', index=False)
            
            output.seek(0)
            
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



@batch_bp.route('/api/manager/batch/<string:batch_id>/qc-details')
@manager_required
def get_batch_qc_details(batch_id):
    """Get QC details for a specific batch including Image ID, Status, Remarks, etc."""
    logger.info(f"Fetching QC details for batch_id: {batch_id}")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    i.image_id,
                    COALESCE(q.status, 'Not Assigned') AS status,
                    q.remarks,
                    q.orientation_error,
                    COALESCE(u.name, 'Not Assigned') AS reviewer,
                    q.qc_date
                FROM image_table i
                LEFT JOIN qc_table q ON i.image_id = q.image_id AND i.batch_id = q.batch_id
                LEFT JOIN user_table u ON q.unique_userid = u.unique_userid
                WHERE i.batch_id = %s
                ORDER BY i.image_id
            """, (batch_id,))
            
            qc_details = cursor.fetchall()
            
            # Format dates for JSON serialization
            for detail in qc_details:
                if detail['qc_date']:
                    detail['qc_date'] = detail['qc_date'].isoformat()
                else:
                    detail['qc_date'] = None
            
            logger.info(f"Successfully retrieved QC details for {len(qc_details)} images in batch: {batch_id}")
            return jsonify({'success': True, 'qc_details': qc_details})
            
    except Exception as e:
        logger.error(f"Error fetching QC details: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Failed to fetch QC details'}), 500
    finally:
        if conn:
            conn.close()
