from flask import Blueprint, render_template, request, jsonify, session
import pymysql
from utils.database import get_db_connection
from utils.auth import no_cache, login_required, api_login_required
from s3_upload import s3_client, SPACES_NAME
import os
from PIL import Image, ImageDraw, ImageFont
import io
import base64

image_viewer_bp = Blueprint('image_viewer', __name__)

@image_viewer_bp.route('/image_viewer')
@no_cache
@login_required
def image_viewer():
    """Render the image viewer page for vendors"""
    return render_template('vendor_image_viewer.html', session_id=session['user']['unique_userid'])

@image_viewer_bp.route('/get_vendor_batches', methods=['GET'])
@no_cache
@api_login_required
def get_vendor_batches():
    """Get all batch IDs for the current vendor"""
    unique_userid = session['user']['unique_userid']
    batch_filter = request.args.get('batch_filter', '').strip()
    
    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            if batch_filter:
                # Search with filter
                cursor.execute("""
                    SELECT DISTINCT batch_id, upload_date, image_count, location, pandas_name, bahi_name, record_type
                    FROM pandas_upload_table 
                    WHERE unique_userid = %s AND batch_id LIKE %s
                    ORDER BY upload_date DESC
                """, (unique_userid, f'%{batch_filter}%'))
            else:
                # Get all batches
                cursor.execute("""
                    SELECT DISTINCT batch_id, upload_date, image_count, location, pandas_name, bahi_name, record_type
                    FROM pandas_upload_table 
                    WHERE unique_userid = %s
                    ORDER BY upload_date DESC
                """, (unique_userid,))
            
            batches = cursor.fetchall()
            
            # Add QC status counts for each batch
            for batch in batches:
                batch_id = batch['batch_id']
                
                # Count accepted images
                cursor.execute("""
                    SELECT COUNT(*) AS accepted_count
                    FROM qc_table
                    WHERE batch_id = %s AND status = 'accepted'
                """, (batch_id,))
                accepted_result = cursor.fetchone()
                batch['accepted_count'] = accepted_result['accepted_count'] if accepted_result else 0
                
                # Count rejected images
                cursor.execute("""
                    SELECT COUNT(*) AS rejected_count
                    FROM qc_table
                    WHERE batch_id = %s AND status = 'rejected'
                """, (batch_id,))
                rejected_result = cursor.fetchone()
                batch['rejected_count'] = rejected_result['rejected_count'] if rejected_result else 0
                
                # Pending count
                batch['pending_count'] = batch['image_count'] - batch['accepted_count'] - batch['rejected_count']
        
        conn.close()
        return jsonify({"success": True, "batches": batches})
        
    except Exception as e:
        print(f"Get vendor batches error: {str(e)}")
        return jsonify({"success": False, "error": "Database error"}), 500

@image_viewer_bp.route('/get_batch_images', methods=['GET'])
@no_cache
@api_login_required
def get_batch_images():
    """Get images for a specific batch, filtered by status"""
    unique_userid = session['user']['unique_userid']
    batch_id = request.args.get('batch_id')
    status_filter = request.args.get('status', 'all')  # 'accepted', 'rejected', or 'all'
    
    if not batch_id:
        return jsonify({"error": "Missing batch_id"}), 400
    
    try:
        conn = get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Verify this batch belongs to the current vendor
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM pandas_upload_table 
                WHERE batch_id = %s AND unique_userid = %s
            """, (batch_id, unique_userid))
            
            if cursor.fetchone()['count'] == 0:
                return jsonify({"error": "Batch not found or access denied"}), 403
            
            # Get images based on status filter
            if status_filter == 'accepted':
                cursor.execute("""
                    SELECT i.image_id, q.status, q.remarks, q.qc_date
                    FROM image_table i
                    INNER JOIN qc_table q ON i.batch_id = q.batch_id AND i.image_id = q.image_id
                    WHERE i.batch_id = %s AND q.status = 'accepted'
                    ORDER BY q.qc_date DESC
                """, (batch_id,))
            elif status_filter == 'rejected':
                cursor.execute("""
                    SELECT i.image_id, q.status, q.remarks, q.qc_date
                    FROM image_table i
                    INNER JOIN qc_table q ON i.batch_id = q.batch_id AND i.image_id = q.image_id
                    WHERE i.batch_id = %s AND q.status = 'rejected'
                    ORDER BY q.qc_date DESC
                """, (batch_id,))
            else:  # all images
                cursor.execute("""
                    SELECT i.image_id, COALESCE(q.status, 'pending') as status, q.remarks, q.qc_date
                    FROM image_table i
                    LEFT JOIN qc_table q ON i.batch_id = q.batch_id AND i.image_id = q.image_id
                    WHERE i.batch_id = %s
                    ORDER BY i.date_uploaded DESC
                """, (batch_id,))
            
            images = cursor.fetchall()
        
        conn.close()
        
        # Generate thumbnail URLs for each image
        for image in images:
            image['thumbnail_url'] = f'/get_image_thumbnail?batch_id={batch_id}&image_id={image["image_id"]}'
        
        return jsonify({
            "success": True, 
            "images": images,
            "total": len(images)
        })
        
    except Exception as e:
        print(f"Get batch images error: {str(e)}")
        return jsonify({"success": False, "error": "Database error"}), 500

@image_viewer_bp.route('/get_image_thumbnail', methods=['GET'])
@no_cache
@api_login_required
def get_image_thumbnail():
    """Get a watermarked thumbnail of an image"""
    unique_userid = session['user']['unique_userid']
    batch_id = request.args.get('batch_id')
    image_id = request.args.get('image_id')
    
    if not batch_id or not image_id:
        return jsonify({"error": "Missing batch_id or image_id"}), 400
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Verify this batch belongs to the current vendor
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM pandas_upload_table 
                WHERE batch_id = %s AND unique_userid = %s
            """, (batch_id, unique_userid))
            
            if cursor.fetchone()['count'] == 0:
                return jsonify({"error": "Access denied"}), 403
            
            # Verify image exists in this batch
            cursor.execute("""
                SELECT image_id
                FROM image_table
                WHERE batch_id = %s AND image_id = %s
            """, (batch_id, image_id))
            
            if not cursor.fetchone():
                return jsonify({"error": "Image not found"}), 404
        
        conn.close()
        
        # Construct S3 path
        folder_path = batch_id.replace('_', '/') + '/'
        s3_path = folder_path + image_id
        
        # Get image from S3
        try:
            response = s3_client.get_object(Bucket=SPACES_NAME, Key=s3_path)
            image_data = response['Body'].read()
            
            # Create thumbnail with watermark
            watermarked_thumbnail = create_watermarked_thumbnail(image_data, batch_id, image_id)
            
            return watermarked_thumbnail, 200, {
                'Content-Type': 'image/jpeg',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            }
            
        except Exception as e:
            print(f"Error fetching image from S3: {str(e)}")
            return jsonify({"error": "Image not found in storage"}), 404
            
    except Exception as e:
        print(f"Get image thumbnail error: {str(e)}")
        return jsonify({"error": "Server error"}), 500

def create_watermarked_thumbnail(image_data, batch_id, image_id, max_size=(400, 400)):
    """Create a watermarked thumbnail from image data"""
    try:
        # Open the image
        image = Image.open(io.BytesIO(image_data))
        
        # Convert to RGB if necessary
        if image.mode in ('RGBA', 'LA', 'P'):
            image = image.convert('RGB')
        
        # Create thumbnail while maintaining aspect ratio
        image.thumbnail(max_size, Image.Resampling.LANCZOS)
        
        # Create watermark
        width, height = image.size
        
        # Create a transparent overlay
        overlay = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        # Try to use a default font, fallback to basic if not available
        try:
            font_size = max(12, min(width, height) // 20)
            font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", font_size)
        except (OSError, IOError):
            try:
                font = ImageFont.truetype("arial.ttf", font_size)
            except (OSError, IOError):
                font = ImageFont.load_default()
        
        # Watermark text
        watermark_lines = [
            "CONFIDENTIAL",
            f"Batch: {batch_id[-20:]}",  # Last 20 chars to avoid too long text
            f"File: {image_id[:15]}...",  # First 15 chars + ellipsis
            "© PURVAJ.COM"
        ]
        
        # Add single watermark in the center
        center_x = width // 2
        center_y = height // 2 - (len(watermark_lines) * 15) // 2
        
        for i, line in enumerate(watermark_lines):
            # Calculate text position (centered)
            text_bbox = draw.textbbox((0, 0), line, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            x = center_x - text_width // 2
            y = center_y + i * 15
            
            # Draw outline (black)
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    if dx != 0 or dy != 0:
                        draw.text((x + dx, y + dy), line, font=font, fill=(0, 0, 0, 100))
            
            # Draw main text (white)
            draw.text((x, y), line, font=font, fill=(255, 255, 255, 180))
        
        # Composite the watermark onto the image
        watermarked = Image.alpha_composite(image.convert('RGBA'), overlay)
        watermarked = watermarked.convert('RGB')
        
        # Save as JPEG with reduced quality
        output = io.BytesIO()
        watermarked.save(output, format='JPEG', quality=70, optimize=True)
        output.seek(0)
        
        return output.getvalue()
        
    except Exception as e:
        print(f"Error creating watermarked thumbnail: {str(e)}")
        # Return a simple error image if watermarking fails
        error_img = Image.new('RGB', (400, 300), color='lightgray')
        draw = ImageDraw.Draw(error_img)
        draw.text((50, 150), "Image not available", fill='black')
        
        output = io.BytesIO()
        error_img.save(output, format='JPEG')
        output.seek(0)
        return output.getvalue() 