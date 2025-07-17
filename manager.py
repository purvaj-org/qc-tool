from flask import Blueprint, render_template, session, redirect, url_for, jsonify, request, send_file
from functools import wraps
import pymysql
import os
from dotenv import load_dotenv
from datetime import datetime, date
import logging
import pandas as pd
from io import BytesIO

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
        host='192.168.7.97',  # Remove the port from here  
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
