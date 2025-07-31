import datetime
from .database import get_db_connection

def format_datetime(dt):
    """Format datetime object to string."""
    if isinstance(dt, datetime.datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return dt

def validate_date_string(date_string, expected_format='%Y-%m-%d %H:%M:%S'):
    """Validate and parse date string."""
    try:
        return datetime.datetime.strptime(date_string, expected_format)
    except ValueError:
        return None

def get_vendor_mapping():
    """Get mapping of vendor IDs to names."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT unique_userid, name FROM user_table")
            return {row["unique_userid"]: row["name"] for row in cursor.fetchall()}
    finally:
        conn.close()

def get_qc_users():
    """Get list of QC users."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT unique_userid, name FROM user_table WHERE user_role = 'qc'")
            return cursor.fetchall()
    finally:
        conn.close()

def update_allocation_table():
    """Update allocation table status based on QC progress."""
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