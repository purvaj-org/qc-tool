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
                UPDATE allocation_table a
                LEFT JOIN (
                    SELECT 
                        i.batch_id,
                        i.date_uploaded,
                        COALESCE(SUM(CASE WHEN q.status = 'accepted' THEN 1 ELSE 0 END), 0) as approved_count,
                        COALESCE(SUM(CASE WHEN q.status = 'rejected' THEN 1 ELSE 0 END), 0) as rejected_count,
                        COALESCE(COUNT(q.image_id), 0) as total_processed
                    FROM image_table i
                    LEFT JOIN qc_table q ON i.image_id = q.image_id AND i.batch_id = q.batch_id
                    GROUP BY i.batch_id, i.date_uploaded
                ) qc_stats ON a.batch_id = qc_stats.batch_id AND a.upload_date = qc_stats.date_uploaded
                SET 
                    a.approved_count = COALESCE(qc_stats.approved_count, 0),
                    a.rejected_count = COALESCE(qc_stats.rejected_count, 0),
                    a.status = CASE 
                        WHEN a.status = 'revoked' THEN 'revoked'
                        WHEN COALESCE(qc_stats.total_processed, 0) = 0 THEN 'Pending'
                        WHEN COALESCE(qc_stats.total_processed, 0) < a.image_count THEN 'In Progress'
                        WHEN COALESCE(qc_stats.total_processed, 0) = a.image_count THEN 'Completed'
                        ELSE 'Error'
                    END
                WHERE a.status != 'revoked'
            """)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating allocation_table: {e}")
    finally:
        conn.close() 