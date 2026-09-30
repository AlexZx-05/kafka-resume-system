import psycopg

DATABASE_URL = "postgresql://resume_user:resume_password@localhost:5432/resume_db"


def get_connection():
    return psycopg.connect(DATABASE_URL)