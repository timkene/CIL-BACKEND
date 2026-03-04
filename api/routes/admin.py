"""
Admin Module API Routes
========================

Provides user management functionality including:
- User registration
- User listing
- User termination
- Password management
- Role and permission management
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, EmailStr
import logging
import duckdb
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import tempfile

logger = logging.getLogger(__name__)
router = APIRouter()

# Database path and schema (same as core.database)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'ai_driven_data.duckdb')
SCHEMA = 'AI DRIVEN DATA'

# Use local DuckDB unless USE_LOCAL_DB is explicitly false (aligns with core.database)
USE_LOCAL_DB = os.getenv('USE_LOCAL_DB', 'true').lower() in ('true', '1', 'yes')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN') or os.getenv('MOTHERDUCK_PAT')
MOTHERDUCK_DB = 'ai_driven_data'
if not USE_LOCAL_DB and not MOTHERDUCK_TOKEN:
    raise ValueError("MOTHERDUCK_TOKEN is required when USE_LOCAL_DB is false. Set it in .env or set USE_LOCAL_DB=true for local database.")

# Request/Response models
class UserCreate(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    department: Optional[str] = None

class UserUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None
    department: Optional[str] = None
    status: Optional[str] = None  # 'Active' or 'Terminated'

class PasswordChange(BaseModel):
    email: EmailStr
    new_password: str

class DepartmentPermissionUpdate(BaseModel):
    department: str
    modules: List[str]  # List of module IDs

class StaffClientAllocationCreate(BaseModel):
    user_id: int
    groupnames: List[str]  # List of client groupnames to allocate

class StaffClientAllocationUpdate(BaseModel):
    user_id: int
    groupnames: List[str]  # List of client groupnames to allocate

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

def get_db_connection(read_only=False):
    """Get DB connection: local DuckDB when USE_LOCAL_DB=true, else MotherDuck (same as core.database)."""
    try:
        if USE_LOCAL_DB:
            conn = duckdb.connect(DB_PATH, read_only=read_only)
            conn.execute(f'USE "{SCHEMA}"')
            return conn
        conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
        conn.execute(f"USE {MOTHERDUCK_DB}")
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")

def init_users_table():
    """Initialize the USERS table if it doesn't exist"""
    try:
        conn = get_db_connection(read_only=False)
        
        # Create schema if not exists
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        
        # Create table if not exists
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."USERS" (
                user_id INTEGER PRIMARY KEY,
                first_name VARCHAR NOT NULL,
                last_name VARCHAR NOT NULL,
                email VARCHAR NOT NULL UNIQUE,
                department VARCHAR,
                password VARCHAR NOT NULL,
                status VARCHAR DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.close()
        logger.info("✅ USERS table initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize USERS table: {e}")
        raise

def init_department_permissions_table():
    """Initialize the DEPARTMENT_PERMISSIONS table if it doesn't exist"""
    try:
        conn = get_db_connection(read_only=False)
        
        # Create schema if not exists
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        
        # Create table if not exists
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."DEPARTMENT_PERMISSIONS" (
                permission_id INTEGER PRIMARY KEY,
                department VARCHAR NOT NULL UNIQUE,
                modules VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.close()
        logger.info("✅ DEPARTMENT_PERMISSIONS table initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize DEPARTMENT_PERMISSIONS table: {e}")
        raise

def init_staff_client_allocations_table():
    """Initialize the STAFF_CLIENT_ALLOCATIONS table if it doesn't exist"""
    try:
        conn = get_db_connection(read_only=False)
        
        # Create schema if not exists
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        
        # Create table if not exists
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS" (
                allocation_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                groupname VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, groupname)
            )
        ''')
        
        conn.close()
        logger.info("✅ STAFF_CLIENT_ALLOCATIONS table initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize STAFF_CLIENT_ALLOCATIONS table: {e}")
        raise

def generate_password(first_name: str, last_name: str) -> str:
    """Generate password in format: Password-{first_name_length}-{last_name_length}"""
    first_len = len(first_name)
    last_len = len(last_name)
    return f"Password-{first_len}-{last_len}"

@router.get("/users")
async def get_users(
    status: Optional[str] = Query(None, description="Filter by status: Active or Terminated")
):
    """
    Get list of all users, optionally filtered by status.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=True)
        
        query = f'SELECT * FROM "{SCHEMA}"."USERS"'
        params = []
        
        if status:
            query += ' WHERE status = ?'
            params.append(status)
        
        query += ' ORDER BY created_at DESC'
        
        df = conn.execute(query, params).fetchdf()
        conn.close()
        
        users = df.to_dict('records') if not df.empty else []
        
        # Convert timestamps to strings for JSON serialization
        for user in users:
            if 'created_at' in user and user['created_at']:
                user['created_at'] = user['created_at'].isoformat() if hasattr(user['created_at'], 'isoformat') else str(user['created_at'])
            if 'updated_at' in user and user['updated_at']:
                user['updated_at'] = user['updated_at'].isoformat() if hasattr(user['updated_at'], 'isoformat') else str(user['updated_at'])
        
        return {
            'success': True,
            'data': users,
            'count': len(users)
        }
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/users")
async def create_user(user_data: UserCreate):
    """
    Create a new user with auto-generated password.
    Password format: Password-{first_name_length}-{last_name_length}
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Check if email already exists
        existing = conn.execute(
            f'SELECT email FROM "{SCHEMA}"."USERS" WHERE email = ?',
            [user_data.email]
        ).fetchone()
        
        if existing:
            conn.close()
            raise HTTPException(status_code=400, detail=f"User with email {user_data.email} already exists")
        
        # Generate password
        password = generate_password(user_data.first_name, user_data.last_name)
        
        # Get next user_id
        max_id_result = conn.execute(f'SELECT COALESCE(MAX(user_id), 0) FROM "{SCHEMA}"."USERS"').fetchone()
        next_id = (max_id_result[0] if max_id_result else 0) + 1
        
        # Insert new user
        conn.execute(f'''
            INSERT INTO "{SCHEMA}"."USERS" 
            (user_id, first_name, last_name, email, department, password, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'Active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', [
            next_id,
            user_data.first_name,
            user_data.last_name,
            user_data.email,
            user_data.department,
            password
        ])
        
        conn.close()
        
        return {
            'success': True,
            'message': 'User created successfully',
            'data': {
                'user_id': next_id,
                'first_name': user_data.first_name,
                'last_name': user_data.last_name,
                'email': user_data.email,
                'department': user_data.department,
                'password': password,  # Return password so admin can see it
                'status': 'Active'
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/{user_id}/terminate")
async def terminate_user(user_id: int):
    """
    Terminate a user (set status to 'Terminated').
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Check if user exists
        user = conn.execute(
            f'SELECT user_id, email, status FROM "{SCHEMA}"."USERS" WHERE user_id = ?',
            [user_id]
        ).fetchone()
        
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        
        if user[2] == 'Terminated':
            conn.close()
            raise HTTPException(status_code=400, detail="User is already terminated")
        
        # Update status
        conn.execute(f'''
            UPDATE "{SCHEMA}"."USERS" 
            SET status = 'Terminated', updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', [user_id])
        
        conn.close()
        
        return {
            'success': True,
            'message': f'User {user[1]} has been terminated'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error terminating user: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/{user_id}/reactivate")
async def reactivate_user(user_id: int):
    """
    Reactivate a terminated user (set status to 'Active').
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Check if user exists
        user = conn.execute(
            f'SELECT user_id, email, status FROM "{SCHEMA}"."USERS" WHERE user_id = ?',
            [user_id]
        ).fetchone()
        
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        
        if user[2] == 'Active':
            conn.close()
            raise HTTPException(status_code=400, detail="User is already active")
        
        # Update status
        conn.execute(f'''
            UPDATE "{SCHEMA}"."USERS" 
            SET status = 'Active', updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', [user_id])
        
        conn.close()
        
        return {
            'success': True,
            'message': f'User {user[1]} has been reactivated'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reactivating user: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/{user_id}")
async def update_user(user_id: int, user_data: UserUpdate):
    """
    Update user information.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Check if user exists
        user = conn.execute(
            f'SELECT user_id FROM "{SCHEMA}"."USERS" WHERE user_id = ?',
            [user_id]
        ).fetchone()
        
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        
        # Build update query dynamically
        updates = []
        params = []
        
        if user_data.first_name is not None:
            updates.append('first_name = ?')
            params.append(user_data.first_name)
        
        if user_data.last_name is not None:
            updates.append('last_name = ?')
            params.append(user_data.last_name)
        
        if user_data.email is not None:
            # Check if email is already taken by another user
            existing = conn.execute(
                f'SELECT user_id FROM "{SCHEMA}"."USERS" WHERE email = ? AND user_id != ?',
                [user_data.email, user_id]
            ).fetchone()
            
            if existing:
                conn.close()
                raise HTTPException(status_code=400, detail=f"Email {user_data.email} is already in use")
            
            updates.append('email = ?')
            params.append(user_data.email)
        
        if user_data.department is not None:
            updates.append('department = ?')
            params.append(user_data.department)
        
        if user_data.status is not None:
            if user_data.status not in ['Active', 'Terminated']:
                conn.close()
                raise HTTPException(status_code=400, detail="Status must be 'Active' or 'Terminated'")
            updates.append('status = ?')
            params.append(user_data.status)
        
        if not updates:
            conn.close()
            raise HTTPException(status_code=400, detail="No fields to update")
        
        updates.append('updated_at = CURRENT_TIMESTAMP')
        params.append(user_id)
        
        query = f'''
            UPDATE "{SCHEMA}"."USERS" 
            SET {', '.join(updates)}
            WHERE user_id = ?
        '''
        
        conn.execute(query, params)
        conn.close()
        
        return {
            'success': True,
            'message': 'User updated successfully'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/change-password")
async def change_password(password_data: PasswordChange):
    """
    Change a user's password.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Check if user exists
        user = conn.execute(
            f'SELECT user_id, email FROM "{SCHEMA}"."USERS" WHERE email = ?',
            [password_data.email]
        ).fetchone()
        
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail=f"User with email {password_data.email} not found")
        
        # Update password
        conn.execute(f'''
            UPDATE "{SCHEMA}"."USERS" 
            SET password = ?, updated_at = CURRENT_TIMESTAMP
            WHERE email = ?
        ''', [password_data.new_password, password_data.email])
        
        conn.close()
        
        return {
            'success': True,
            'message': 'Password changed successfully'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error changing password: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/users/{user_id}")
async def delete_user(user_id: int):
    """
    Permanently delete a user from the database.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Check if user exists
        user = conn.execute(
            f'SELECT user_id, email FROM "{SCHEMA}"."USERS" WHERE user_id = ?',
            [user_id]
        ).fetchone()
        
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        
        # Delete user
        conn.execute(f'DELETE FROM "{SCHEMA}"."USERS" WHERE user_id = ?', [user_id])
        
        conn.close()
        
        return {
            'success': True,
            'message': f'User {user[1]} has been permanently deleted'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# Department Permissions Endpoints

@router.get("/department-permissions")
async def get_department_permissions():
    """
    Get all department permissions.
    Returns a list of departments with their assigned modules.
    """
    try:
        init_department_permissions_table()
        conn = get_db_connection(read_only=True)
        
        df = conn.execute(f'''
            SELECT permission_id, department, modules, created_at, updated_at
            FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
            ORDER BY department
        ''').fetchdf()
        
        conn.close()
        
        permissions = []
        if not df.empty:
            for _, row in df.iterrows():
                modules_list = [m.strip() for m in str(row['modules']).split(',') if m.strip()]
                permissions.append({
                    'permission_id': int(row['permission_id']),
                    'department': str(row['department']),
                    'modules': modules_list,
                    'created_at': row['created_at'].isoformat() if hasattr(row['created_at'], 'isoformat') else str(row['created_at']),
                    'updated_at': row['updated_at'].isoformat() if hasattr(row['updated_at'], 'isoformat') else str(row['updated_at'])
                })
        
        return {
            'success': True,
            'data': permissions,
            'count': len(permissions)
        }
    except Exception as e:
        logger.error(f"Error getting department permissions: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/department-permissions/{department}")
async def get_department_permission(department: str):
    """
    Get permissions for a specific department.
    """
    try:
        init_department_permissions_table()
        conn = get_db_connection(read_only=True)
        
        result = conn.execute(f'''
            SELECT permission_id, department, modules, created_at, updated_at
            FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
            WHERE department = ?
        ''', [department]).fetchone()
        
        conn.close()
        
        if not result:
            return {
                'success': True,
                'data': {
                    'department': department,
                    'modules': []
                }
            }
        
        modules_list = [m.strip() for m in str(result[2]).split(',') if m.strip()]
        
        return {
            'success': True,
            'data': {
                'permission_id': int(result[0]),
                'department': str(result[1]),
                'modules': modules_list,
                'created_at': result[3].isoformat() if hasattr(result[3], 'isoformat') else str(result[3]),
                'updated_at': result[4].isoformat() if hasattr(result[4], 'isoformat') else str(result[4])
            }
        }
    except Exception as e:
        logger.error(f"Error getting department permission: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/department-permissions")
async def update_department_permission(permission_data: DepartmentPermissionUpdate):
    """
    Update or create department permissions.
    If department exists, updates modules. If not, creates new entry.
    """
    try:
        init_department_permissions_table()
        conn = get_db_connection(read_only=False)
        
        # Check if department already exists
        existing = conn.execute(f'''
            SELECT permission_id FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
            WHERE department = ?
        ''', [permission_data.department]).fetchone()
        
        # Convert modules list to comma-separated string
        modules_str = ','.join(permission_data.modules)
        
        if existing:
            # Update existing
            conn.execute(f'''
                UPDATE "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                SET modules = ?, updated_at = CURRENT_TIMESTAMP
                WHERE department = ?
            ''', [modules_str, permission_data.department])
            action = 'updated'
        else:
            # Create new
            max_id_result = conn.execute(f'SELECT COALESCE(MAX(permission_id), 0) FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"').fetchone()
            next_id = (max_id_result[0] if max_id_result else 0) + 1
            
            conn.execute(f'''
                INSERT INTO "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                (permission_id, department, modules, created_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', [next_id, permission_data.department, modules_str])
            action = 'created'
        
        conn.close()
        
        return {
            'success': True,
            'message': f'Department permissions {action} successfully',
            'data': {
                'department': permission_data.department,
                'modules': permission_data.modules
            }
        }
    except Exception as e:
        logger.error(f"Error updating department permission: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/department-permissions/{department}")
async def delete_department_permission(department: str):
    """
    Delete permissions for a department.
    """
    try:
        init_department_permissions_table()
        conn = get_db_connection(read_only=False)
        
        # Check if exists
        existing = conn.execute(f'''
            SELECT department FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
            WHERE department = ?
        ''', [department]).fetchone()
        
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Permissions for department '{department}' not found")
        
        # Delete
        conn.execute(f'''
            DELETE FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
            WHERE department = ?
        ''', [department])
        
        conn.close()
        
        return {
            'success': True,
            'message': f'Permissions for department {department} deleted successfully'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting department permission: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/available-modules")
async def get_available_modules():
    """
    Get list of all available modules in the system.
    """
    modules = [
        {'id': 'mlr', 'label': 'MLR Analysis', 'icon': '📊'},
        {'id': 'client', 'label': 'Client Analysis', 'icon': '🏢'},
        {'id': 'enrollee', 'label': 'Enrollee Management', 'icon': '👥'},
        {'id': 'providers', 'label': 'Providers', 'icon': '🏥'},
        {'id': 'hospitalband', 'label': 'Hospital Band Analysis', 'icon': '📊'},
        {'id': 'finance', 'label': 'Finances', 'icon': '💰'},
        {'id': 'paclaims', 'label': 'PA & Claims', 'icon': '📋'},
        {'id': 'critical', 'label': 'Critical Alert', 'icon': '🚨'},
        {'id': 'admin', 'label': 'Admin', 'icon': '⚙️'}
    ]
    
    return {
        'success': True,
        'data': modules
    }

@router.get("/departments")
async def get_departments():
    """
    Get list of all unique departments from USERS table.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=True)
        
        df = conn.execute(f'''
            SELECT DISTINCT department
            FROM "{SCHEMA}"."USERS"
            WHERE department IS NOT NULL AND department != ''
            ORDER BY department
        ''').fetchdf()
        
        conn.close()
        
        departments = df['department'].tolist() if not df.empty else []
        
        return {
            'success': True,
            'data': departments,
            'count': len(departments)
        }
    except Exception as e:
        logger.error(f"Error getting departments: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# Authentication Endpoints

@router.post("/login")
async def login(login_data: LoginRequest):
    """
    Authenticate user with email and password.
    Returns user info and accessible modules based on department.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=True)
        
        # Find user by email (case-insensitive)
        user_result = conn.execute(f'''
            SELECT user_id, first_name, last_name, email, department, password, status
            FROM "{SCHEMA}"."USERS"
            WHERE LOWER(email) = LOWER(?)
        ''', [login_data.email]).fetchone()
        
        conn.close()
        
        if not user_result:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        user_id, first_name, last_name, email, department, password, status = user_result
        
        # Check if user is active
        if status != 'Active':
            raise HTTPException(status_code=403, detail="User account is terminated")
        
        # Verify password
        if password != login_data.password:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        # Get user's accessible modules based on department
        accessible_modules = []
        
        if department:
            # Always fetch from DEPARTMENT_PERMISSIONS for consistency
            try:
                init_department_permissions_table()
                conn = get_db_connection(read_only=True)
                perm_result = conn.execute(f'''
                    SELECT modules
                    FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                    WHERE department = ?
                ''', [department]).fetchone()
                conn.close()
                
                if perm_result:
                    modules_str = perm_result[0]
                    accessible_modules = [m.strip() for m in modules_str.split(',') if m.strip()]
                else:
                    # If no permissions found in database, user has no access
                    accessible_modules = []
                    logger.warning(f"No permissions found for department {department}")
            except Exception as e:
                logger.warning(f"Could not fetch permissions for department {department}: {e}")
                # If error fetching permissions, user has no access
                accessible_modules = []
        
        return {
            'success': True,
            'message': 'Login successful',
            'data': {
                'user_id': user_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'department': department,
                'accessible_modules': accessible_modules
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during login: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-modules/{email}")
async def get_user_modules(email: str):
    """
    Get accessible modules for a user based on their department.
    """
    try:
        init_users_table()
        conn = get_db_connection(read_only=True)
        
        # Get user's department
        user_result = conn.execute(f'''
            SELECT department, status
            FROM "{SCHEMA}"."USERS"
            WHERE LOWER(email) = LOWER(?)
        ''', [email]).fetchone()
        
        if not user_result:
            conn.close()
            raise HTTPException(status_code=404, detail="User not found")
        
        department, status = user_result
        
        if status != 'Active':
            conn.close()
            raise HTTPException(status_code=403, detail="User account is terminated")
        
        # Get accessible modules from database
        accessible_modules = []
        if department:
            try:
                init_department_permissions_table()
                perm_result = conn.execute(f'''
                    SELECT modules
                    FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                    WHERE department = ?
                ''', [department]).fetchone()
                
                if perm_result:
                    modules_str = perm_result[0]
                    accessible_modules = [m.strip() for m in modules_str.split(',') if m.strip()]
                else:
                    accessible_modules = []
                    logger.warning(f"No permissions found for department {department}")
            except Exception as e:
                logger.warning(f"Could not fetch permissions for department {department}: {e}")
                accessible_modules = []
        
        conn.close()
        
        return {
            'success': True,
            'data': {
                'department': department,
                'accessible_modules': accessible_modules
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user modules: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# Staff-Client Allocation Endpoints

@router.get("/staff-client-allocations")
async def get_staff_client_allocations():
    """
    Get all staff-client allocations.
    Only returns staff who have access to CLIENT ANALYSIS module.
    """
    try:
        init_staff_client_allocations_table()
        init_users_table()
        init_department_permissions_table()
        conn = get_db_connection(read_only=True)
        
        # Get all staff with CLIENT ANALYSIS access
        eligible_staff_query = f'''
            SELECT DISTINCT u.user_id, u.first_name, u.last_name, u.email, u.department
            FROM "{SCHEMA}"."USERS" u
            INNER JOIN "{SCHEMA}"."DEPARTMENT_PERMISSIONS" dp ON u.department = dp.department
            WHERE u.status = 'Active'
            AND (
                dp.modules LIKE '%client%' 
                OR u.department IN ('MGT', 'ADMIN', 'IT')
            )
            ORDER BY u.department, u.first_name, u.last_name
        '''
        eligible_staff = conn.execute(eligible_staff_query).fetchdf()
        
        # Get all allocations
        allocations_query = f'''
            SELECT allocation_id, user_id, groupname, created_at, updated_at
            FROM "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"
            ORDER BY user_id, groupname
        '''
        allocations_df = conn.execute(allocations_query).fetchdf()
        
        conn.close()
        
        # Group allocations by user
        allocations_by_user = {}
        if not allocations_df.empty:
            for _, row in allocations_df.iterrows():
                user_id = int(row['user_id'])
                if user_id not in allocations_by_user:
                    allocations_by_user[user_id] = []
                allocations_by_user[user_id].append({
                    'allocation_id': int(row['allocation_id']),
                    'groupname': str(row['groupname']),
                    'created_at': row['created_at'].isoformat() if hasattr(row['created_at'], 'isoformat') else str(row['created_at']),
                    'updated_at': row['updated_at'].isoformat() if hasattr(row['updated_at'], 'isoformat') else str(row['updated_at'])
                })
        
        # Build response with staff and their allocations
        staff_allocations = []
        if not eligible_staff.empty:
            for _, staff in eligible_staff.iterrows():
                user_id = int(staff['user_id'])
                staff_allocations.append({
                    'user_id': user_id,
                    'first_name': str(staff['first_name']),
                    'last_name': str(staff['last_name']),
                    'email': str(staff['email']),
                    'department': str(staff['department']),
                    'allocated_clients': allocations_by_user.get(user_id, [])
                })
        
        return {
            'success': True,
            'data': staff_allocations,
            'count': len(staff_allocations)
        }
    except Exception as e:
        logger.error(f"Error getting staff-client allocations: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/staff-client-allocations/{user_id}")
async def get_staff_client_allocation(user_id: int):
    """
    Get client allocations for a specific staff member.
    """
    try:
        init_staff_client_allocations_table()
        init_users_table()
        conn = get_db_connection(read_only=True)
        
        # Verify user exists and is active
        user_result = conn.execute(f'''
            SELECT user_id, first_name, last_name, email, department, status
            FROM "{SCHEMA}"."USERS"
            WHERE user_id = ?
        ''', [user_id]).fetchone()
        
        if not user_result:
            conn.close()
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id_db, first_name, last_name, email, department, status = user_result
        
        if status != 'Active':
            conn.close()
            raise HTTPException(status_code=403, detail="User account is terminated")
        
        # Get allocations
        allocations_query = f'''
            SELECT allocation_id, groupname, created_at, updated_at
            FROM "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"
            WHERE user_id = ?
            ORDER BY groupname
        '''
        allocations_df = conn.execute(allocations_query, [user_id]).fetchdf()
        
        conn.close()
        
        allocations = []
        if not allocations_df.empty:
            for _, row in allocations_df.iterrows():
                allocations.append({
                    'allocation_id': int(row['allocation_id']),
                    'groupname': str(row['groupname']),
                    'created_at': row['created_at'].isoformat() if hasattr(row['created_at'], 'isoformat') else str(row['created_at']),
                    'updated_at': row['updated_at'].isoformat() if hasattr(row['updated_at'], 'isoformat') else str(row['updated_at'])
                })
        
        return {
            'success': True,
            'data': {
                'user_id': user_id_db,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'department': department,
                'allocated_clients': allocations
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting staff-client allocation: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/eligible-staff")
async def get_eligible_staff():
    """
    Get list of staff eligible for client analysis allocation.
    Only returns staff in departments with CLIENT ANALYSIS access.
    """
    try:
        init_users_table()
        init_department_permissions_table()
        conn = get_db_connection(read_only=True)
        
        # Get staff with CLIENT ANALYSIS access
        eligible_staff_query = f'''
            SELECT DISTINCT u.user_id, u.first_name, u.last_name, u.email, u.department
            FROM "{SCHEMA}"."USERS" u
            INNER JOIN "{SCHEMA}"."DEPARTMENT_PERMISSIONS" dp ON u.department = dp.department
            WHERE u.status = 'Active'
            AND (
                dp.modules LIKE '%client%' 
                OR u.department IN ('MGT', 'ADMIN', 'IT')
            )
            ORDER BY u.department, u.first_name, u.last_name
        '''
        eligible_staff_df = conn.execute(eligible_staff_query).fetchdf()
        
        conn.close()
        
        staff_list = []
        if not eligible_staff_df.empty:
            for _, staff in eligible_staff_df.iterrows():
                staff_list.append({
                    'user_id': int(staff['user_id']),
                    'first_name': str(staff['first_name']),
                    'last_name': str(staff['last_name']),
                    'email': str(staff['email']),
                    'department': str(staff['department'])
                })
        
        return {
            'success': True,
            'data': staff_list,
            'count': len(staff_list)
        }
    except Exception as e:
        logger.error(f"Error getting eligible staff: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/available-clients")
async def get_available_clients():
    """
    Get list of all available clients from GROUP_CONTRACT.
    """
    try:
        conn = get_db_connection(read_only=True)
        
        clients_query = '''
            SELECT DISTINCT groupname
            FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
            WHERE iscurrent = 1
            ORDER BY groupname
        '''
        clients_df = conn.execute(clients_query).fetchdf()
        
        conn.close()
        
        clients = []
        if not clients_df.empty:
            clients = [str(row['groupname']) for _, row in clients_df.iterrows()]
        
        return {
            'success': True,
            'data': clients,
            'count': len(clients)
        }
    except Exception as e:
        logger.error(f"Error getting available clients: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/staff-client-allocations")
async def update_staff_client_allocation(allocation_data: StaffClientAllocationUpdate):
    """
    Update or create staff-client allocations.
    Replaces all existing allocations for the user with the new list.
    """
    try:
        init_staff_client_allocations_table()
        init_users_table()
        conn = get_db_connection(read_only=False)
        
        # Verify user exists and is active
        user_result = conn.execute(f'''
            SELECT user_id, status, department
            FROM "{SCHEMA}"."USERS"
            WHERE user_id = ?
        ''', [allocation_data.user_id]).fetchone()
        
        if not user_result:
            conn.close()
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id, status, department = user_result
        
        if status != 'Active':
            conn.close()
            raise HTTPException(status_code=403, detail="Cannot allocate clients to terminated user")
        
        # Verify user has CLIENT ANALYSIS access
        init_department_permissions_table()
        perm_result = conn.execute(f'''
            SELECT modules
            FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
            WHERE department = ?
        ''', [department]).fetchone()
        
        has_client_access = False
        if department and department.upper() in ['MGT', 'ADMIN', 'IT']:
            has_client_access = True
        elif perm_result:
            modules_str = perm_result[0]
            modules = [m.strip() for m in modules_str.split(',') if m.strip()]
            has_client_access = 'client' in modules
        
        if not has_client_access:
            conn.close()
            raise HTTPException(status_code=403, detail="User's department does not have CLIENT ANALYSIS access")
        
        # Delete existing allocations for this user
        conn.execute(f'''
            DELETE FROM "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"
            WHERE user_id = ?
        ''', [allocation_data.user_id])
        
        # Insert new allocations
        inserted_count = 0
        for groupname in allocation_data.groupnames:
            if groupname and groupname.strip():
                try:
                    max_id_result = conn.execute(f'SELECT COALESCE(MAX(allocation_id), 0) FROM "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"').fetchone()
                    next_id = (max_id_result[0] if max_id_result else 0) + 1
                    
                    conn.execute(f'''
                        INSERT INTO "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"
                        (allocation_id, user_id, groupname, created_at, updated_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ''', [next_id, allocation_data.user_id, groupname.strip()])
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Could not insert allocation for {groupname}: {e}")
        
        conn.close()
        
        return {
            'success': True,
            'message': f'Staff-client allocations updated successfully',
            'data': {
                'user_id': allocation_data.user_id,
                'allocated_clients': allocation_data.groupnames,
                'count': inserted_count
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating staff-client allocation: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/staff-client-allocations/{user_id}")
async def delete_staff_client_allocation(user_id: int):
    """
    Delete all client allocations for a staff member.
    """
    try:
        init_staff_client_allocations_table()
        conn = get_db_connection(read_only=False)
        
        # Check if user has allocations
        existing = conn.execute(f'''
            SELECT COUNT(*) FROM "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"
            WHERE user_id = ?
        ''', [user_id]).fetchone()
        
        if not existing or existing[0] == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="No allocations found for this user")
        
        # Delete all allocations
        conn.execute(f'''
            DELETE FROM "{SCHEMA}"."STAFF_CLIENT_ALLOCATIONS"
            WHERE user_id = ?
        ''', [user_id])
        
        conn.close()
        
        return {
            'success': True,
            'message': f'All client allocations removed for user {user_id}'
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting staff-client allocation: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-benefitcode-tables")
async def export_benefitcode_tables():
    """
    Export benefitcode and benefitcode_procedure tables to Excel.
    """
    try:
        from core.database import get_db_connection
        
        conn = get_db_connection(read_only=True)
        
        # Try different table name variations
        benefitcode_df = pd.DataFrame()
        benefitcode_procedure_df = pd.DataFrame()
        
        # Find benefitcode table
        print("🔍 Searching for benefitcode table...")
        benefitcode_variations = ['benefitcode', 'BENEFITCODE', 'BenefitCode']
        for var in benefitcode_variations:
            try:
                query = f'SELECT * FROM "{SCHEMA}"."{var}"'
                df = conn.execute(query).fetchdf()
                if not df.empty:
                    benefitcode_df = df
                    logger.info(f"✅ Found benefitcode ({var}): {len(df)} rows")
                    break
            except Exception as e:
                continue
        
        if benefitcode_df.empty:
            logger.warning("⚠️ benefitcode table not found or empty")
        
        # Find benefitcode_procedure table
        print("🔍 Searching for benefitcode_procedure table...")
        procedure_variations = ['benefitcode_procedure', 'BENEFITCODE_PROCEDURE', 'BenefitCode_Procedure']
        for var in procedure_variations:
            try:
                query = f'SELECT * FROM "{SCHEMA}"."{var}"'
                df = conn.execute(query).fetchdf()
                if not df.empty:
                    benefitcode_procedure_df = df
                    logger.info(f"✅ Found benefitcode_procedure ({var}): {len(df)} rows")
                    break
            except Exception as e:
                continue
        
        if benefitcode_procedure_df.empty:
            logger.warning("⚠️ benefitcode_procedure table not found or empty")
        
        # Create Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).parent.parent.parent
        output_file = project_root / f'benefitcode_export_{timestamp}.xlsx'
        
        with pd.ExcelWriter(str(output_file), engine='openpyxl') as writer:
            benefitcode_df.to_excel(writer, sheet_name='benefitcode', index=False)
            benefitcode_procedure_df.to_excel(writer, sheet_name='benefitcode_procedure', index=False)
        
        logger.info(f"✅ Exported to {output_file}")
        logger.info(f"   benefitcode: {len(benefitcode_df)} rows")
        logger.info(f"   benefitcode_procedure: {len(benefitcode_procedure_df)} rows")
        
        return FileResponse(
            str(output_file),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=f'benefitcode_export_{timestamp}.xlsx'
        )
        
    except Exception as e:
        logger.error(f"Error exporting benefitcode tables: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/debug-benefit-tables")
async def debug_benefit_tables():
    """Debug endpoint to see what benefit tables exist"""
    try:
        from core.database import get_db_connection
        conn = get_db_connection(read_only=True)
        
        # List all tables in the schema
        all_tables = conn.execute(f'SHOW TABLES FROM "{SCHEMA}"').fetchdf()
        benefit_related = all_tables[all_tables['name'].str.contains('benefit|plan', case=False)]
        
        result = {
            'all_tables': all_tables['name'].tolist() if not all_tables.empty else [],
            'benefit_related': benefit_related['name'].tolist() if not benefit_related.empty else []
        }
        
        # Try to query each benefit table
        for table in result['benefit_related']:
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table}"').fetchone()[0]
                result[table] = {'exists': True, 'row_count': count}
            except Exception as e:
                result[table] = {'exists': False, 'error': str(e)}
        
        return {'success': True, 'data': result}
    except Exception as e:
        logger.error(f"Debug error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-benefit-tables")
async def export_benefit_tables():
    """
    Export benefit-related tables to Excel:
    - planbenefitcode_limit
    - benefitcode
    - benefitcode_procedure
    """
    try:
        from core.database import get_db_connection
        
        conn = get_db_connection(read_only=True)
        
        # First, check what tables exist
        try:
            all_tables_df = conn.execute(f'SHOW TABLES FROM "{SCHEMA}"').fetchdf()
            all_table_names = all_tables_df['name'].str.lower().tolist() if not all_tables_df.empty else []
            logger.info(f"Available tables in {SCHEMA}: {len(all_table_names)} tables")
        except Exception as e:
            logger.warning(f"Could not list tables: {e}")
            all_table_names = []
        
        # Try different table name variations (DuckDB is case-sensitive)
        table_configs = {
            'planbenefitcode_limit': ['planbenefitcode_limit', 'PLANBENEFITCODE_LIMIT', 'PlanBenefitCode_Limit'],
            'benefitcode': ['benefitcode', 'BENEFITCODE', 'BenefitCode'],
            'benefitcode_procedure': ['benefitcode_procedure', 'BENEFITCODE_PROCEDURE', 'BenefitCode_Procedure']
        }
        
        dfs = {}
        for sheet_name, name_variations in table_configs.items():
            df = pd.DataFrame()
            found_table = None
            
            # Find which variation exists
            for var in name_variations:
                if var.lower() in [t.lower() for t in all_table_names]:
                    found_table = var
                    break
            
            if found_table:
                try:
                    query = f'SELECT * FROM "{SCHEMA}"."{found_table}"'
                    df = conn.execute(query).fetchdf()
                    logger.info(f"✅ Loaded {sheet_name} (table: {found_table}): {len(df)} rows")
                except Exception as e:
                    logger.warning(f"⚠️ Error querying {found_table}: {e}")
            else:
                # Try all variations anyway
                for var in name_variations:
                    try:
                        query = f'SELECT * FROM "{SCHEMA}"."{var}"'
                        df = conn.execute(query).fetchdf()
                        if not df.empty:
                            logger.info(f"✅ Loaded {sheet_name} (table: {var}): {len(df)} rows")
                            found_table = var
                            break
                    except:
                        continue
                
                if df.empty:
                    logger.warning(f"⚠️ Table {sheet_name} not found. Searched: {name_variations}")
            
            dfs[sheet_name] = df
        
        # Create Excel file in project root
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        project_root = Path(__file__).parent.parent.parent
        output_file = project_root / f'benefit_tables_export_{timestamp}.xlsx'
        
        with pd.ExcelWriter(str(output_file), engine='openpyxl') as writer:
            for sheet_name, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        return FileResponse(
            str(output_file),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=f'benefit_tables_export_{timestamp}.xlsx'
        )
        
    except Exception as e:
        logger.error(f"Error exporting benefit tables: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
