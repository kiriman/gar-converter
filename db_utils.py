import os
import subprocess
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

def get_source_db_connection_string():
    """Получение строки подключения к исходной базе данных."""
    host = os.getenv('SOURCE_DB_HOST', 'localhost')
    port = os.getenv('SOURCE_DB_PORT', '3306')
    user = os.getenv('SOURCE_DB_USER', 'root')
    password = os.getenv('SOURCE_DB_PASSWORD', '')
    db_name = os.getenv('SOURCE_DB_NAME', 'gar_db')
    
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

def get_target_db_connection_string():
    """Получение строки подключения к целевой базе данных."""
    host = os.getenv('TARGET_DB_HOST', 'localhost')
    port = os.getenv('TARGET_DB_PORT', '3306')
    user = os.getenv('TARGET_DB_USER', 'root')
    password = os.getenv('TARGET_DB_PASSWORD', '')
    db_name = os.getenv('TARGET_DB_NAME', 'gar_simple_db')
    
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

def create_source_engine():
    """Создание движка SQLAlchemy для исходной базы данных."""
    connection_string = get_source_db_connection_string()
    return create_engine(connection_string)

def create_target_engine():
    """Создание движка SQLAlchemy для целевой базы данных."""
    connection_string = get_target_db_connection_string()
    return create_engine(connection_string)

def execute_query(engine, query, params=None):
    """Выполнение SQL-запроса."""
    try:
        with engine.connect() as connection:
            if params:
                result = connection.execute(text(query), params)
            else:
                result = connection.execute(text(query))
            return result
    except SQLAlchemyError as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None

def import_dump_file():
    """Импорт дампа SQL в исходную базу данных."""
    dump_file_path = os.getenv('DUMP_FILE_PATH')
    if not dump_file_path or not os.path.exists(dump_file_path):
        print(f"Файл дампа не найден: {dump_file_path}")
        return False
    
    host = os.getenv('SOURCE_DB_HOST', 'localhost')
    port = os.getenv('SOURCE_DB_PORT', '3306')
    user = os.getenv('SOURCE_DB_USER', 'root')
    password = os.getenv('SOURCE_DB_PASSWORD', '')
    db_name = os.getenv('SOURCE_DB_NAME', 'gar_db')
    
    try:
        # Создание базы данных, если она не существует
        cmd_create_db = f'mysql -h {host} -P {port} -u {user} -p{password} -e "CREATE DATABASE IF NOT EXISTS {db_name}"'
        subprocess.run(cmd_create_db, shell=True, check=True)
        
        # Импорт дампа
        cmd_import = f'mysql -h {host} -P {port} -u {user} -p{password} {db_name} < "{dump_file_path}"'
        subprocess.run(cmd_import, shell=True, check=True)
        
        print(f"Дамп успешно импортирован в базу данных {db_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ошибка импорта дампа: {e}")
        return False

def create_target_database():
    """Создание целевой базы данных."""
    host = os.getenv('TARGET_DB_HOST', 'localhost')
    port = os.getenv('TARGET_DB_PORT', '3306')
    user = os.getenv('TARGET_DB_USER', 'root')
    password = os.getenv('TARGET_DB_PASSWORD', '')
    db_name = os.getenv('TARGET_DB_NAME', 'gar_simple_db')
    
    try:
        cmd = f'mysql -h {host} -P {port} -u {user} -p{password} -e "CREATE DATABASE IF NOT EXISTS {db_name}"'
        subprocess.run(cmd, shell=True, check=True)
        print(f"База данных {db_name} успешно создана")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ошибка создания базы данных: {e}")
        return False 