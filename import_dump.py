#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import argparse
import time


def parse_args():
    parser = argparse.ArgumentParser(description="Импорт SQL-дампа в PostgreSQL")
    
    parser.add_argument(
        "dump_file", 
        nargs="?", 
        default="dump.sql",
        help="Путь к SQL-файлу дампа (по умолчанию: dump.sql)"
    )
    
    parser.add_argument(
        "--host", 
        default="localhost",
        help="Хост PostgreSQL (по умолчанию: localhost)"
    )
    
    parser.add_argument(
        "--port", 
        type=int,
        default=5432,
        help="Порт PostgreSQL (по умолчанию: 5432)"
    )
    
    parser.add_argument(
        "--user", 
        default="postgres",
        help="Имя пользователя PostgreSQL (по умолчанию: postgres)"
    )
    
    parser.add_argument(
        "--password", 
        default="root",
        help="Пароль PostgreSQL (по умолчанию: root)"
    )
    
    parser.add_argument(
        "--database", 
        default="gar_simple_db",
        help="Имя базы данных PostgreSQL (по умолчанию: gar_simple_db)"
    )
    
    parser.add_argument(
        "--clean", 
        action="store_true",
        help="Очистить существующие таблицы перед импортом"
    )
    
    parser.add_argument(
        "--psql-path",
        default="C:\\Program Files\\PostgreSQL\\17\\bin\\psql.exe",
        help="Путь к исполняемому файлу psql (по умолчанию: C:\\Program Files\\PostgreSQL\\17\\bin\\psql.exe)"
    )
    
    return parser.parse_args()


def check_dump_file(dump_file):
    """Проверяет наличие файла дампа"""
    if not os.path.exists(dump_file):
        print(f"Ошибка: Файл дампа '{dump_file}' не найден!")
        return False
    
    if not os.path.isfile(dump_file):
        print(f"Ошибка: '{dump_file}' не является файлом!")
        return False
    
    file_size = os.path.getsize(dump_file)
    if file_size == 0:
        print(f"Предупреждение: Файл дампа '{dump_file}' пуст (0 байт)!")
        return False
    
    print(f"Файл дампа: {dump_file} (размер: {file_size/1024/1024:.2f} МБ)")
    return True


def check_psql(psql_path):
    """Проверяет наличие psql"""
    if not os.path.exists(psql_path):
        print(f"Ошибка: psql не найден по пути '{psql_path}'")
        return False
    
    try:
        # Пробуем запустить psql с параметром --version
        result = subprocess.run(
            [psql_path, "--version"], 
            capture_output=True, 
            text=True, 
            encoding="utf-8"
        )
        if result.returncode == 0:
            print(f"Найден {result.stdout.strip()}")
            return True
        else:
            print(f"Ошибка при проверке версии psql: {result.stderr}")
            return False
    except Exception as e:
        print(f"Ошибка при запуске psql: {e}")
        return False


def clean_database(psql_path, host, port, user, password, database):
    """Очищает существующие таблицы перед импортом"""
    print("\nОчистка базы данных...")
    
    # Устанавливаем переменные окружения для подключения
    env = os.environ.copy()
    env["PGHOST"] = host
    env["PGPORT"] = str(port)
    env["PGUSER"] = user
    env["PGPASSWORD"] = password
    env["PGDATABASE"] = database
    
    # SQL-команда для удаления таблиц
    clean_command = "DROP TABLE IF EXISTS BUILDINGS CASCADE; DROP TABLE IF EXISTS STREETS CASCADE; DROP TABLE IF EXISTS LOCALITIES CASCADE;"
    
    try:
        result = subprocess.run(
            [psql_path, "-c", clean_command],
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8"
        )
        
        if result.returncode == 0:
            print("Очистка базы данных выполнена успешно.")
        else:
            print(f"Предупреждение: Возникли проблемы при очистке базы данных:\n{result.stderr}")
            print("Продолжаем импорт...")
    except Exception as e:
        print(f"Ошибка при очистке базы данных: {e}")
        print("Продолжаем импорт...")


def import_dump(psql_path, dump_file, host, port, user, password, database):
    """Импортирует дамп в базу данных"""
    print("\nНачинаем импорт данных...")
    print("Это может занять некоторое время, пожалуйста, подождите.")
    
    # Устанавливаем переменные окружения для подключения
    env = os.environ.copy()
    env["PGHOST"] = host
    env["PGPORT"] = str(port)
    env["PGUSER"] = user
    env["PGPASSWORD"] = password
    env["PGDATABASE"] = database
    
    start_time = time.time()
    
    try:
        # Запускаем psql для импорта данных
        result = subprocess.run(
            [psql_path, "-v", "ON_ERROR_STOP=1", "-f", dump_file],
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8"
        )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        if result.returncode == 0:
            print(f"\nИмпорт успешно завершен за {elapsed_time:.2f} секунд!")
            return True
        else:
            print(f"\nОшибка при импорте данных. Код ошибки: {result.returncode}")
            print(f"Сообщение об ошибке:\n{result.stderr}")
            return False
    except Exception as e:
        print(f"\nОшибка при выполнении импорта: {e}")
        return False


def check_import_success(psql_path, host, port, user, password, database):
    """Проверяет наличие импортированных таблиц и количество строк"""
    print("\nПроверяем наличие импортированных таблиц...")
    
    # Устанавливаем переменные окружения для подключения
    env = os.environ.copy()
    env["PGHOST"] = host
    env["PGPORT"] = str(port)
    env["PGUSER"] = user
    env["PGPASSWORD"] = password
    env["PGDATABASE"] = database
    
    # SQL-запрос для проверки наличия таблиц
    check_tables_sql = """
    SELECT table_name, 
           (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) AS columns_count 
    FROM information_schema.tables t 
    WHERE table_schema = 'public' 
      AND (table_name ILIKE 'localities' OR table_name ILIKE 'streets' OR table_name ILIKE 'buildings');
    """
    
    try:
        result = subprocess.run(
            [psql_path, "-c", check_tables_sql],
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8"
        )
        
        if result.returncode == 0:
            tables_output = result.stdout
            print(tables_output)
            
            # Если таблицы найдены, проверяем количество строк
            if "0 rows" not in tables_output:
                print("\nПроверяем количество строк в таблицах...")
                count_rows_sql = """
                SELECT 'LOCALITIES' as table_name, COUNT(*) as row_count FROM LOCALITIES 
                UNION ALL 
                SELECT 'STREETS', COUNT(*) FROM STREETS 
                UNION ALL 
                SELECT 'BUILDINGS', COUNT(*) FROM BUILDINGS;
                """
                
                result = subprocess.run(
                    [psql_path, "-c", count_rows_sql],
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8"
                )
                
                if result.returncode == 0:
                    print(result.stdout)
                else:
                    print(f"Ошибка при подсчете строк: {result.stderr}")
            else:
                print("\nТаблицы не найдены. Проверяем таблицы с учетом регистра...")
                # Проверяем таблицы с учетом регистра (верхний регистр)
                check_uppercase_sql = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND (table_name = 'LOCALITIES' OR table_name = 'STREETS' OR table_name = 'BUILDINGS');
                """
                
                result = subprocess.run(
                    [psql_path, "-c", check_uppercase_sql],
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8"
                )
                
                if result.returncode == 0:
                    print(result.stdout)
                    
                    # Если найдены таблицы в верхнем регистре
                    if "0 rows" not in result.stdout:
                        print("\nПроверяем количество строк в таблицах (верхний регистр)...")
                        count_uppercase_rows_sql = """
                        SELECT 'LOCALITIES' as table_name, COUNT(*) as row_count FROM "LOCALITIES" 
                        UNION ALL 
                        SELECT 'STREETS', COUNT(*) FROM "STREETS" 
                        UNION ALL 
                        SELECT 'BUILDINGS', COUNT(*) FROM "BUILDINGS";
                        """
                        
                        result = subprocess.run(
                            [psql_path, "-c", count_uppercase_rows_sql],
                            env=env,
                            capture_output=True,
                            text=True,
                            encoding="utf-8"
                        )
                        
                        if result.returncode == 0:
                            print(result.stdout)
                        else:
                            print(f"Ошибка при подсчете строк (верхний регистр): {result.stderr}")
        else:
            print(f"Ошибка при проверке таблиц: {result.stderr}")
    
    except Exception as e:
        print(f"Ошибка при проверке импорта: {e}")


def main():
    """Основная функция"""
    args = parse_args()
    
    print("Импорт SQL-дампа в PostgreSQL")
    print("=" * 40)
    
    # Проверяем файл дампа
    if not check_dump_file(args.dump_file):
        sys.exit(1)
    
    # Проверяем наличие psql
    if not check_psql(args.psql_path):
        sys.exit(1)
    
    # Выводим параметры подключения
    print("\nПараметры подключения:")
    print(f"Хост: {args.host}")
    print(f"Порт: {args.port}")
    print(f"Пользователь: {args.user}")
    print(f"База данных: {args.database}")
    
    # Очищаем базу данных, если указан флаг --clean
    if args.clean:
        clean_database(
            args.psql_path,
            args.host,
            args.port,
            args.user,
            args.password,
            args.database
        )
    
    # Импортируем дамп
    success = import_dump(
        args.psql_path,
        args.dump_file,
        args.host,
        args.port,
        args.user,
        args.password,
        args.database
    )
    
    # Проверяем результат импорта
    if success:
        check_import_success(
            args.psql_path,
            args.host,
            args.port,
            args.user,
            args.password,
            args.database
        )
    
    print("\nОперация завершена.")


if __name__ == "__main__":
    main() 