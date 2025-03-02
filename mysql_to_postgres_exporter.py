#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import time
import sys
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import urllib.parse

# Загрузка переменных окружения
load_dotenv()

def print_progress(current, total, description, start_time=None):
    """
    Выводит прогресс выполнения операции в одну строку.
    
    Args:
        current (int): Текущее значение прогресса
        total (int): Общее количество шагов
        description (str): Описание операции
        start_time (float, optional): Время начала операции для расчета оставшегося времени
    """
    percent = int(100 * current / total) if total > 0 else 0
    bar_length = 30
    filled_length = int(bar_length * current / total) if total > 0 else 0
    bar = '█' * filled_length + ' ' * (bar_length - filled_length)
    
    elapsed = ""
    if start_time is not None:
        elapsed_seconds = time.time() - start_time
        if elapsed_seconds > 0 and current > 0:
            estimated_total = elapsed_seconds * total / current
            remaining = estimated_total - elapsed_seconds
            elapsed = f" [{int(elapsed_seconds)}s<{int(remaining)}s]"
    
    sys.stdout.write(f"\r{description}: {percent}%|{bar}| {current}/{total}{elapsed}")
    sys.stdout.flush()
    
    # Если операция завершена, переходим на новую строку
    if current >= total:
        sys.stdout.write('\n')
        sys.stdout.flush()

class MySQLToPostgresExporter:
    """
    Класс для экспорта данных из MySQL и их подготовки для импорта в PostgreSQL.
    """
    
    def __init__(self, dump_file=None, verbose=False, check_connection=False, schema_only=False, data_only=False, 
              table_filter=None, encoding=None, ignore_encoding_errors=False, export_foreign_keys=False):
        """
        Инициализация экспортера
        
        Args:
            dump_file (str, optional): Путь к файлу дампа (переопределяет значение из .env)
            verbose (bool, optional): Режим подробного вывода
            check_connection (bool, optional): Только проверить подключение к базе данных
            schema_only (bool, optional): Экспортировать только схему базы данных (без данных)
            data_only (bool, optional): Экспортировать только данные (без создания таблиц)
            table_filter (list, optional): Список таблиц для экспорта
            encoding (str, optional): Кодировка исходных данных (utf8, cp1251, win1251)
            ignore_encoding_errors (bool, optional): Игнорировать ошибки кодировки и заменять проблемные символы
            export_foreign_keys (bool, optional): Экспортировать внешние ключи в отдельный файл
        """
        # Параметры подключения к исходной базе данных (MySQL)
        # Поддержка двух вариантов имен переменных: SOURCE_DB_ и IMPORT_DB_
        self.source_host = os.getenv('IMPORT_DB_HOST') or os.getenv('SOURCE_DB_HOST', 'localhost')
        self.source_port = os.getenv('IMPORT_DB_PORT') or os.getenv('SOURCE_DB_PORT', '3306')
        self.source_user = os.getenv('IMPORT_DB_USER') or os.getenv('SOURCE_DB_USER', 'root')
        self.source_password = os.getenv('IMPORT_DB_PASSWORD') or os.getenv('SOURCE_DB_PASSWORD', 'root')
        self.source_db = os.getenv('IMPORT_DB_NAME') or os.getenv('SOURCE_DB_NAME', 'gar_address')

        # Параметры подключения к целевой базе данных (PostgreSQL)
        self.target_host = os.getenv('TARGET_DB_HOST', 'localhost')
        self.target_port = os.getenv('TARGET_DB_PORT', '5432')
        self.target_user = os.getenv('TARGET_DB_USER', 'postgres')
        self.target_password = os.getenv('TARGET_DB_PASSWORD', 'postgres')
        self.target_db = os.getenv('TARGET_DB_NAME', 'gar_simple_db')
        
        # Параметры для сохранения дампа
        self.dump_file_path = dump_file or os.getenv('DUMP_FILE_PATH', 'dump.sql')
        self.verbose = verbose
        self.check_connection = check_connection
        self.schema_only = schema_only
        self.data_only = data_only
        self.table_filter = table_filter
        self.ignore_encoding_errors = ignore_encoding_errors
        self.export_foreign_keys = export_foreign_keys
        
        # Путь к файлу с внешними ключами
        if export_foreign_keys:
            base_path = os.path.splitext(self.dump_file_path)[0]
            self.fk_file_path = f"{base_path}_fk.sql"
            
        # Кодировка исходных данных
        self.source_encoding = encoding or os.getenv('SOURCE_DATA_ENCODING', 'utf8')
        if self.source_encoding.lower() in ('win1251', 'windows-1251', 'cp1251'):
            self.source_encoding = 'cp1251'
        else:
            self.source_encoding = 'utf8'
            
        if self.verbose:
            print(f"Кодировка исходных данных: {self.source_encoding}")
            if self.ignore_encoding_errors:
                print("Режим игнорирования ошибок кодировки: ВКЛ")
        
        # Создаем подключение к исходной БД
        self.source_engine = create_engine(
            f"mysql+pymysql://{self.source_user}:{self.source_password}@{self.source_host}:{self.source_port}/{self.source_db}?charset={self.source_encoding}"
        )
        
        # Словарь преобразования типов данных MySQL в PostgreSQL
        self.type_mapping = {
            'int': 'integer',
            'tinyint': 'smallint',
            'smallint': 'smallint',
            'mediumint': 'integer',
            'bigint': 'bigint',
            'float': 'real',
            'double': 'double precision',
            'decimal': 'numeric',
            'varchar': 'varchar',
            'char': 'char',
            'text': 'text',
            'tinytext': 'text',
            'mediumtext': 'text',
            'longtext': 'text',
            'datetime': 'timestamp',
            'timestamp': 'timestamp',
            'date': 'date',
            'time': 'time',
            'boolean': 'boolean',
            'tinyint(1)': 'boolean',
            'bit(1)': 'boolean',
        }
        
        # Проверяем наличие директории для сохранения файла дампа
        dump_dir = os.path.dirname(self.dump_file_path)
        if dump_dir and not os.path.exists(dump_dir):
            os.makedirs(dump_dir)
            
        if self.verbose:
            print(f"Параметры подключения к MySQL:")
            print(f"  Хост: {self.source_host}:{self.source_port}")
            print(f"  База данных: {self.source_db}")
            print(f"  Пользователь: {self.source_user}")
            print(f"Параметры подключения к PostgreSQL:")
            print(f"  Хост: {self.target_host}:{self.target_port}")
            print(f"  База данных: {self.target_db}")
            print(f"  Пользователь: {self.target_user}")
            print(f"Файл дампа: {self.dump_file_path}")

    def convert_mysql_type_to_postgres(self, mysql_type):
        """
        Преобразование типа данных MySQL в соответствующий тип PostgreSQL
        
        Args:
            mysql_type (str): Тип данных в MySQL
            
        Returns:
            str: Соответствующий тип данных в PostgreSQL
        """
        # Удаляем размерность из целочисленных типов
        mysql_type_clean = re.sub(r'int\(\d+\)', 'int', mysql_type.lower())
        
        # Обрабатываем особые случаи
        if '(' in mysql_type_clean:
            base_type = mysql_type_clean.split('(')[0]
            if base_type in self.type_mapping:
                # Сохраняем размерность для varchar и подобных типов
                if base_type in ['varchar', 'char']:
                    return mysql_type_clean.replace(base_type, self.type_mapping[base_type])
                return self.type_mapping[base_type]
        
        # Проверяем, есть ли прямое соответствие
        for mysql_pattern, pg_type in self.type_mapping.items():
            if mysql_pattern in mysql_type_clean:
                return pg_type
        
        # Если тип не найден, возвращаем как есть (с предупреждением)
        print(f"Предупреждение: Неизвестный тип данных MySQL: {mysql_type}")
        return mysql_type

    def get_tables(self):
        """
        Получение списка таблиц из исходной базы данных
        
        Returns:
            list: Список имен таблиц
        """
        inspector = inspect(self.source_engine)
        return inspector.get_table_names()

    def get_table_structure(self, table_name):
        """
        Получение структуры таблицы
        
        Args:
            table_name (str): Имя таблицы
            
        Returns:
            dict: Структура таблицы, содержащая информацию о столбцах и первичном ключе
        """
        try:
            structure = {'columns': [], 'primary_key': []}
            
            # Получаем метаданные таблицы
            inspector = inspect(self.source_engine)
            columns = inspector.get_columns(table_name)
            pk_columns = inspector.get_pk_constraint(table_name).get('constrained_columns', [])
            
            for column in columns:
                column_info = {
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column['nullable'],
                    'default': column.get('default')
                }
                
                # Добавляем информацию о первичном ключе
                if column['name'] in pk_columns:
                    column_info['primary_key'] = True
                
                structure['columns'].append(column_info)
            
            # Запоминаем имена столбцов первичного ключа
            structure['primary_key'] = pk_columns
            
            return structure
            
        except Exception as e:
            print(f"Ошибка при получении структуры таблицы {table_name}: {e}")
            return {'columns': [], 'primary_key': []}

    def get_table_comment(self, table_name):
        """
        Получение комментария к таблице
        
        Args:
            table_name (str): Имя таблицы
            
        Returns:
            str: Комментарий к таблице или пустая строка
        """
        with self.source_engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT table_comment FROM information_schema.tables "
                f"WHERE table_schema = '{self.source_db}' AND table_name = '{table_name}'"
            ))
            row = result.fetchone()
            if row and row[0]:
                return row[0]
        return ""

    def get_column_comments(self, table_name):
        """
        Получение комментариев к столбцам таблицы
        
        Args:
            table_name (str): Имя таблицы
            
        Returns:
            dict: Словарь с комментариями к столбцам {column_name: comment}
        """
        with self.source_engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT column_name, column_comment FROM information_schema.columns "
                f"WHERE table_schema = '{self.source_db}' AND table_name = '{table_name}'"
            ))
            return {row[0]: row[1] for row in result if row[1]}

    def escape_value(self, value, column_type):
        """
        Экранирование значений для PostgreSQL
        
        Args:
            value: Значение для экранирования
            column_type: Тип данных столбца
            
        Returns:
            str: Экранированное значение для вставки в SQL
        """
        if value is None:
            return 'NULL'
        
        # Преобразование для boolean типов
        if 'boolean' in column_type:
            if isinstance(value, int):
                return 'TRUE' if value == 1 else 'FALSE'
            if isinstance(value, str) and value.isdigit():
                return 'TRUE' if int(value) == 1 else 'FALSE'
            return 'TRUE' if value else 'FALSE'
            
        # Экранирование строковых типов
        if isinstance(value, str):
            # Специальная обработка для CP1251
            if self.source_encoding == 'cp1251':
                try:
                    # Пробуем обработать строку как cp1251
                    # Сначала проверяем, не является ли она уже валидной UTF-8
                    try:
                        value.encode('utf-8').decode('utf-8')
                    except UnicodeError:
                        # Пробуем декодировать как CP1251
                        try:
                            # Используем latin1 как промежуточную кодировку
                            latin1_bytes = value.encode('latin1')
                            # Перекодируем в cp1251, затем в utf-8
                            value = latin1_bytes.decode('cp1251')
                        except Exception as e:
                            if self.verbose and not self.ignore_encoding_errors:
                                print(f"Предупреждение: Ошибка преобразования кодировки: {e}")
                            
                            if self.ignore_encoding_errors:
                                # Заменяем проблемные символы на '?'
                                clean_value = ""
                                for char in value:
                                    try:
                                        char.encode('utf-8')
                                        clean_value += char
                                    except UnicodeError:
                                        clean_value += '?'
                                value = clean_value
                except Exception as e:
                    if self.verbose:
                        print(f"Предупреждение: Общая ошибка обработки строки: {e}")
                    
                    if self.ignore_encoding_errors:
                        # Если все методы не сработали, заменяем на строку с безопасными символами
                        value = ''.join(c if ord(c) < 128 else '?' for c in value)
            
            # Заменяем одинарные кавычки на двойные одинарные кавычки (для PostgreSQL)
            escaped_value = value.replace("'", "''")
            
            # В режиме игнорирования ошибок кодировки, дополнительно обеспечиваем, 
            # что все символы безопасны для UTF-8
            if self.ignore_encoding_errors:
                try:
                    escaped_value.encode('utf-8')
                except UnicodeError:
                    # Если символы всё ещё проблемные, производим принудительную очистку
                    escaped_value = ''.join(c if ord(c) < 128 else '?' for c in escaped_value)
            
            return f"'{escaped_value}'"
        
        # Обработка дат
        if isinstance(value, datetime):
            return f"'{value.isoformat()}'"
        
        # Другие типы возвращаем как строку
        return str(value)
        
    def _is_valid_utf8(self, text):
        """
        Проверяет, является ли строка валидной в UTF-8
        
        Args:
            text (str): Строка для проверки
            
        Returns:
            bool: True, если строка валидная UTF-8, иначе False
        """
        try:
            text.encode('utf-8').decode('utf-8')
            return True
        except UnicodeError:
            return False

    def generate_create_table(self, table_name, structure, table_comment, column_comments):
        """
        Генерирует SQL-запрос для создания таблицы в PostgreSQL
        
        Args:
            table_name (str): Имя таблицы
            structure (dict): Структура таблицы
            table_comment (str): Комментарий к таблице
            column_comments (dict): Комментарии к столбцам
            
        Returns:
            str: SQL-запрос для создания таблицы
        """
        # Используем имя таблицы в верхнем регистре без кавычек
        table_name_upper = table_name.upper()
        
        # Генерируем SQL запрос на удаление и создание таблицы
        sql = f"DROP TABLE IF EXISTS {table_name_upper} CASCADE;\n"
        sql += f"CREATE TABLE {table_name_upper} (\n"
        
        # Добавляем столбцы
        columns_sql = []
        
        for column in structure['columns']:
            column_name = column['name'].upper()  # Имя колонки в верхнем регистре
            pg_type = self.convert_mysql_type_to_postgres(str(column['type']))
            
            # Исправляем здесь: всегда используем NULL вместе с NOT 
            if not column['nullable']:
                nullable = " NOT NULL"
            else:
                nullable = " NULL"
                
            default = f" DEFAULT {column['default']}" if column['default'] is not None else ""
            
            # Формируем определение столбца
            column_sql = f"  {column_name} {pg_type}{nullable}{default}"
            columns_sql.append(column_sql)
            
        # Добавляем первичный ключ, если он есть
        if structure['primary_key']:
            pk_columns = ', '.join([col.upper() for col in structure['primary_key']])
            columns_sql.append(f"  PRIMARY KEY ({pk_columns})")
            
        # Объединяем все колонки
        sql += ',\n'.join(columns_sql)
        sql += "\n);\n\n"
        
        # Добавляем комментарий к таблице, если он есть
        if table_comment:
            escaped_comment = table_comment.replace("'", "''")
            sql += f"COMMENT ON TABLE {table_name_upper} IS '{escaped_comment}';\n"
            
        # Добавляем комментарии к столбцам, если они есть
        for column_name, comment in column_comments.items():
            if comment:
                column_name_upper = column_name.upper()
                escaped_comment = comment.replace("'", "''")
                sql += f"COMMENT ON COLUMN {table_name_upper}.{column_name_upper} IS '{escaped_comment}';\n"
                
        return sql

    def export_table_data(self, table_name, structure):
        """
        Экспорт данных таблицы в формате, подходящем для PostgreSQL
        
        Args:
            table_name (str): Имя таблицы
            structure (dict): Структура таблицы
            
        Returns:
            str: SQL-запросы для вставки данных
        """
        try:
            connection = self.source_engine.connect()
            
            # Получаем количество строк в таблице для отображения прогресса
            count_query = f"SELECT COUNT(*) FROM `{table_name}`"
            result = connection.execute(text(count_query))
            total_rows = result.scalar()
            
            if total_rows == 0:
                if self.verbose:
                    print(f"Таблица {table_name} пуста. Пропускаем.")
                return ""
            
            if self.verbose:
                print(f"Всего строк в таблице {table_name}: {total_rows}")
                
            # Обрабатываем пакетами по 1000 строк для минимизации нагрузки на память
            batch_size = 1000
            total_batches = (total_rows + batch_size - 1) // batch_size  # Округление вверх
            
            output = ""
            
            for batch_num in range(total_batches):
                offset = batch_num * batch_size
                
                # Получаем данные из таблицы
                query = f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"
                result = connection.execute(text(query))
                
                # Преобразуем строки в список словарей
                rows = []
                for row in result:
                    rows.append(dict(row._mapping))
                
                if not rows:
                    break
                    
                # Генерируем SQL для INSERT
                if batch_num == 0:
                    # Получаем имена колонок из первой строки
                    columns = structure['columns']
                    column_names = [col['name'].upper() for col in columns]
                    
                    # Начинаем SQL для INSERT
                    output += f"INSERT INTO {table_name.upper()} ({', '.join(column_names)}) VALUES\n"
                
                # Добавляем значения
                values_list = []
                for row in rows:
                    # Преобразуем значения строки
                    values = []
                    for column in structure['columns']:
                        column_name = column['name']
                        column_type = column['type']
                        value = row.get(column_name)
                        values.append(self.escape_value(value, column_type))
                    
                    values_str = f"({', '.join(values)})"
                    values_list.append(values_str)
                
                # Объединяем все значения
                output += ',\n'.join(values_list)
                
                # Если это не последний пакет, добавляем разделитель
                if batch_num < total_batches - 1:
                    output += ";\n\nINSERT INTO {0} ({1}) VALUES\n".format(
                        table_name.upper(), ', '.join(column_names)
                    )
                else:
                    output += ";\n"
                
                if self.verbose:
                    progress = (batch_num + 1) / total_batches * 100
                    print(f"Экспорт таблицы {table_name}: {progress:.1f}% ({(batch_num + 1) * batch_size} из {total_rows})")
            
            connection.close()
            return output
            
        except Exception as e:
            print(f"Ошибка при экспорте данных таблицы {table_name}: {e}")
            return ""

    def export_to_file(self):
        """
        Экспорт всех таблиц и данных в файл SQL для PostgreSQL
        
        Returns:
            bool: True, если экспорт выполнен успешно, иначе False
        """
        try:
            # Если указан фильтр таблиц, используем его, иначе получаем все таблицы
            if self.table_filter:
                tables = self.table_filter
                if self.verbose:
                    print(f"Экспорт только указанных таблиц: {', '.join(tables)}")
            else:
                tables = self.get_tables()
            
            total_tables = len(tables)
            
            if total_tables == 0:
                print("Не найдено таблиц для экспорта!")
                return False
                
            print(f"Найдено таблиц: {total_tables}")
            start_time = time.time()
            
            # Сортируем таблицы для корректного порядка импорта (справочники сначала)
            sorted_tables = self._get_tables_in_order(tables)
            
            with open(self.dump_file_path, 'w', encoding='utf-8') as f:
                # Добавляем заголовок файла с указанием кодировки
                f.write(f"-- MySQL to PostgreSQL dump generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("-- Generated by MySQL to PostgreSQL Exporter\n")
                f.write("-- Encoding: UTF-8\n\n")
                
                # Добавляем команды для настройки PostgreSQL
                f.write("-- Настройка параметров PostgreSQL\n")
                f.write("SET client_encoding = 'UTF8';\n")
                f.write("SET standard_conforming_strings = on;\n")
                f.write("SET check_function_bodies = false;\n")
                f.write("SET client_min_messages = warning;\n")
                f.write("SET escape_string_warning = off;\n\n")

                # Если экспортируется БД ГАР, добавляем команду для очистки
                # существующих последовательностей (если они есть)
                f.write("-- Сброс последовательностей\n")
                f.write("BEGIN;\n")
                f.write("DO $$\n")
                f.write("DECLARE\n")
                f.write("    seq_name text;\n")
                f.write("BEGIN\n")
                f.write("    FOR seq_name IN SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public' LOOP\n")
                f.write("        EXECUTE 'DROP SEQUENCE IF EXISTS ' || seq_name || ' CASCADE';\n")
                f.write("    END LOOP;\n")
                f.write("END $$;\n")
                f.write("COMMIT;\n\n")

                # Экспортируем каждую таблицу
                for i, table_name in enumerate(sorted_tables, 1):
                    print_progress(i, total_tables, f"Экспорт таблицы {table_name}", start_time)
                    
                    # Создание схемы, если не выбран режим только данных
                    if not self.data_only:
                        # Получаем структуру таблицы и комментарии
                        structure = self.get_table_structure(table_name)
                        table_comment = self.get_table_comment(table_name)
                        column_comments = self.get_column_comments(table_name)
                        
                        # Генерируем SQL для создания таблицы
                        create_table_sql = self.generate_create_table(table_name, structure, table_comment, column_comments)
                        f.write(f"\n-- Table: {table_name}\n")
                        f.write(create_table_sql)
                    
                    # Экспорт данных, если не выбран режим только схемы
                    if not self.schema_only:
                        structure = self.get_table_structure(table_name)
                        data_sql = self.export_table_data(table_name, structure)
                        if data_sql:
                            f.write(f"\n-- Data for table: {table_name}\n")
                            f.write(data_sql)
                            f.write("\n")
            
            elapsed_seconds = time.time() - start_time
            print(f"\nЭкспорт успешно завершен за {int(elapsed_seconds)} секунд.")
            print(f"Результат сохранен в файл: {self.dump_file_path}")
            
            # Выводим информацию о режиме экспорта
            if self.schema_only:
                print("Режим экспорта: только схема (без данных)")
            elif self.data_only:
                print("Режим экспорта: только данные (без схемы)")
            else:
                print("Режим экспорта: схема и данные")
                
            return True
        
        except Exception as e:
            print(f"Ошибка при экспорте: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    def _get_tables_in_order(self, tables):
        """
        Сортирует таблицы для правильного порядка импорта
        
        Args:
            tables (list): Список имен таблиц
            
        Returns:
            list: Отсортированный список таблиц
        """
        # Типы таблиц (справочники), которые должны импортироваться первыми
        reference_tables_patterns = [
            '_TYPES', 'LEVELS', 'KINDS', 'REESTR_OBJECTS'
        ]
        
        # Таблицы, которые должны импортироваться в середине
        middle_tables_patterns = [
            'ADDR_OBJ', 'STEADS', 'HOUSES', 'APARTMENTS', 'ROOMS', 'CARPLACES'
        ]
        
        # Таблицы, которые должны импортироваться в конце (содержат связи)
        last_tables_patterns = [
            '_PARAMS', '_HIERARCHY', '_DIVISION', 'CHANGE_HISTORY', 'NORMATIVE_DOCS'
        ]
        
        # Сортируем таблицы по приоритету
        reference_tables = []
        middle_tables = []
        last_tables = []
        other_tables = []
        
        for table in tables:
            # Проверяем, является ли таблица справочником
            if any(pattern in table for pattern in reference_tables_patterns):
                reference_tables.append(table)
            # Проверяем, является ли таблица промежуточной
            elif any(pattern in table for pattern in middle_tables_patterns):
                middle_tables.append(table)
            # Проверяем, является ли таблица финальной
            elif any(pattern in table for pattern in last_tables_patterns):
                last_tables.append(table)
            # Иначе добавляем в другие таблицы
            else:
                other_tables.append(table)
        
        # Возвращаем отсортированный список
        return reference_tables + other_tables + middle_tables + last_tables

    def test_connection(self, check_postgres=False):
        """
        Тестирование подключения к исходной базе данных и опционально к целевой
        
        Args:
            check_postgres (bool): Проверять ли также подключение к PostgreSQL
        
        Returns:
            bool: True, если подключение к MySQL успешно (и к PostgreSQL, если check_postgres=True), иначе False
        """
        mysql_success = False
        postgres_success = False
        
        # Проверка подключения к MySQL
        try:
            with self.source_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()
                if row and row[0] == 1:
                    print("✅ Подключение к MySQL успешно!")
                    mysql_success = True
        except Exception as e:
            print(f"❌ Ошибка подключения к MySQL: {e}")
        
        # Проверка подключения к PostgreSQL (опционально)
        if check_postgres:
            try:
                # Кодируем специальные символы в пароле
                encoded_password = urllib.parse.quote_plus(self.target_password)
                
                postgres_engine = create_engine(
                    f"postgresql://{self.target_user}:{encoded_password}@{self.target_host}:{self.target_port}/{self.target_db}"
                )
                with postgres_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1"))
                    row = result.fetchone()
                    if row and row[0] == 1:
                        print("✅ Подключение к PostgreSQL успешно!")
                        postgres_success = True
            except Exception as e:
                print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            
            # Выводим общий результат при проверке обоих подключений
            if mysql_success and postgres_success:
                print("✅ Все подключения успешны!")
                return True
            elif mysql_success:
                print("⚠️ Подключение к MySQL успешно, но подключение к PostgreSQL не удалось.")
                return False
            else:
                print("❌ Подключение к MySQL не удалось. Экспорт невозможен.")
                return False
        else:
            # Выводим результат при проверке только MySQL
            if mysql_success:
                return True
            else:
                print("❌ Подключение к MySQL не удалось. Экспорт невозможен.")
                return False

    def get_foreign_keys(self):
        """
        Получение информации о внешних ключах из MySQL
        
        Returns:
            list: Список словарей с информацией о внешних ключах
        """
        foreign_keys = []
        inspector = inspect(self.source_engine)
        
        for table_name in self.get_tables():
            fk_list = inspector.get_foreign_keys(table_name)
            
            for fk in fk_list:
                if not fk.get('name'):
                    # Генерируем имя для FK, если оно не задано
                    fk['name'] = f"fk_{fk['referred_table']}_{fk['constrained_columns'][0]}"
                
                foreign_keys.append({
                    'table': table_name,
                    'name': fk['name'],
                    'columns': fk['constrained_columns'],
                    'ref_table': fk['referred_table'],
                    'ref_columns': fk['referred_columns'],
                    'onupdate': fk.get('options', {}).get('onupdate', 'NO ACTION'),
                    'ondelete': fk.get('options', {}).get('ondelete', 'NO ACTION')
                })
        
        return foreign_keys
        
    def export_foreign_keys_to_file(self):
        """
        Экспорт внешних ключей в отдельный файл SQL
        
        Returns:
            bool: True, если экспорт выполнен успешно, иначе False
        """
        if not self.export_foreign_keys:
            return False
            
        try:
            foreign_keys = self.get_foreign_keys()
            
            if not foreign_keys:
                print("Внешние ключи не найдены.")
                return False
            
            print(f"Найдено внешних ключей: {len(foreign_keys)}")
            
            with open(self.fk_file_path, 'w', encoding='utf-8') as f:
                f.write(f"-- Foreign Keys for PostgreSQL\n")
                f.write(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("-- Настройка параметров PostgreSQL\n")
                f.write("SET client_encoding = 'UTF8';\n")
                f.write("SET standard_conforming_strings = on;\n\n")
                
                f.write("BEGIN;\n\n")
                
                # Генерируем SQL для создания внешних ключей
                for fk in foreign_keys:
                    table_name = fk['table'].upper()
                    fk_name = fk['name']
                    columns = ', '.join([col.upper() for col in fk['columns']])
                    ref_table = fk['ref_table'].upper()
                    ref_columns = ', '.join([col.upper() for col in fk['ref_columns']])
                    
                    # Чистим имя FK чтобы избежать проблем с PostgreSQL
                    safe_fk_name = re.sub(r'[^a-zA-Z0-9_]', '_', fk_name).upper()
                    
                    f.write(f"-- Foreign Key: {safe_fk_name}\n")
                    f.write(f"ALTER TABLE {table_name} ADD CONSTRAINT {safe_fk_name} FOREIGN KEY ({columns})\n")
                    f.write(f"    REFERENCES {ref_table} ({ref_columns}) MATCH SIMPLE\n")
                    f.write(f"    ON UPDATE {fk['onupdate']} ON DELETE {fk['ondelete']};\n\n")
                
                f.write("COMMIT;\n")
            
            print(f"Внешние ключи экспортированы в файл: {self.fk_file_path}")
            return True
            
        except Exception as e:
            print(f"Ошибка при экспорте внешних ключей: {e}")
            import traceback
            traceback.print_exc()
            return False

def parse_arguments():
    """
    Разбор аргументов командной строки
    
    Returns:
        argparse.Namespace: Объект с разобранными аргументами
    """
    parser = argparse.ArgumentParser(
        description='Экстрактор данных из MySQL в PostgreSQL для ГАР',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Путь к файлу дампа (переопределяет значение из .env)',
        default=None
    )
    
    parser.add_argument(
        '-v', '--verbose',
        help='Режим подробного вывода',
        action='store_true'
    )
    
    parser.add_argument(
        '-c', '--check',
        help='Только проверить подключение к базе данных MySQL',
        action='store_true'
    )
    
    parser.add_argument(
        '--check-postgres',
        help='Проверить также подключение к PostgreSQL',
        action='store_true'
    )
    
    parser.add_argument(
        '--schema-only',
        help='Экспортировать только схему базы данных (без данных)',
        action='store_true'
    )
    
    parser.add_argument(
        '--data-only',
        help='Экспортировать только данные (без создания таблиц)',
        action='store_true'
    )
    
    parser.add_argument(
        '--tables',
        help='Список таблиц для экспорта, разделенный запятыми',
        default=None
    )
    
    parser.add_argument(
        '--encoding',
        help='Кодировка исходных данных (utf8, cp1251, win1251)',
        default=None
    )
    
    parser.add_argument(
        '--ignore-encoding-errors',
        help='Игнорировать ошибки кодировки и заменять проблемные символы на "?"',
        action='store_true'
    )
    
    parser.add_argument(
        '--export-foreign-keys',
        help='Экспортировать внешние ключи в отдельный файл',
        action='store_true'
    )
    
    return parser.parse_args()

def print_banner():
    """
    Выводит информацию о программе
    """
    banner = """
╔═════════════════════════════════════════════════════════════════════╗
║                                                                     ║
║         MySQL в PostgreSQL экстрактор для ГАР                       ║
║                                                                     ║
╚═════════════════════════════════════════════════════════════════════╝

Инструмент для экспорта данных из MySQL базы ГАР и подготовки их 
для импорта в PostgreSQL.

ВАЖНО! Экспортер создает таблицы с именами в ВЕРХНЕМ РЕГИСТРЕ без кавычек,
что делает их хорошо видимыми в pgAdmin и других инструментах PostgreSQL.

Примеры использования:

1. Проверка подключения к MySQL:
   python mysql_to_postgres_exporter.py --check

2. Проверка подключения к MySQL и PostgreSQL:
   python mysql_to_postgres_exporter.py --check --check-postgres

3. Экспорт всех таблиц (схема и данные):
   python mysql_to_postgres_exporter.py

4. Экспорт только схемы:
   python mysql_to_postgres_exporter.py --schema-only

5. Экспорт только данных:
   python mysql_to_postgres_exporter.py --data-only

6. Экспорт выбранных таблиц:
   python mysql_to_postgres_exporter.py --tables "ADDR_OBJ,ADDR_OBJ_PARAMS"

7. Сохранение в указанный файл:
   python mysql_to_postgres_exporter.py --output /path/to/dump.sql

8. Указание кодировки исходных данных (для решения проблем с Windows-1251):
   python mysql_to_postgres_exporter.py --encoding cp1251

9. Игнорирование ошибок кодировки:
   python mysql_to_postgres_exporter.py --encoding cp1251 --ignore-encoding-errors

10. Экспорт внешних ключей:
    python mysql_to_postgres_exporter.py --export-foreign-keys

11. Подробный вывод:
    python mysql_to_postgres_exporter.py -v

После экспорта используйте команду для импорта в PostgreSQL:
psql -U postgres -d gar_simple_db -f dump.sql
"""
    print(banner)

def main():
    """
    Основная функция для запуска экспорта.
    """
    # Выводим информацию о программе
    print_banner()
    
    args = parse_arguments()
    
    print("Запуск экспорта данных из MySQL в PostgreSQL...")
    
    # Создаем экземпляр экспортера
    exporter = MySQLToPostgresExporter(
        dump_file=args.output,
        verbose=args.verbose,
        check_connection=args.check,
        schema_only=args.schema_only,
        data_only=args.data_only,
        table_filter=args.tables.split(',') if args.tables else None,
        encoding=args.encoding,
        ignore_encoding_errors=args.ignore_encoding_errors,
        export_foreign_keys=args.export_foreign_keys
    )
    
    # Если нужно только проверить соединение
    if args.check:
        return exporter.test_connection(check_postgres=args.check_postgres)
    
    # Запускаем экспорт
    result = exporter.export_to_file()
    
    # Если нужно экспортировать внешние ключи
    if result and args.export_foreign_keys:
        fk_result = exporter.export_foreign_keys_to_file()
        if fk_result:
            print("✅ Внешние ключи экспортированы успешно!")
    
    if result:
        print("✅ Экспорт успешно завершен!")
        if args.export_foreign_keys:
            print("⚠️ ВАЖНО: Внешние ключи экспортированы в отдельный файл.")
            print(f"   Импортируйте основной дамп сначала, затем файл с внешними ключами.")
            print(f"   Файл с внешними ключами: {exporter.fk_file_path}")
        print("\nДля импорта дампа в PostgreSQL используйте команду:")
        print(f"psql -U {exporter.target_user} -d {exporter.target_db} -f {exporter.dump_file_path}")
    else:
        print("❌ Экспорт завершился с ошибками")
    
    return result

if __name__ == "__main__":
    main() 