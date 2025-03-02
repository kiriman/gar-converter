#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
from dotenv import load_dotenv
from db_utils import create_source_engine, execute_query

def load_config():
    """Загрузка конфигурации из .env файла"""
    load_dotenv()
    return {
        'source_db_host': os.getenv('SOURCE_DB_HOST', 'localhost'),
        'source_db_port': os.getenv('SOURCE_DB_PORT', '3306'),
        'source_db_user': os.getenv('SOURCE_DB_USER', 'root'),
        'source_db_password': os.getenv('SOURCE_DB_PASSWORD', 'root'),
        'source_db_name': os.getenv('SOURCE_DB_NAME', 'gar_address'),
        'base_path': os.getenv('BASE_PATH', '381755.95238215')
    }

def check_building_by_id(building_id):
    """Проверка наличия здания по ID в таблице mun_hierarchy"""
    engine = create_source_engine()
    
    # Проверка в mun_hierarchy
    query = f"""
    SELECT * FROM mun_hierarchy 
    WHERE PATH LIKE '%.{building_id}' OR PATH LIKE '%.{building_id}.%'
    """
    result = execute_query(engine, query)
    rows = result.fetchall() if result else []
    
    print(f"Найдено записей в mun_hierarchy: {len(rows)}")
    for row in rows:
        print(f"OBJECTID: {row.OBJECTID}, PATH: {row.PATH}, ISACTIVE: {row.ISACTIVE}")
    
    # Проверка в adm_hierarchy
    query = f"""
    SELECT * FROM adm_hierarchy 
    WHERE PATH LIKE '%.{building_id}' OR PATH LIKE '%.{building_id}.%'
    """
    result = execute_query(engine, query)
    rows = result.fetchall() if result else []
    
    print(f"\nНайдено записей в adm_hierarchy: {len(rows)}")
    for row in rows:
        print(f"OBJECTID: {row.OBJECTID}, PATH: {row.PATH}, ISACTIVE: {row.ISACTIVE}")
    
    # Проверка в house_types
    query = f"""
    SELECT * FROM houses 
    WHERE OBJECTID = {building_id}
    """
    result = execute_query(engine, query)
    rows = result.fetchall() if result else []
    
    print(f"\nНайдено записей в houses: {len(rows)}")
    for row in rows:
        print(f"OBJECTID: {row.OBJECTID}, HOUSENUM: {row.HOUSENUM if hasattr(row, 'HOUSENUM') else 'N/A'}")

def check_locality_by_name(locality_name):
    """Проверка наличия населенного пункта по имени в таблице addr_obj"""
    engine = create_source_engine()
    
    query = f"""
    SELECT * FROM addr_obj 
    WHERE NAME = '{locality_name}'
    """
    result = execute_query(engine, query)
    rows = result.fetchall() if result else []
    
    print(f"Найдено записей в addr_obj с именем '{locality_name}': {len(rows)}")
    for row in rows:
        print(f"OBJECTID: {row.OBJECTID}, NAME: {row.NAME}, TYPENAME: {row.TYPENAME}, LEVEL: {row.LEVEL}, ISACTIVE: {row.ISACTIVE}")
        
        # Проверяем наличие в иерархии
        mun_query = f"""
        SELECT * FROM mun_hierarchy 
        WHERE OBJECTID = {row.OBJECTID}
        """
        mun_result = execute_query(engine, mun_query)
        mun_rows = mun_result.fetchall() if mun_result else []
        
        print(f"  Найдено записей в mun_hierarchy: {len(mun_rows)}")
        for mun_row in mun_rows:
            print(f"  PATH: {mun_row.PATH}, ISACTIVE: {mun_row.ISACTIVE}")

def check_path_structure(base_path):
    """Анализ структуры путей в базе данных"""
    engine = create_source_engine()
    
    query = f"""
    SELECT 
        LENGTH(PATH) - LENGTH(REPLACE(PATH, '.', '')) as dots_count,
        COUNT(*) as count
    FROM mun_hierarchy 
    WHERE PATH LIKE '{base_path}.%'
    GROUP BY dots_count
    ORDER BY dots_count
    """
    result = execute_query(engine, query)
    rows = result.fetchall() if result else []
    
    print(f"Анализ структуры путей для базового пути '{base_path}':")
    for row in rows:
        dots = row[0] if hasattr(row, '0') else row.dots_count
        count = row[1] if hasattr(row, '1') else row.count
        octets = dots + 1
        print(f"Путей с {octets} октетами (точек: {dots}): {count}")

def check_target_db_buildings():
    """Проверка зданий в целевой базе данных"""
    from db_utils import create_target_engine
    
    engine = create_target_engine()
    
    query = """
    SELECT COUNT(*) as count FROM buildings
    """
    result = execute_query(engine, query)
    row = result.fetchone() if result else None
    count = row[0] if hasattr(row, '0') else row.count if row else 0
    
    print(f"Количество зданий в целевой базе данных: {count}")
    
    # Выборка нескольких зданий для примера
    query = """
    SELECT * FROM buildings LIMIT 5
    """
    result = execute_query(engine, query)
    rows = result.fetchall() if result else []
    
    print("\nПримеры зданий в целевой базе данных:")
    for row in rows:
        print(f"OBJECTID: {row.objectid}, HOUSE_NUM: {row.house_num}, STREET_ID: {row.street_id}")

def main():
    parser = argparse.ArgumentParser(description='Инструмент для проверки данных ГАР')
    subparsers = parser.add_subparsers(dest='command', help='Команды')
    
    # Команда для проверки здания по ID
    building_parser = subparsers.add_parser('building', help='Проверка здания по ID')
    building_parser.add_argument('building_id', type=int, help='ID здания для проверки')
    
    # Команда для проверки населенного пункта по имени
    locality_parser = subparsers.add_parser('locality', help='Проверка населенного пункта по имени')
    locality_parser.add_argument('name', type=str, help='Имя населенного пункта')
    
    # Команда для анализа структуры путей
    path_parser = subparsers.add_parser('paths', help='Анализ структуры путей')
    
    # Команда для проверки зданий в целевой БД
    target_parser = subparsers.add_parser('target', help='Проверка зданий в целевой БД')
    
    args = parser.parse_args()
    config = load_config()
    
    if args.command == 'building':
        check_building_by_id(args.building_id)
    elif args.command == 'locality':
        check_locality_by_name(args.name)
    elif args.command == 'paths':
        check_path_structure(config['base_path'])
    elif args.command == 'target':
        check_target_db_buildings()
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 