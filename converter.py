import os
import time
import sys
from sqlalchemy import text
from db_utils import create_source_engine, create_target_engine, execute_query
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Загрузка переменных окружения
load_dotenv()

# Функция для отображения прогресса в одну строку
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

class GarConverter:
    """
    Класс для конвертации данных из базы ГАР в упрощенную структуру
    для Приморского края и Артемовского городского округа.
    """
    
    def __init__(self, max_workers=8):
        """
        Инициализация конвертера.
        
        Args:
            max_workers (int): Максимальное количество рабочих потоков
        """
        self.source_engine = create_source_engine()
        self.target_engine = create_target_engine()
        self.base_path = os.getenv('BASE_PATH', '381755.95238215')
        self.max_workers = max_workers
        # Увеличиваем размеры пакетов для ускорения обработки
        self.locality_batch_size = 10
        self.street_batch_size = 100  # Увеличено с 50 до 100
        self.building_batch_size = 500  # Увеличено с 100 до 500
        print(f"Базовый путь: {self.base_path}")
        print(f"Количество рабочих потоков: {self.max_workers}")
        print(f"Размер пакета для населенных пунктов: {self.locality_batch_size}")
        print(f"Размер пакета для улиц: {self.street_batch_size}")
        print(f"Размер пакета для домов: {self.building_batch_size}")
    
    def create_target_schema(self):
        """Создание схемы таблиц в целевой базе данных."""
        try:
            # Создаем таблицу населенных пунктов
            localities_table = """
            CREATE TABLE IF NOT EXISTS localities (
                id INT AUTO_INCREMENT PRIMARY KEY,
                objectid INT NOT NULL,
                objectguid VARCHAR(36) NOT NULL,
                name VARCHAR(250) NOT NULL,
                typename VARCHAR(50) NOT NULL,
                path_octets VARCHAR(128) NOT NULL,
                UNIQUE KEY (objectid)
            ) ENGINE=InnoDB;
            """
            
            # Создаем таблицу улиц
            streets_table = """
            CREATE TABLE IF NOT EXISTS streets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                locality_id INT NOT NULL,
                objectid INT NOT NULL,
                objectguid VARCHAR(36) NOT NULL,
                name VARCHAR(250) NOT NULL,
                typename VARCHAR(50) NOT NULL,
                path_octets VARCHAR(128) NOT NULL,
                UNIQUE KEY (objectid),
                FOREIGN KEY (locality_id) REFERENCES localities(id)
            ) ENGINE=InnoDB;
            """
            
            # Создаем таблицу домов
            buildings_table = """
            CREATE TABLE IF NOT EXISTS buildings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                street_id INT NOT NULL,
                objectid INT NOT NULL,
                objectguid VARCHAR(36) NOT NULL,
                house_num VARCHAR(50) NULL,
                add_num1 VARCHAR(50) NULL,
                add_num2 VARCHAR(50) NULL,
                housetype INT NULL,
                addtype1 INT NULL,
                addtype2 INT NULL,
                path_octets VARCHAR(128) NOT NULL,
                UNIQUE KEY (objectid),
                FOREIGN KEY (street_id) REFERENCES streets(id)
            ) ENGINE=InnoDB;
            """
            
            # Выполняем скрипты для создания таблиц
            execute_query(self.target_engine, localities_table)
            execute_query(self.target_engine, streets_table)
            execute_query(self.target_engine, buildings_table)
            
            print("Схема успешно создана в целевой БД")
            return True
        except Exception as e:
            print(f"Ошибка при создании схемы: {e}")
            return False
    
    def clear_target_db(self):
        """Очистка таблиц в целевой базе данных перед конвертацией."""
        try:
            # Отключаем проверку внешних ключей
            execute_query(self.target_engine, "SET FOREIGN_KEY_CHECKS = 0;")
            
            # Очищаем таблицы
            execute_query(self.target_engine, "TRUNCATE TABLE buildings;")
            execute_query(self.target_engine, "TRUNCATE TABLE streets;")
            execute_query(self.target_engine, "TRUNCATE TABLE localities;")
            
            # Включаем проверку внешних ключей
            execute_query(self.target_engine, "SET FOREIGN_KEY_CHECKS = 1;")
            
            print("Таблицы в целевой БД успешно очищены")
            return True
        except Exception as e:
            print(f"Ошибка при очистке таблиц: {e}")
            return False
    
    def fill_dictionaries(self):
        """Заполнение справочников в целевой базе данных, если необходимо."""
        # В текущей реализации справочники не требуются
        return True
    
    def run_conversion(self):
        """Запуск процесса конвертации данных."""
        print(f"Запуск конвертации данных ГАР для пути: {self.base_path}")
        
        try:
            # Этап 1: Получаем объекты для конвертации
            print("Этап 1: Получение объектов для конвертации")
            query = f"""
            SELECT OBJECTID, PATH 
            FROM mun_hierarchy 
            WHERE PATH LIKE '{self.base_path}.%' 
            AND ISACTIVE = 1
            """
            
            result = self.execute_with_retry(self.source_engine, query)
            if not result:
                print("Не удалось получить объекты для конвертации")
                return False
            
            rows = result.fetchall()
            total_objects = len(rows)
            
            if total_objects == 0:
                print(f"Не найдено объектов для конвертации по пути {self.base_path}")
                return False
            
            # Определяем регион и район по первым двум октетам
            region_query = f"""
            SELECT ao.NAME 
            FROM addr_obj ao 
            JOIN mun_hierarchy mh ON ao.OBJECTID = mh.OBJECTID 
            WHERE mh.PATH = '{self.base_path.split('.')[0]}'
            AND ao.ISACTUAL = 1 AND ao.ISACTIVE = 1
            LIMIT 1
            """
            
            region_result = self.execute_with_retry(self.source_engine, region_query)
            region_row = region_result.fetchone() if region_result else None
            region_name = region_row.NAME if region_row else "Неизвестный регион"
            
            district_query = f"""
            SELECT ao.NAME 
            FROM addr_obj ao 
            JOIN mun_hierarchy mh ON ao.OBJECTID = mh.OBJECTID 
            WHERE mh.PATH = '{self.base_path}'
            AND ao.ISACTUAL = 1 AND ao.ISACTIVE = 1
            LIMIT 1
            """
            
            district_result = self.execute_with_retry(self.source_engine, district_query)
            district_row = district_result.fetchone() if district_result else None
            district_name = district_row.NAME if district_row else "Неизвестный район"
            
            print(f"Найдено {total_objects} объектов для конвертации в {region_name}, {district_name}")
            
            # Этап 2: Извлекаем и сохраняем населенные пункты
            print("Этап 2: Извлечение и сохранение населенных пунктов")
            localities_count = self.process_localities_parallel()
            
            if localities_count == 0:
                print("Не удалось обработать населенные пункты")
                return False
            
            # Этап 3: Извлекаем и сохраняем улицы
            print("Этап 3: Извлечение и сохранение улиц")
            streets_count = self.process_streets_parallel()
            
            if streets_count == 0:
                print("Не удалось обработать улицы")
                return False
            
            # Этап 4: Извлекаем и сохраняем здания
            print("Этап 4: Извлечение и сохранение зданий")
            buildings_count = self.process_buildings_parallel()
            
            print(f"Конвертация завершена. Обработано: {localities_count} населенных пунктов, {streets_count} улиц, {buildings_count} зданий")
            return True
            
        except Exception as e:
            print(f"Ошибка при конвертации данных: {e}")
            return False
    
    def execute_with_retry(self, engine, query, params=None, max_retries=3):
        """Выполнение запроса с повторными попытками при ошибке."""
        retries = 0
        while retries < max_retries:
            try:
                return execute_query(engine, query, params)
            except Exception as e:
                retries += 1
                print(f"Ошибка при выполнении запроса (попытка {retries}/{max_retries}): {e}")
                if retries >= max_retries:
                    raise
                time.sleep(1)  # Пауза перед повторной попыткой
    
    def process_localities_parallel(self):
        """
        Обработка населенных пунктов с использованием параллельных потоков.
        Извлекает уникальные ID населенных пунктов из третьего октета пути.
        """
        print("Этап 2: Обработка населенных пунктов...")
        
        # Получаем уникальные ID населенных пунктов из третьего октета пути
        query = f"""
        SELECT DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(PATH, '.', 3), '.', -1) as locality_id
        FROM mun_hierarchy
        WHERE PATH LIKE '{self.base_path}.%'
        AND ISACTIVE = 1
        """
        
        result = self.execute_with_retry(self.source_engine, query)
        if not result:
            print("Не удалось получить список населенных пунктов")
            return 0
        
        # Получаем список ID населенных пунктов
        locality_ids = [row.locality_id for row in result.fetchall()]
        unique_localities = len(locality_ids)
        print(f"Найдено {unique_localities} уникальных населенных пунктов")
        
        # Разбиваем на группы для параллельной обработки
        batch_size = self.locality_batch_size
        batches = [locality_ids[i:i+batch_size] for i in range(0, len(locality_ids), batch_size)]
        
        # Создаем пул потоков для параллельной обработки
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Запускаем обработку каждой группы в отдельном потоке
            futures = [executor.submit(self._process_locality_batch, batch, batch_idx, len(batches)) 
                      for batch_idx, batch in enumerate(batches)]
            
            # Собираем результаты с отображением прогресса
            processed_ids = []
            start_time = time.time()
            completed = 0
            total = len(futures)
            
            # Инициализируем прогресс
            print_progress(completed, total, "Обработка населенных пунктов", start_time)
            
            for future in as_completed(futures):
                try:
                    batch_processed_ids = future.result()
                    processed_ids.extend(batch_processed_ids)
                except Exception as e:
                    print(f"\nОшибка при обработке группы населенных пунктов: {e}")
                
                completed += 1
                print_progress(completed, total, "Обработка населенных пунктов", start_time)
        
        # Проверяем, все ли населенные пункты были обработаны
        missing_ids = [id for id in locality_ids if id not in processed_ids]
        
        if missing_ids:
            print(f"Не удалось обработать {len(missing_ids)} населенных пунктов. Попытка альтернативной обработки...")
            additional_processed = self._process_missing_localities(missing_ids)
            total_processed = len(processed_ids) + additional_processed
        else:
            total_processed = len(processed_ids)
        
        print(f"Всего обработано населенных пунктов: {total_processed} из {unique_localities}")
        return total_processed
    
    def process_streets_parallel(self):
        """
        Параллельная обработка и сохранение улиц (4-й октет).
        """
        print("Этап 3: Обработка улиц...")
        
        # Получаем список населенных пунктов из целевой БД
        query = "SELECT id, objectid FROM localities"
        result = self.execute_with_retry(self.target_engine, query)
        if not result:
            print("Не удалось получить список населенных пунктов")
            return 0
            
        localities = {str(loc.objectid): loc.id for loc in result.fetchall()}
        
        if not localities:
            print("Не найдено населенных пунктов в целевой БД")
            return 0
            
        print(f"Найдено {len(localities)} населенных пунктов для обработки улиц")
        
        # Получаем уникальные ID улиц из четвертого октета пути
        query = f"""
        SELECT DISTINCT 
            SUBSTRING_INDEX(SUBSTRING_INDEX(PATH, '.', 3), '.', -1) as locality_id,
            SUBSTRING_INDEX(SUBSTRING_INDEX(PATH, '.', 4), '.', -1) as street_id
        FROM mun_hierarchy
        WHERE PATH LIKE '{self.base_path}.%.%.%'
        AND ISACTIVE = 1
        """
        
        result = self.execute_with_retry(self.source_engine, query)
        if not result:
            print("Не удалось получить список улиц")
            return 0
            
        # Группируем улицы по населенным пунктам
        locality_streets = {}
        for row in result.fetchall():
            if row.locality_id not in locality_streets:
                locality_streets[row.locality_id] = set()
            locality_streets[row.locality_id].add(row.street_id)
        
        # Формируем список всех улиц для обработки
        all_streets = []
        all_locality_ids = {}  # street_id -> locality_id
        
        for locality_id, streets in locality_streets.items():
            if locality_id not in localities:
                continue
                
            target_locality_id = localities[locality_id]
            for street_id in streets:
                all_streets.append(street_id)
                all_locality_ids[street_id] = target_locality_id
        
        unique_streets = len(all_streets)
        print(f"Найдено {unique_streets} уникальных улиц")
        
        # Разбиваем на группы для параллельной обработки
        batch_size = self.street_batch_size
        batches = [all_streets[i:i+batch_size] for i in range(0, len(all_streets), batch_size)]
        
        # Создаем пул потоков для параллельной обработки
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Запускаем обработку каждой группы в отдельном потоке
            futures = [executor.submit(self._process_street_batch, batch, all_locality_ids, batch_idx, len(batches)) 
                      for batch_idx, batch in enumerate(batches)]
            
            # Собираем результаты с отображением прогресса
            processed_ids = []
            start_time = time.time()
            completed = 0
            total = len(futures)
            
            # Инициализируем прогресс
            print_progress(completed, total, "Обработка улиц", start_time)
            
            for future in as_completed(futures):
                try:
                    batch_processed_ids = future.result()
                    processed_ids.extend(batch_processed_ids)
                except Exception as e:
                    print(f"\nОшибка при обработке группы улиц: {e}")
                
                completed += 1
                print_progress(completed, total, "Обработка улиц", start_time)
        
        # Проверяем, все ли улицы были обработаны
        missing_ids = [id for id in all_streets if id not in processed_ids]
        
        if missing_ids:
            print(f"Не удалось обработать {len(missing_ids)} улиц")
            # Здесь можно добавить метод для обработки пропущенных улиц, аналогично _process_missing_localities
        
        total_processed = len(processed_ids)
        print(f"Всего обработано улиц: {total_processed} из {unique_streets}")
        return total_processed
        
    def _process_street_batch(self, batch, locality_ids, batch_idx, total_batches):
        """
        Обработка одной группы улиц.
        
        Args:
            batch (list): Список ID улиц для обработки
            locality_ids (dict): Словарь соответствия ID улиц и ID населенных пунктов
            batch_idx (int): Индекс текущей группы
            total_batches (int): Общее количество групп
            
        Returns:
            list: Список успешно обработанных ID улиц
        """
        processed_ids = []
        
        try:
            # Формируем список ID для запроса
            id_list = ", ".join([f"'{id}'" for id in batch])
            
            # Получаем данные из addr_obj по objectid
            query = f"""
            SELECT ao.OBJECTID, ao.OBJECTGUID, ao.NAME, ao.TYPENAME, mh.PATH 
            FROM addr_obj ao
            JOIN mun_hierarchy mh ON ao.OBJECTID = mh.OBJECTID
            WHERE ao.OBJECTID IN ({id_list})
            AND ao.ISACTUAL = 1 AND ao.ISACTIVE = 1
            """
            
            # Выполняем запрос с повторными попытками
            result = self.execute_with_retry(self.source_engine, query)
            streets = result.fetchall() if result else []
            
            # Сохраняем результаты в целевую БД
            with self.target_engine.begin() as connection:
                for street in streets:
                    street_id = str(street.OBJECTID)
                    if street_id in locality_ids:
                        insert_query = """
                        INSERT INTO streets (objectid, objectguid, name, typename, path_octets, locality_id)
                        VALUES (:objectid, :objectguid, :name, :typename, :path_octets, :locality_id)
                        ON DUPLICATE KEY UPDATE 
                        objectguid = :objectguid, name = :name, typename = :typename, 
                        path_octets = :path_octets, locality_id = :locality_id
                        """
                        connection.execute(text(insert_query), {
                            "objectid": street.OBJECTID,
                            "objectguid": street.OBJECTGUID,
                            "name": street.NAME,
                            "typename": street.TYPENAME,
                            "path_octets": street.PATH,
                            "locality_id": locality_ids[street_id]
                        })
                        
                        # Добавляем ID в список обработанных
                        processed_ids.append(street_id)
            
            # Убираем вывод информации о прогрессе, чтобы не перебивать прогресс-бар
            # if batch_idx == 0 or batch_idx == total_batches - 1 or batch_idx % 5 == 0:
            #     print(f"Обработано {len(processed_ids)} улиц в группе {batch_idx+1}/{total_batches}")
            
        except Exception as e:
            print(f"\nОшибка при обработке группы улиц {batch_idx+1}: {e}")
        
        return processed_ids
    
    def process_buildings_parallel(self):
        """
        Извлечение и сохранение зданий по улицам с использованием параллельной обработки.
        
        Returns:
            int: Количество успешно обработанных зданий
        """
        print("Этап 4: Обработка зданий...")
        
        # Получаем список улиц из целевой БД для привязки зданий
        query = "SELECT objectid, id FROM streets"
        result = self.execute_with_retry(self.target_engine, query)
        
        streets = {str(street.objectid): street.id for street in result.fetchall()}
        
        if not streets:
            print("Не найдено улиц в целевой БД")
            return 0
            
        print(f"Найдено {len(streets)} улиц для обработки зданий")
        
        # Создаем временные индексы для ускорения запросов (если их нет)
        try:
            # Проверяем существование индекса для mun_hierarchy.PATH
            check_mh_index = """
            SELECT COUNT(*) as cnt FROM information_schema.statistics 
            WHERE table_schema = DATABASE() 
            AND table_name = 'mun_hierarchy' 
            AND index_name = 'idx_mun_hierarchy_path'
            """
            result = self.execute_with_retry(self.source_engine, check_mh_index)
            if result and result.fetchone().cnt == 0:
                # Индекс не существует, создаем его
                self.execute_with_retry(self.source_engine, "CREATE INDEX idx_mun_hierarchy_path ON mun_hierarchy(PATH)")
                print("Создан индекс idx_mun_hierarchy_path")
            
            # Проверяем существование индекса для houses.OBJECTID
            check_houses_index = """
            SELECT COUNT(*) as cnt FROM information_schema.statistics 
            WHERE table_schema = DATABASE() 
            AND table_name = 'houses' 
            AND index_name = 'idx_houses_objectid'
            """
            result = self.execute_with_retry(self.source_engine, check_houses_index)
            if result and result.fetchone().cnt == 0:
                # Индекс не существует, создаем его
                self.execute_with_retry(self.source_engine, "CREATE INDEX idx_houses_objectid ON houses(OBJECTID)")
                print("Создан индекс idx_houses_objectid")
            
            print("Проверка индексов завершена")
        except Exception as e:
            print(f"Предупреждение: не удалось создать временные индексы: {e}")
            print("Продолжаем обработку без дополнительных индексов...")
        
        # Оптимизированный запрос для извлечения зданий из mun_hierarchy с путями, соответствующими шаблону
        # Используем STRAIGHT_JOIN для принудительного порядка соединения таблиц
        query = f"""
        SELECT 
            mh.PATH,
            mh.OBJECTID,
            SUBSTRING_INDEX(SUBSTRING_INDEX(mh.PATH, '.', 4), '.', -1) as street_id,
            SUBSTRING_INDEX(SUBSTRING_INDEX(mh.PATH, '.', 5), '.', -1) as building_id
        FROM mun_hierarchy mh
        WHERE mh.PATH LIKE '{self.base_path}.%.%.%'
        AND mh.ISACTIVE = 1
        """
        
        result = self.execute_with_retry(self.source_engine, query)
        if not result:
            print("Не удалось получить список зданий")
            return 0
            
        # Словарь для группировки зданий по улицам
        street_buildings = {}
        
        # Обрабатываем каждую запись
        rows_processed = 0
        rows_with_building = 0
        
        for row in result.fetchall():
            rows_processed += 1
            street_id = row.street_id
            building_id = row.building_id
            
            # Проверяем, есть ли улица в нашем списке
            if street_id in streets:
                if street_id not in street_buildings:
                    street_buildings[street_id] = set()
                street_buildings[street_id].add(building_id)
                rows_with_building += 1
        
        print(f"Обработано записей: {rows_processed}, из них зданий: {rows_with_building}")
        
        # Формируем список всех зданий для обработки
        all_buildings = []
        all_street_ids = {}  # building_id -> street_id
        
        for street_id, buildings in street_buildings.items():
            if street_id not in streets:
                continue
                
            target_street_id = streets[street_id]
            for building_id in buildings:
                all_buildings.append(building_id)
                all_street_ids[building_id] = target_street_id
        
        unique_buildings = len(all_buildings)
        print(f"Найдено {unique_buildings} уникальных зданий")
        
        # Проверяем, есть ли здания для обработки
        if not all_buildings:
            print("Не найдено зданий для обработки")
            return 0
        
        # Разбиваем на группы для параллельной обработки
        batch_size = self.building_batch_size
        batches = [all_buildings[i:i+batch_size] for i in range(0, len(all_buildings), batch_size)]
        
        # Создаем пул потоков для параллельной обработки
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Запускаем обработку каждой группы в отдельном потоке
            futures = [executor.submit(self._process_building_batch, batch, all_street_ids, batch_idx, len(batches)) 
                      for batch_idx, batch in enumerate(batches)]
            
            # Собираем результаты с отображением прогресса
            processed_ids = []
            start_time = time.time()
            completed = 0
            total = len(futures)
            
            # Инициализируем прогресс
            print_progress(completed, total, "Обработка зданий", start_time)
            
            for future in as_completed(futures):
                try:
                    batch_processed_ids = future.result()
                    processed_ids.extend(batch_processed_ids)
                except Exception as e:
                    print(f"\nОшибка при обработке группы зданий: {e}")
                
                completed += 1
                print_progress(completed, total, "Обработка зданий", start_time)
        
        # Проверяем, все ли здания были обработаны
        missing_ids = [id for id in all_buildings if id not in processed_ids]
        
        if missing_ids:
            print(f"Не удалось обработать {len(missing_ids)} зданий")
        
        total_processed = len(processed_ids)
        print(f"Всего обработано зданий: {total_processed} из {unique_buildings}")
        return total_processed
    
    def _process_building_batch(self, batch, street_ids, batch_idx, total_batches):
        """
        Обработка одной группы зданий.
        
        Args:
            batch (list): Список ID зданий для обработки
            street_ids (dict): Словарь соответствия ID зданий и ID улиц
            batch_idx (int): Индекс текущей группы
            total_batches (int): Общее количество групп
            
        Returns:
            list: Список успешно обработанных ID зданий
        """
        processed_ids = []
        
        try:
            # Формируем список ID для запроса
            id_list = ", ".join([f"'{id}'" for id in batch])
            
            # Оптимизируем запрос, используя только необходимые поля и подсказки оптимизатору
            query = f"""
            SELECT 
                h.OBJECTID, 
                h.OBJECTGUID, 
                h.HOUSENUM, 
                h.HOUSETYPE, 
                h.ADDNUM1, 
                h.ADDNUM2, 
                mh.PATH 
            FROM houses h
            STRAIGHT_JOIN mun_hierarchy mh ON h.OBJECTID = mh.OBJECTID AND mh.ISACTIVE = 1
            WHERE h.OBJECTID IN ({id_list})
            AND h.ISACTUAL = 1 AND h.ISACTIVE = 1
            """
            
            # Выполняем запрос с повторными попытками
            result = self.execute_with_retry(self.source_engine, query)
            buildings = result.fetchall() if result else []
            
            # Подготавливаем все записи для пакетной вставки
            building_records = []
            
            for building in buildings:
                building_id = str(building.OBJECTID)
                if building_id in street_ids:
                    # Формируем данные для вставки
                    building_records.append({
                        "objectid": building.OBJECTID,
                        "objectguid": building.OBJECTGUID,
                        "house_num": building.HOUSENUM or "",
                        "add_num1": building.ADDNUM1 or "",
                        "add_num2": building.ADDNUM2 or "",
                        "housetype": building.HOUSETYPE,
                        "path_octets": building.PATH,
                        "street_id": street_ids[building_id]
                    })
                    
                    # Добавляем ID в список обработанных
                    processed_ids.append(building_id)
            
            # Выполняем пакетную вставку для всех записей
            if building_records:
                with self.target_engine.begin() as connection:
                    # Временно отключаем проверку внешних ключей для ускорения вставки
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                    
                    # Используем улучшенную пакетную вставку с лучшей производительностью
                    insert_query = """
                    INSERT INTO buildings 
                    (objectid, objectguid, house_num, add_num1, add_num2, housetype, path_octets, street_id)
                    VALUES (:objectid, :objectguid, :house_num, :add_num1, :add_num2, :housetype, :path_octets, :street_id)
                    ON DUPLICATE KEY UPDATE 
                    objectguid = VALUES(objectguid), 
                    house_num = VALUES(house_num), 
                    add_num1 = VALUES(add_num1), 
                    add_num2 = VALUES(add_num2),
                    housetype = VALUES(housetype), 
                    path_octets = VALUES(path_octets), 
                    street_id = VALUES(street_id)
                    """
                    
                    # Вставляем данные пакетами по 1000 записей для лучшей производительности
                    batch_size = 1000
                    for i in range(0, len(building_records), batch_size):
                        batch_to_insert = building_records[i:i+batch_size]
                        connection.execute(text(insert_query), batch_to_insert)
                    
                    # Включаем проверку внешних ключей обратно
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
            
            # Убираем вывод информации о прогрессе, чтобы не перебивать прогресс-бар
            # if batch_idx == 0 or batch_idx == total_batches - 1 or batch_idx % 3 == 0:
            #     print(f"Обработано {len(processed_ids)} зданий в группе {batch_idx+1}/{total_batches}")
            
        except Exception as e:
            print(f"\nОшибка при обработке группы зданий {batch_idx+1}: {e}")
        
        return processed_ids
    
    def _process_missing_localities(self, missing_ids):
        """
        Обработка пропущенных населенных пунктов с использованием альтернативного подхода.
        
        Args:
            missing_ids (list): Список ID населенных пунктов, которые не были обработаны
        """
        print(f"Попытка обработать {len(missing_ids)} пропущенных населенных пунктов")
        
        # Разбиваем на группы для обработки
        batch_size = self.locality_batch_size
        batches = [missing_ids[i:i+batch_size] for i in range(0, len(missing_ids), batch_size)]
        
        total_processed = 0
        start_time = time.time()
        
        # Используем свой механизм отображения прогресса вместо tqdm
        for batch_idx, batch in enumerate(batches):
            print_progress(batch_idx, len(batches), "Обработка пропущенных населенных пунктов", start_time)
            
            try:
                # Формируем список ID для запроса
                id_list = ", ".join([f"'{id}'" for id in batch])
                
                # Сначала ищем в mun_hierarchy для получения путей
                mh_query = f"""
                SELECT mh.OBJECTID, mh.PATH 
                FROM mun_hierarchy mh
                WHERE mh.OBJECTID IN ({id_list})
                AND mh.ISACTIVE = 1
                """
                
                mh_result = self.execute_with_retry(self.source_engine, mh_query)
                mh_records = mh_result.fetchall() if mh_result else []
                
                # Для каждого найденного пути пытаемся найти информацию
                localities_to_insert = []
                
                for mh_record in mh_records:
                    # Сначала пытаемся найти в addr_obj
                    ao_query = f"""
                    SELECT ao.OBJECTID, ao.OBJECTGUID, ao.NAME, ao.TYPENAME
                    FROM addr_obj ao
                    WHERE ao.OBJECTID = {mh_record.OBJECTID}
                    """
                    
                    ao_result = self.execute_with_retry(self.source_engine, ao_query)
                    ao_records = ao_result.fetchall() if ao_result else []
                    
                    if ao_records:
                        # Нашли запись в addr_obj
                        for ao_record in ao_records:
                            localities_to_insert.append({
                                "objectid": ao_record.OBJECTID,
                                "objectguid": ao_record.OBJECTGUID,
                                "name": ao_record.NAME,
                                "typename": ao_record.TYPENAME,
                                "path_octets": mh_record.PATH
                            })
                    else:
                        # Не нашли в addr_obj, используем данные из mun_hierarchy
                        # Ищем родительский объект для получения типа
                        parent_path = ".".join(mh_record.PATH.split(".")[:-1])
                        parent_query = f"""
                        SELECT ao.NAME, ao.TYPENAME
                        FROM addr_obj ao
                        JOIN mun_hierarchy mh ON ao.OBJECTID = mh.OBJECTID
                        WHERE mh.PATH = '{parent_path}'
                        LIMIT 1
                        """
                        
                        parent_result = self.execute_with_retry(self.source_engine, parent_query)
                        parent_record = parent_result.fetchone() if parent_result else None
                        
                        # Используем информацию из родительского объекта или значения по умолчанию
                        typename = "неизвестно"
                        if parent_record:
                            # Определяем тип на основе родительского объекта
                            if "город" in parent_record.TYPENAME.lower():
                                typename = "микрорайон"
                            elif "район" in parent_record.TYPENAME.lower():
                                typename = "поселок"
                            else:
                                typename = "населенный пункт"
                        
                        # Ищем имя объекта в других таблицах
                        name_query = f"""
                        SELECT NAME FROM addr_obj 
                        WHERE OBJECTID = {mh_record.OBJECTID}
                        LIMIT 1
                        """
                        
                        name_result = self.execute_with_retry(self.source_engine, name_query)
                        name_record = name_result.fetchone() if name_result else None
                        
                        name = name_record.NAME if name_record else f"Объект {mh_record.OBJECTID}"
                        
                        # Генерируем GUID если его нет
                        guid_query = f"""
                        SELECT OBJECTGUID FROM addr_obj 
                        WHERE OBJECTID = {mh_record.OBJECTID}
                        LIMIT 1
                        """
                        
                        guid_result = self.execute_with_retry(self.source_engine, guid_query)
                        guid_record = guid_result.fetchone() if guid_result else None
                        
                        objectguid = guid_record.OBJECTGUID if guid_record else f"00000000-0000-0000-0000-{mh_record.OBJECTID:012d}"
                        
                        localities_to_insert.append({
                            "objectid": mh_record.OBJECTID,
                            "objectguid": objectguid,
                            "name": name,
                            "typename": typename,
                            "path_octets": mh_record.PATH
                        })
                
                # Сохраняем результаты в целевую БД
                if localities_to_insert:
                    with self.target_engine.begin() as connection:
                        for locality in localities_to_insert:
                            insert_query = """
                            INSERT INTO localities (objectid, objectguid, name, typename, path_octets)
                            VALUES (:objectid, :objectguid, :name, :typename, :path_octets)
                            ON DUPLICATE KEY UPDATE 
                            objectguid = :objectguid, name = :name, typename = :typename, path_octets = :path_octets
                            """
                            connection.execute(text(insert_query), locality)
                    
                    total_processed += len(localities_to_insert)
            
            except Exception as e:
                print(f"\nОшибка при обработке пропущенных населенных пунктов в группе {batch_idx+1}: {e}")
        
        # Показываем завершение процесса
        print_progress(len(batches), len(batches), "Обработка пропущенных населенных пунктов", start_time)
        print(f"Всего дополнительно обработано населенных пунктов: {total_processed}")
        return total_processed
    
    def _process_locality_batch(self, batch, batch_idx, total_batches):
        """
        Обработка одной группы населенных пунктов.
        
        Args:
            batch (list): Список ID населенных пунктов для обработки
            batch_idx (int): Индекс текущей группы
            total_batches (int): Общее количество групп
            
        Returns:
            list: Список успешно обработанных ID населенных пунктов
        """
        processed_ids = []
        
        try:
            # Формируем список ID для запроса
            id_list = ", ".join([f"'{id}'" for id in batch])
            
            # Получаем данные из addr_obj по objectid
            query = f"""
            SELECT ao.OBJECTID, ao.OBJECTGUID, ao.NAME, ao.TYPENAME, mh.PATH 
            FROM addr_obj ao
            JOIN mun_hierarchy mh ON ao.OBJECTID = mh.OBJECTID
            WHERE ao.OBJECTID IN ({id_list})
            AND ao.ISACTUAL = 1 AND ao.ISACTIVE = 1
            """
            
            # Выполняем запрос с повторными попытками
            result = self.execute_with_retry(self.source_engine, query)
            localities = result.fetchall() if result else []
            
            # Сохраняем результаты в целевую БД
            with self.target_engine.begin() as connection:
                for locality in localities:
                    insert_query = """
                    INSERT INTO localities (objectid, objectguid, name, typename, path_octets)
                    VALUES (:objectid, :objectguid, :name, :typename, :path_octets)
                    ON DUPLICATE KEY UPDATE 
                    objectguid = :objectguid, name = :name, typename = :typename, path_octets = :path_octets
                    """
                    connection.execute(text(insert_query), {
                        "objectid": locality.OBJECTID,
                        "objectguid": locality.OBJECTGUID,
                        "name": locality.NAME,
                        "typename": locality.TYPENAME,
                        "path_octets": locality.PATH
                    })
                    
                    # Добавляем ID в список обработанных
                    processed_ids.append(str(locality.OBJECTID))
            
            # Убираем вывод информации о прогрессе, чтобы не перебивать прогресс-бар
            # if batch_idx == 0 or batch_idx == total_batches - 1 or batch_idx % 5 == 0:
            #     print(f"Обработано {len(processed_ids)} населенных пунктов в группе {batch_idx+1}/{total_batches}")
            
        except Exception as e:
            print(f"\nОшибка при обработке группы населенных пунктов {batch_idx+1}: {e}")
        
        return processed_ids 