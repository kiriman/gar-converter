from converter import GarConverter

def main():
    """
    Основная функция для запуска конвертера ГАР.
    """
    print("Запуск конвертера данных ГАР...")
    
    # Создаем экземпляр конвертера
    converter = GarConverter()
    
    # Создаем схему в целевой БД, если ее нет
    print("Создание схемы в целевой БД...")
    if not converter.create_target_schema():
        print("Ошибка при создании схемы.")
        return False
    
    # Очищаем таблицы перед конвертацией
    print("Очистка целевой БД...")
    if not converter.clear_target_db():
        print("Ошибка при очистке БД.")
        return False
    
    # Заполняем справочники
    print("Заполнение справочников...")
    if not converter.fill_dictionaries():
        print("Ошибка при заполнении справочников.")
        return False
    
    # Запускаем конвертацию
    print("Запуск конвертации...")
    result = converter.run_conversion()
    
    if result:
        print("Конвертация успешно завершена!")
    else:
        print("Конвертация завершилась с ошибками")
    
    return result

if __name__ == "__main__":
    main() 