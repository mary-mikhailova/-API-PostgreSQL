import requests
import json
import logging
from datetime import datetime, timedelta 
from logging.handlers import TimedRotatingFileHandler
import os
import psycopg2
import gspread
from google.oauth2.service_account import Credentials
import smtplib
import ssl
from email.message import EmailMessage


#ОРГАНИЗАЦИЯ ЛОГИРОВАНИЯ
#проверка существования директории. Если нет-создать
if not os.path.exists('logs'):
    os.makedirs('logs')

#создание и настройка File Handler (хранит только 3 ахвные файла - остальные удалит)
file_handler=TimedRotatingFileHandler('logs/app_log.txt', when='midnight', interval=1, backupCount=3)
file_handler.setLevel(logging.INFO)

#формат логов с временной меткой
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)

#создание логера согласно формату
logger=logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)            


#ОТПРАВКА ЗАПРСА К API

#данные для подключния к API

#в задаче часовой пояс -3 часа МСК. Данные будут собираться ежедневно за прошлый день
hours = 3
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
start_datetime = datetime.strptime(f'{yesterday} 00:00:00.000000', "%Y-%m-%d %H:%M:%S.%f") + timedelta(hours=hours)
end_datetime = datetime.strptime(f'{yesterday} 23:59:59.999999', "%Y-%m-%d %H:%M:%S.%f") + timedelta(hours=hours)
start = (start_datetime).strftime("%Y-%m-%d %H:%M:%S.%f")
end = (end_datetime).strftime("%Y-%m-%d %H:%M:%S.%f")

api_url = 'https://b2b.itresume.ru/api/statistics'
params = {
    'client': 'Skillfactory',
    'client_key': 'M2MGWS',
    'start': start,
    'end': end
  }

#выгрузка данных
def get_api_data(api_url, params):
    #логирование
    logging.info('Скачивание данных началось')
    r = requests.get(api_url, params=params)
    status_code=r.status_code
    #при ошибке доступа к API: логирование ошибки.
    if status_code !=200:
        logging.error(f'Ошибка доступа к API {api_url}: {status_code}')
    else:
        logging.info('Скачивание данных завершилось')
    data=r.json()
    return data

#шаблон для валидации данных
def validate_and_assign(key, dict, client_id):
    #проверяем, что значения типа строка и не пустые (за ислючением уникального токена клиента-отсутвует во всех данных)
    if isinstance(dict[key], str) and (dict[key] or key=='oauth_consumer_key'):
        return dict[key]
    #логирование неподходящих данных
    else:
        if not isinstance(dict[key], str):
            return  logging.error(f'Неверный тип данных для {key}: {type(dict[key])}. Ожидается str. Клиент: {client_id}')
        elif not dict[key] and key!='oauth_consumer_key':
            return logging.error(f'{key} пустой. Ожидается значение. Клиент: {client_id}')

#валидация данных
def prepare_data(data):
    validated_data=[]
    for attempt in data:
        try:
            #распаковывка passback_params из json строки
            passback_params = json.loads(attempt['passback_params'].replace("'", "\""))
            clear_attempt={}
            #валидация данных по шаблону
            clear_attempt['user_id'] = validate_and_assign('lti_user_id', attempt, attempt['lti_user_id'])
            clear_attempt['oauth_consumer_key'] = validate_and_assign('oauth_consumer_key', passback_params, attempt['lti_user_id'])
            clear_attempt['lis_result_sourcedid'] = validate_and_assign('lis_result_sourcedid', passback_params, attempt['lti_user_id'])
            clear_attempt['lis_outcome_service_url']=validate_and_assign('lis_outcome_service_url', passback_params, attempt['lti_user_id'])

            #проверка, что is_correct имеет ожидаемое значение
            if attempt['is_correct'] in (0, 1, None):
                clear_attempt['is_correct']=attempt['is_correct']
            else: 
                logging.error(f'Неверный тип данных для is_correct: {attempt["is_correct"]}. Ожидается 0, 1 или None. Клиент: {attempt["lti_user_id"]}')
                continue
            
            #валидация по шаблону
            clear_attempt['attempt_type'] = validate_and_assign('attempt_type', attempt, attempt['lti_user_id'])

            #попытка конвертировать строку в дату
            clear_attempt['created_at']=datetime.strptime(attempt['created_at'],'%Y-%m-%d %H:%M:%S.%f')
           
            #сбор попыток с непустыми значениями (за исключением is_correct и oauth_consumer_key - они могут быть пустыми)
            if all(value is not None for key, value in clear_attempt.items() if key not in ('is_correct', 'oauth_consumer_key')):
                validated_data.append(clear_attempt)

        #логирование исключений: -в passback_params нет нужного ключа, -не декодировать JSON, -дата подана в неправилном формате
        except KeyError as err:
            logging.error(f'Отсутствует ключ {err} в passback_params для клиента {attempt['lti_user_id']}')
        except json.JSONDecodeError as err:
            logging.error(f'Ошибка декодирования JSON: {err} для клиент {attempt['lti_user_id']}')
        except ValueError as err:
            logging.error(f'Ошибка в обработке даты: {err} для клиента {attempt["lti_user_id"]}')
    return validated_data


#РАБОТА С БД (РАЗВЕРНУТА ЛОКАЛЬНО)

#данные для подключния к БД
DATABASE="Python_project"
USER="postgres"
PASSWORD="12345"
HOST="localhost"
PORT="5433"

#класс для подключения и загрузки данных в БД
class Database:
    #шаблон Singleton(возможно только 1 подглючение к БД)
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance=super().__new__(cls)
        return cls.instance
    
    #cоздание подключения
    def __init__(self, host, port, database, user, password):
        try: 
            self.connection=psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            )
            logging.info(f'Подключение к БД {database} установлено')
            self.connection.autocommit=True
        except Exception as err:
            logging.error(f'Ошибка подключения к БД {database}: {err}')
    
    #создание таблицы в БД, если она не существует
    def create_table(self, table_name):
        try:
            cursor=self.connection.cursor()
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            oauth_consumer_key VARCHAR(255),
            lis_result_sourcedid TEXT,
            lis_outcome_service_url TEXT,
            is_correct BOOLEAN,
            attempt_type VARCHAR(10),
            created_at TIMESTAMP NOT NULL
            );
            """
            cursor.execute(create_table_query)
            logging.info(f'Таблица {table_name} успешно создана или уже существует')

            # Создание уникального индекса. Так одинаковые запросы не будут дублироваться в БД 
            create_unique_index_query = f"""
            CREATE UNIQUE INDEX IF NOT EXISTS unique_user_date_idx
            ON {table_name} (user_id, created_at);
            """
            cursor.execute(create_unique_index_query)
            logging.info(f'Уникальный индекс для {table_name} успешно создан или уже существует')

        except Exception as err:
            logging.error(f'Ошибка при создании талблицы {table_name}: {err}')


    #закгрузка данных в БД
    def load_query(self, tablename, validated_data):
        try:
            with self.connection.cursor() as cursor:
                #перед загрузкой данных, очищение таблицы, чтобы в ней оставлись только данные последнего запроса.
                #Кажется нет смысла хранить резульаты всех выгрузок с API - если их в любой момент можно достать повторно
                #+далее будет удобнее делать SQL-запрос, не будет необходимости допполнительно фильровать данные для аналитики
                trancate_query=f"""TRUNCATE TABLE {tablename}"""
                cursor.execute(trancate_query)
                logging.info(f'Таблица {tablename} успешно очищена')
                for row in validated_data:
                    # Обработка значения is_correct
                    is_correct_value = row.get('is_correct')
                    if is_correct_value is not None:
                        is_correct_value = bool(is_correct_value)  # Преобразование 1 и 0 в True и False

                    values = (
                        row.get('user_id'),
                        row.get('oauth_consumer_key'),
                        row.get('lis_result_sourcedid'),
                        row.get('lis_outcome_service_url'),
                        is_correct_value,
                        row.get('attempt_type'),
                        row.get('created_at'),
                    )
                    insert_query = f"""
                    INSERT INTO {tablename}(user_id, oauth_consumer_key, lis_result_sourcedid, 
                    lis_outcome_service_url, is_correct, attempt_type, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, created_at) DO NOTHING;
                    """
                    cursor.execute(insert_query, values)
                logging.info(f'Данные успешно загружены в таблицу {tablename}')
        except Exception as err:
            logging.error(f'Ошибка при загрузке данных в таблицу {tablename}: {err}')

def upload_to_google_sheets(results, spreadsheet_name, worksheet_name):
    #Область доступа для API Google Sheets и Google Drive
    scopes = ["https://www.googleapis.com/auth/spreadsheets", 
              "https://www.googleapis.com/auth/drive"]
    
    #загрузка учетных данных из локальтного файла для авторизации
    credentials = Credentials.from_service_account_file('/Users/marrymek/Desktop/Simulative/Python_project/leafy-tractor-477013-e5-7cac74545a5e.json', scopes=scopes)

    #авторизация
    gc = gspread.authorize(credentials)

    try:
        #открытие таблицы
        spreadsheet=gc.open(spreadsheet_name)
        #выбор листа
        worksheet=spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        logging.error(f"Таблица '{spreadsheet_name}' не найдена.")
        return
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"Лист '{worksheet_name}' не найден.")
        return
    
    #очистка аналитики предыдущей выгрузки
    worksheet.clear()

    #форматирование заголовков
    headers = ['Дата отчета', 'Показатель', 'Значение']

    if len(worksheet.get_all_values()) == 0:
        worksheet.append_row(headers)

    values= [headers]+[[yesterday, row[0],row[1]] for row in results]

    #добление данных
    worksheet.append_rows(values)
    logging.info(f'Данные успешно загружены в Google Таблицу: {spreadsheet_name} -> {worksheet_name}')

def main():
    try:
        # Получение сырых данных через API
        raw_data = get_api_data(api_url, params)
        
        # Валидация и подготовка данных
        validated_data = prepare_data(raw_data)
        
        # Создание подключения к базе данных
        db = Database(HOST, PORT, DATABASE, USER, PASSWORD)
        
        # Обработка и загрузка данных
        db.create_table('grader')
        db.load_query('grader', validated_data)

        # Выполнение SQL-запроов
        with db.connection.cursor() as cursor:
            query = """
            WITH all_submits AS (
                SELECT user_id, is_correct, attempt_type
                FROM grader g 
                WHERE attempt_type='submit'
            )
            SELECT 'Количество попыток (submit)' as "Показатель",
            count(user_id) as "Значение"
            FROM all_submits
            union all
            select 'Количество успешных попыток (submit)' as "Показатель", 
            count(case when is_correct=true then user_id end) as "Значение"
            from all_submits
            union all
            select 'Количество уникальных юзеров' as "Показатель", 
            count(distinct user_id) as "Значение"
            from grader;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            #выгрузка результата
            upload_to_google_sheets(results,'Агрегированные данные за день', 'Лист1')

    except Exception as err:
        logging.error(f'Ошибка в процессе выполнения: {err}')
    finally:
        if db.connection:
            db.connection.close()
            logging.info('Подключение к БД закрыто')

main()

def send_mail():
    try:
        context = ssl.create_default_context()
        msg = EmailMessage()

        subject = "Выгрузка grades завершена"
        message = """Коллеги, привет!\n\nДанные по успеваемости студентов за последнений день обновлены в БД.\n Акутуальная аналитика в табличке https://docs.google.com/spreadsheets/d/1ZLsHi8aYZqz2LneJd2I7LdYoKR-kDTKBLp9o6BVFgDM/edit?gid=0#gid=0. 
        """
        msg.set_content(message)
        From = 'marimik@mail.ru'
        To = 'marimik@mail.ru'
        
        msg['Subject'] = subject
        msg['From'] = From
        msg['To'] = To
        smtp_server='smtp.mail.ru'
        port='465'

        sender_email='marimik@mail.ru'

        # Получение пароля из переменной окружения
        password = os.getenv("send_email_for_python_project")

        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.send_message(msg)
    except Exception as err:
        logging.error(f'Ошибка при отправке письма: {err}')

send_mail()