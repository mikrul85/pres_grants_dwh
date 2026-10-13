"""
ETL-процесс загрузки данных «Проекты конкурса Фонда президентских грантов»
в хранилище по методологии Кимбалла (чистая звезда) на PostgreSQL.
Финальная версия: модель по er_diagram.drawio - 8 измерений, первичный ключ
факта составной из всех внешних ключей; SCD Type 1 для dim_org, Type 0 для
остальных измерений (см. «Типы медленно меняющихся измерений.docx»).

Этапы:
  init_db       - создание схем и таблиц (ddl.sql)
  extract_clean - извлечение сырого CSV, нормализация, чистый слой (flat-файл)
  load_stage    - COPY чистого CSV в стейджинговую таблицу
  load_dims     - заполнение 8 измерений (surrogate key через SERIAL)
  load_fact     - заполнение таблицы фактов
  build_marts   - создание витрин (схема mart) для BI

Запуск целиком:  python3 etl.py
Запуск этапа:    python3 etl.py extract_clean
"""
import json
import os
import sys

import pandas as pd
import psycopg2

# папка проекта
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# конфиг: подключение к БД, схемы, пути
with open(os.path.join(BASE_DIR, 'config.json')) as f:
    CONFIG = json.load(f)

# имена схем хранилища: стейдж, звезда, витрины
STG = CONFIG['schemas']['stage']
DWH = CONFIG['schemas']['dwh']
MART = CONFIG['schemas']['mart']
# сколько строк CSV читать за раз (файл больше 2 ГБ, целиком в память не берем)
CHUNKSIZE = CONFIG['chunksize']

# путь к сырому CSV
RAW_CSV = os.environ.get('PRES_GRANTS_RAW_CSV',
                         os.path.join(BASE_DIR, CONFIG['paths']['raw_csv']))
# путь к чистому слою - результату extract_clean
CLEAN_CSV = os.path.join(BASE_DIR, CONFIG['paths']['clean_csv'])


def get_conn():
    """Возвращает новое подключение к PostgreSQL из настроек конфига."""
    db = CONFIG['db']
    return psycopg2.connect(
        dbname=db['dbname'], user=db['user'], password=db['password'],
        host=db['host'], port=db['port'])


def _run_sql_file(cur, path):
    """Читает sql-файл, подставляет имена схем и выполняет.

    cur  - курсор psycopg2, в котором выполняется sql
    path - путь к sql-файлу с плейсхолдерами {stg}, {dwh}, {mart}
    """
    with open(path) as f:
        sql = f.read()
    # плейсхолдеры в sql-файле заменяем реальными именами схем из конфига
    sql = sql.replace('{stg}', STG).replace('{dwh}', DWH).replace('{mart}', MART)
    cur.execute(sql)


def init_db():
    """Создает схемы и таблицы из ddl.sql (IF NOT EXISTS, можно звать повторно)."""
    conn = get_conn()
    cur = conn.cursor()
    _run_sql_file(cur, os.path.join(BASE_DIR, 'ddl.sql'))
    conn.commit()
    conn.close()
    print('init_db: схемы и таблицы созданы')


def _normalize_chunk(chunk):
    """Приводит сырые строковые поля источника к рабочим типам.

    chunk - часть сырого CSV (DataFrame), прочитанная pandas одним чанком;
            возвращается новый DataFrame, исходный не изменяется
    """
    chunk = chunk.copy()  # копия, чтобы не менять исходный DataFrame у вызывающего кода

    # строковые поля - убираем пробелы по краям
    for col in ['name', 'contest', 'direction', 'region', 'org', 'inn', 'ogrn',
                'address', 'web_site', 'req_num', 'link', 'okato', 'oktmo',
                'level', 'pj_geo', 'target_groups']:
        chunk[col] = chunk[col].astype('string').str.strip()

    # год в источнике в виде '2026.0'
    # errors='coerce': значение, которое не удалось превратить в число, станет NaN,
    # а весь этап не упадет; Int64 - целочисленный тип pandas, который умеет хранить NaN
    chunk['year'] = pd.to_numeric(chunk['year'], errors='coerce').astype('Int64')

    # признак победы: строки 'True'/'False' в булево
    chunk['winner'] = chunk['winner'].map({'True': True, 'False': False})

    # числовые поля
    for col in ['rate', 'money_req_grant', 'cofunding', 'total_money']:
        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    # даты: 'YYYY-MM-DD' и 'YYYY-MM-DD 00:00:00'
    for col in ['date_req', 'implem_start', 'implem_end']:
        # .dt.date отбрасывает время, оставляем только дату
        chunk[col] = pd.to_datetime(chunk[col], errors='coerce').dt.date

    # без номера заявки строка бесполезна (req_num - естественный ключ dim_application)
    chunk = chunk[chunk['req_num'].notna() & (chunk['req_num'] != '')]

    # невалидные ИНН заменяем заглушкой, иначе факт не джойнится с dim_org
    chunk.loc[~chunk['inn'].fillna('').str.fullmatch(r'\d{10}|\d{12}'), 'inn'] = 'Не указан'

    # пропуски в полях справочников закрываем заглушкой: у факта составной PK
    # из всех FK, поэтому каждый ключ обязан находиться
    for col in ['region', 'contest', 'direction']:
        chunk[col] = chunk[col].fillna('Не указан')

    return chunk


def extract_clean():
    """Сырой CSV -> нормализованный clean_grants.csv (читаем чанками, файл 2+ ГБ)"""
    os.makedirs(os.path.dirname(CLEAN_CSV), exist_ok=True)
    total = 0  # счетчик записанных строк
    first = True          # флаг первого чанка: ему пишем заголовок CSV

    # sep=';' - разделитель источника; dtype=str - все поля читаем как текст,
    # типы проставим сами в _normalize_chunk; keep_default_na=False - не считать
    # строки вроде 'NA' и 'null' пропусками, пропуск у нас только пустая строка
    reader = pd.read_csv(RAW_CSV, sep=';', dtype=str, chunksize=CHUNKSIZE,
                         keep_default_na=False, na_values=[''])
    for chunk in reader:
        chunk = _normalize_chunk(chunk)

        # Пишем данные в чистый слой
        # first=True: перезаписываем файл с заголовком; дальше - дописываем в конец
        chunk.to_csv(CLEAN_CSV, mode='w' if first else 'a', header=first,
                     index=False, sep=';')
        first = False
        total += len(chunk)
        print(f'extract_clean: обработано {total} строк', flush=True)

    print(f'extract_clean: итого {total} строк')


def load_stage():
    """Полностью очищаем таблицу стейджа и заново заливаем
       в нее весь чистый файл одной командой массовой загрузки"""
    conn = get_conn()
    cur = conn.cursor()

    # стейдж очищаем, чтобы повторный запуск не удваивал данные
    cur.execute(f'TRUNCATE TABLE {STG}.stg_grant;')
    with open(CLEAN_CSV) as f:
        # из первой строки файла берем имена колонок для COPY
        header = f.readline().strip().split(';')
        cols = ','.join(header)
        # FROM STDIN: PostgreSQL читает данные из переданного файлового объекта f
        cur.copy_expert(
            f"COPY {STG}.stg_grant ({cols}) FROM STDIN "
            f"WITH (FORMAT csv, DELIMITER ';', NULL '')",
            f)

    conn.commit()
    cur.execute(f'SELECT COUNT(*) FROM {STG}.stg_grant;')
    print(f'load_stage: загружено {cur.fetchone()[0]} строк')
    conn.close()


def load_dims():
    """Заполняет 8 измерений из стейджа. Повторный запуск не дублирует строки.

    SCD: dim_org - Type 1 (актуальное значение перезаписывается),
    остальные измерения - Type 0 (значение фиксируется и не меняется).
    """
    conn = get_conn()
    cur = conn.cursor()

    # строка-заглушка календаря: ключ 0 для заявок без распознаваемой даты,
    # чтобы составной PK факта не получал NULL (в текущем датасете не используется)
    cur.execute(f'''
        INSERT INTO {DWH}.dim_date
            (date_reg_key, full_date, year, quarter, month, month_name,
             day, day_of_week, is_weekend)
        VALUES (0, NULL, 0, 0, 0, 'Не указана', 0, 0, false)
        ON CONFLICT DO NOTHING;
    ''')

    # календарь дат подачи: 2016-2028 с запасом, ключ ГГГГММДД
    # generate_series сам порождает последовательность дат, отдельный источник не нужен;
    # ON CONFLICT DO NOTHING - при повторном запуске существующие даты пропускаются
    cur.execute(f'''
        INSERT INTO {DWH}.dim_date
            (date_reg_key, full_date, year, quarter, month, month_name,
             day, day_of_week, is_weekend)
        SELECT
            to_char(d, 'YYYYMMDD')::int,
            d::date,
            EXTRACT(YEAR FROM d)::int,
            EXTRACT(QUARTER FROM d)::int,
            EXTRACT(MONTH FROM d)::int,
            to_char(d, 'TMMonth'),
            EXTRACT(DAY FROM d)::int,
            EXTRACT(ISODOW FROM d)::int,
            EXTRACT(ISODOW FROM d) >= 6
        FROM generate_series('2016-01-01', '2028-12-31', interval '1 day') d
        ON CONFLICT DO NOTHING;
    ''')
    print(f'load_dims: dim_date +{cur.rowcount}')

    # словарь периодов реализации: одна строка на уникальную пару дат старта и финиша;
    # ключи дат - в формате ГГГГММДД, пропуск даты = 0 (неизвестная дата календаря)
    cur.execute(f'''
        INSERT INTO {DWH}.dim_implementation (implem_start_key, implem_end_key)
        SELECT DISTINCT
            COALESCE(to_char(implem_start, 'YYYYMMDD')::int, 0),
            COALESCE(to_char(implem_end, 'YYYYMMDD')::int, 0)
        FROM {STG}.stg_grant
        ON CONFLICT (implem_start_key, implem_end_key) DO NOTHING;
    ''')
    print(f'load_dims: dim_implementation +{cur.rowcount}')

    # описание заявки: все авторские тексты, связь с фактом 1:1;
    # DISTINCT ON (req_num) страхует от повторов номера в стейдже
    cur.execute(f'''
        INSERT INTO {DWH}.dim_application
            (req_num, link, name, description, goal, tasks,
             soc_signif, pj_geo, target_groups)
        SELECT DISTINCT ON (req_num)
            req_num, link, name, description, goal, tasks,
            soc_signif, pj_geo, target_groups
        FROM {STG}.stg_grant
        ORDER BY req_num
        ON CONFLICT (req_num) DO NOTHING;
    ''')
    print(f'load_dims: dim_application +{cur.rowcount}')

    # организация: одна строка на ИНН, SCD Type 1 - атрибуты берем из самой
    # свежей заявки организации (ORDER BY date_req DESC), при повторной
    # загрузке новые значения перезаписывают старые (DO UPDATE), история не ведется
    cur.execute(f'''
        INSERT INTO {DWH}.dim_org
            (inn, org_name, ogrn, address, web_site, okato, oktmo, geo_level)
        SELECT DISTINCT ON (inn)
            inn, org, ogrn, address, web_site, okato, oktmo, level
        FROM {STG}.stg_grant
        ORDER BY inn, date_req DESC NULLS LAST
        ON CONFLICT (inn) DO UPDATE SET
            org_name = EXCLUDED.org_name,
            ogrn = EXCLUDED.ogrn,
            address = EXCLUDED.address,
            web_site = EXCLUDED.web_site,
            okato = EXCLUDED.okato,
            oktmo = EXCLUDED.oktmo,
            geo_level = EXCLUDED.geo_level,
            processed_at = NOW();
    ''')
    print(f'load_dims: dim_org +{cur.rowcount}')

    # конкурс: название + год (год у одного конкурса не меняется, берем максимум)
    cur.execute(f'''
        INSERT INTO {DWH}.dim_contest (contest, year)
        SELECT contest, MAX(year)
        FROM {STG}.stg_grant
        GROUP BY contest
        ON CONFLICT (contest) DO NOTHING;
    ''')
    print(f'load_dims: dim_contest +{cur.rowcount}')

    # простые измерения-справочники: DISTINCT отбирает уникальные значения
    # (направление, регион), суррогатный ключ BIGSERIAL проставляется сам
    for dim, col in [('dim_direction', 'direction'),
                     ('dim_region', 'region')]:
        cur.execute(f'''
            INSERT INTO {DWH}.{dim} ({col})
            SELECT DISTINCT {col} FROM {STG}.stg_grant
            ON CONFLICT ({col}) DO NOTHING;
        ''')
        print(f'load_dims: {dim} +{cur.rowcount}')

    # мини-измерение результата: справочник закрыт, две строки
    cur.execute(f'''
        INSERT INTO {DWH}.dim_winner_status (winner_status)
        VALUES ('Победа'), ('Не победа')
        ON CONFLICT (winner_status) DO NOTHING;
    ''')
    print(f'load_dims: dim_winner_status +{cur.rowcount}')

    conn.commit()
    conn.close()


def load_fact():
    """Заполняет факты: стейдж джойнится с 8 измерениями по естественным ключам.

    PK факта - составной из всех FK, поэтому все ключи NOT NULL:
    отсутствующая дата попадает в строку-заглушку (date_reg_key = 0),
    справочники заполнены из того же стейджа и совпадают всегда.
    """
    conn = get_conn()
    cur = conn.cursor()

    # winner NULL (47 строк датасета) трактуем как 'Не победа';
    # ON CONFLICT DO NOTHING по составному PK - повторный запуск не плодит дубликаты
    cur.execute(f'''
        INSERT INTO {DWH}.fact_grant (
            application_key, date_reg_key, implem_date_key, org_key,
            contest_key, direction_key, region_key, winner_key,
            rate, money_req_grant, cofunding, total_money
        )
        SELECT
            a.application_key,
            COALESCE(dk.date_reg_key, 0),
            imp.implem_date_key,
            o.org_key,
            c.contest_key,
            d.direction_key,
            r.region_key,
            w.winner_key,
            s.rate, s.money_req_grant, s.cofunding, s.total_money
        FROM {STG}.stg_grant s
        JOIN {DWH}.dim_application a ON a.req_num = s.req_num
        LEFT JOIN {DWH}.dim_date dk  ON dk.full_date = s.date_req
        LEFT JOIN {DWH}.dim_implementation imp
               ON imp.implem_start_key = COALESCE(to_char(s.implem_start, 'YYYYMMDD')::int, 0)
              AND imp.implem_end_key = COALESCE(to_char(s.implem_end, 'YYYYMMDD')::int, 0)
        LEFT JOIN {DWH}.dim_org o    ON o.inn = s.inn
        LEFT JOIN {DWH}.dim_contest c ON c.contest = s.contest
        LEFT JOIN {DWH}.dim_direction d ON d.direction = s.direction
        LEFT JOIN {DWH}.dim_region r ON r.region = s.region
        LEFT JOIN {DWH}.dim_winner_status w
               ON w.winner_status = CASE WHEN s.winner THEN 'Победа' ELSE 'Не победа' END
        ON CONFLICT DO NOTHING;
    ''')
    print(f'load_fact: fact_grant +{cur.rowcount}')

    # контроль: сколько заявок стейджа не попало в факты
    cur.execute(f'''
        SELECT COUNT(*) FROM {STG}.stg_grant s
        LEFT JOIN {DWH}.dim_application a ON a.req_num = s.req_num
        LEFT JOIN {DWH}.fact_grant f ON f.application_key = a.application_key
        WHERE f.application_key IS NULL;
    ''')
    print(f'load_fact: не загружено заявок {cur.fetchone()[0]}')

    conn.commit()
    conn.close()


def build_marts():
    """Создает витрины из marts.sql (представления поверх звезды)."""
    conn = get_conn()
    cur = conn.cursor()
    _run_sql_file(cur, os.path.join(BASE_DIR, 'marts.sql'))
    conn.commit()
    conn.close()
    print('build_marts: витрины созданы')

PIPELINE = [init_db, extract_clean, load_stage, load_dims, load_fact, build_marts]

if __name__ == '__main__':
    # без аргументов выполняем весь PIPELINE; с аргументами - только названные этапы
    steps = sys.argv[1:] or [f.__name__ for f in PIPELINE]
    for step in PIPELINE:
        if step.__name__ in steps:
            step()
