# pres_grants_dwh

Хранилище данных по методологии Кимбалла (чистая звезда) для ВКР.
Финальная версия: модель соответствует `Диаграммы/er_diagram.drawio`,
типы медленно меняющихся измерений - по документу
«Типы медленно меняющихся измерений.docx», слои пайплайна - по
`Диаграммы/dwh_layers.drawio`.

Датасет: «Проекты конкурса Фонда президентских грантов с 2017 года»
(больше 190 000 заявок, 29 атрибутов), PostgreSQL, ETL на Python,
оркестрация Airflow.

## Архитектура и слои (по dwh_layers.drawio)

| Слой | Где живет | Содержимое |
|---|---|---|
| Источник | сайт ФПГ | CSV-выгрузка: больше 190 тыс. заявок, 29 атрибутов |
| Сырой (raw) | `data/raw/data_pres_grants_114.csv` | копия исходного файла без изменений |
| Чистый (clean) | `data/clean_grants.csv` | результат `extract_clean`: нормализация типов, чтение чанками |
| Стейджинг (stg) | схема `grants_stg` | таблица `stg_grant`, TRUNCATE + COPY на каждый прогон |
| Ядро (dwh) | схема `grants_dwh` | звезда: `fact_grant` + 8 измерений |
| Витрины (mart) | схема `grants_mart` | 5 представлений для BI-досок |

Оркестрация ETL-процесса: Apache Airflow, DAG `pres_grants_dwh_etl`
(каждый этап - отдельный PythonOperator, запуск вручную).
СУБД: PostgreSQL 17, одиночная конфигурация.

## Модель (звезда Кимбалла, по er_diagram.drawio)

Таблица фактов `fact_grant`: одна строка = одна заявка на грант.
- составной первичный ключ из всех FK: `application_key, date_reg_key,
  implem_date_key, org_key, contest_key, direction_key, region_key, winner_key`
- метрики: `rate`, `money_req_grant`, `cofunding`, `total_money`

Измерения (8 штук, суррогатный ключ BIGSERIAL, естественный ключ UNIQUE):
- `dim_application` - описание заявки (естественный ключ `req_num`), связь с фактом 1:1
- `dim_date` - календарь дат подачи 2016-2028, ключ ГГГГММДД, строка 0 = дата не указана
- `dim_implementation` - словарь периодов реализации (пары дат старта и финиша)
- `dim_org` - организация-заявитель (естественный ключ ИНН)
- `dim_contest` - конкурс (+ год)
- `dim_direction` - конкурсное направление
- `dim_region` - регион
- `dim_winner_status` - результат заявки (мини-измерение, 2 строки)

## Типы медленно меняющихся измерений (SCD)

- SCD Type 0 («значение фиксируется и не меняется»): `dim_application`,
  `dim_date`, `dim_implementation`, `dim_contest`, `dim_direction`,
  `dim_region`, `dim_winner_status` - загрузка однократная из фиксированного
  снимка, история изменений атрибутов не требуется
- SCD Type 1 («одно актуальное значение, без истории»): `dim_org` -
  единственное измерение, чьи атрибуты реально меняются со временем
  (название, адрес, сайт организации); старые значения перезаписываются
  новыми (`ON CONFLICT (inn) DO UPDATE`)
- SCD Type 2 не применяется: при переходе к регулярному обновлению на него
  был бы переведен `dim_org` (версии строк `valid_from`, `valid_to`, `is_current`)

## Витрины (для 3 аналитических досок в BI)

1. `dm_grants_wide` - широкая витрина «заявка + все измерения»
2. `dm_grants_by_region_year` - доска «Статистика по регионам»
3. `dm_grants_by_direction_year` - доска «Конкурсные направления»
4. `dm_grants_by_contest` - доска «Конкурсы»
5. `dm_top_orgs` - доска «Организации-лидеры»

## Структура проекта

```
pres_grants_dwh/
├── config.json             # подключение к БД, схемы, пути, размер чанка
├── ddl.sql                 # DDL схем и таблиц (схемы подставляются из config)
├── etl.py                  # этапы процесса (extract -> load -> marts)
├── marts.sql               # DDL витрин
├── dags/pres_grants_dag.py # Airflow DAG (по оператору на этап)
└── data/                   # рабочие файлы
    ├── raw/data_pres_grants_114.csv  # исходный CSV датасета (~2,3 ГБ)
    └── clean_grants.csv              # результат этапа extract_clean
```

## Запуск

Требования: Python 3.10+, `pandas`, `psycopg2-binary`, PostgreSQL 13+,
для Airflow - `apache-airflow`.

```bash
pip install pandas psycopg2-binary

# 0. Исходный CSV датасета положить в data/raw/data_pres_grants_114.csv
#    (или задать свой путь в config.json -> paths.raw_csv)

# 1. Создать БД (или поднять Postgres в Docker, см. ниже)
createdb pres_grants_dwh

# 2. Полный прогон
python3 etl.py

# или по этапам
python3 etl.py init_db extract_clean
python3 etl.py load_stage load_dims load_fact build_marts
```

Airflow: скопировать `dags/pres_grants_dag.py` в `$AIRFLOW_HOME/dags/`,
путь до проекта задать `PRES_GRANTS_ETL_DIR`. Запуск DAG `pres_grants_dwh_etl`
вручную, загрузка данных разовая (расписание не задано).

## BI-дашборды (Apache Superset в Docker)

Полная инструкция настройки с нуля (проверено на macOS + Docker Desktop).

```bash
# 1. Удалить старый контейнер, если он есть
docker rm -f superset

# 2. Создать контейнер с секретным ключом (генерируется автоматически).
#    Без ключа новый образ не стартует: "Refusing to start due to insecure SECRET_KEY"
docker run -d --name superset -p 8088:8088 -e SUPERSET_SECRET_KEY="$(openssl rand -base64 42)" apache/superset

# 3. Создать администратора
docker exec -it superset superset fab create-admin --username admin --password admin --firstname Admin --lastname User --email admin@local

# 4. Инициализировать внутреннюю БД Superset (несколько минут)
docker exec -it superset superset db upgrade
docker exec -it superset superset init

# 5. Установить драйвер PostgreSQL: в образе его нет,
#    без него подключение к БД падает с "Database Creation Error".
#    -u root обязателен: venv принадлежит root (иначе Permission denied),
#    ensurepip нужен, потому что pip в venv отсутствует (No module named pip)
docker exec -u root -it superset python -m ensurepip --upgrade
docker exec -u root -it superset python -m pip install psycopg2-binary
docker restart superset

# 6. Проверить готовность
curl http://localhost:8088/health   # ответ: OK
docker exec -it superset python -c "import psycopg2; print(psycopg2.__version__)"
```

Открыть http://localhost:8088, логин `admin`, пароль `admin`.

### Подключение хранилища

Settings (шестеренка) -> Database Connections -> + Database -> PostgreSQL:

| Поле | Значение |
|---|---|
| Host | `host.docker.internal` (так контейнер видит PostgreSQL на хосте) |
| Port | `5432` |
| Database name | `pres_grants_dwh` |
| Username | `postgres` |
| Password | `12345678` |

Test Connection -> Connect. Альтернатива одной строкой (вкладка SQLAlchemy URI):

```
postgresql+psycopg2://postgres:12345678@host.docker.internal:5432/pres_grants_dwh
```

### Датасеты и дашборды

Data -> Datasets -> + Dataset, схема `grants_mart`: `dm_grants_by_region_year`,
`dm_grants_by_direction_year`, `dm_top_orgs`, `dm_grants_wide`.

Три аналитические доски:
1. «Статистика по регионам»: Bar Chart по `dm_grants_by_region_year`
   (X-Axis: region, метрики SUM(applications) и SUM(wins),
   фильтр по contest_year, сортировка по applications, Limit 15)
2. «Конкурсные направления»: Line Chart по `dm_grants_by_direction_year`
   (X-Axis: contest_year, серии: direction, метрика SUM(applications))
3. «Организации-лидеры»: Table по `dm_top_orgs`
   (org_name, applications, wins, win_rate, granted_sum; сортировка по wins)

### Повседневное использование

`docker stop superset` / `docker start superset` - настройки и доски сохраняются.
ВНИМАНИЕ: при пересоздании контейнера (docker rm + run) шаги 3-5 нужно повторить.
Постоянное решение - свой образ с драйвером внутри:

```dockerfile
FROM apache/superset
USER root
RUN python -m ensurepip && python -m pip install psycopg2-binary
USER superset
```

### Решенные проблемы при настройке

| Ошибка | Причина | Решение |
|---|---|---|
| Refusing to start due to insecure SECRET_KEY | новый образ не принимает ключ по умолчанию | запуск с -e SUPERSET_SECRET_KEY (шаг 2) |
| Database Creation Error при подключении | нет драйвера psycopg2 в образе | шаг 5 |
| No module named pip | venv собран без pip | python -m ensurepip --upgrade |
| Permission denied при установке | venv принадлежит root | docker exec -u root |

## Особенности данных

- CSV: UTF-8, разделитель `;`; из текстовых полей удалены точки с запятой
- `rate` (рейтинг) заполнен только для победителей
- встречаются некорректные ИНН/ОГРН - невалидные ИНН заменяются заглушкой
  «Не указан», чтобы факт всегда находил измерение организации
- пропуски `region`, `contest`, `direction` заменяются заглушкой «Не указан»:
  у факта составной PK из всех FK, каждый ключ обязан находиться
- у 20 348 заявок не заполнены даты реализации - в `dim_implementation`
  им соответствует пара ключей (0, 0) с неизвестной датой календаря
- у 47 заявок признак `winner` не заполнен - трактуются как «Не победа»
  (отмечено в коде `load_fact`)
- `year` в источнике хранится как «2026.0» - приводится к INTEGER
- дубли заявок в источнике не встречались; защиту на уровне хранилища дает
  `ON CONFLICT DO NOTHING` по составному PK факта

## Повторный запуск

Загрузка идемпотентна: измерения вставляются с `ON CONFLICT DO NOTHING`
(`dim_org` - `DO UPDATE`, это SCD Type 1), факты - с `ON CONFLICT DO NOTHING`
по составному PK, стейдж перед загрузкой очищается. При случайном повторном
запуске дубли в хранилище не появятся.
