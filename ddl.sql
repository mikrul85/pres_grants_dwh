/* DDL хранилища по методологии Кимбалла (чистая звезда), финальная версия.
   Модель соответствует er_diagram.drawio (финальная):
   8 измерений, первичный ключ факта - составной из всех внешних ключей.
   Плейсхолдеры {stg}, {dwh}, {mart} заменяются именами схем из config.json.
   Названия полей стейджа совпадают с атрибутами датасета, в комментариях
   указано, что означает каждое поле по описанию набора данных. */

CREATE SCHEMA IF NOT EXISTS {stg};
CREATE SCHEMA IF NOT EXISTS {dwh};
CREATE SCHEMA IF NOT EXISTS {mart};

-- Стейджинговый слой: копия очищенного файла, структура как у источника
CREATE TABLE IF NOT EXISTS {stg}.stg_grant (
    name TEXT, -- название проекта
    contest TEXT, -- название конкурса, на который подана заявка
    year INTEGER, -- год конкурса (в источнике вид '2026.0', приведен к числу)
    direction TEXT, -- конкурсное направление проекта
    date_req DATE, -- дата подачи заявки на конкурс
    region TEXT, -- регион организации (пропуски заменены на 'Не указан')
    org TEXT, -- наименование организации-заявителя
    inn TEXT, -- ИНН организации (невалидные заменены на 'Не указан')
    ogrn TEXT, -- ОГРН организации
    implem_start DATE, -- предполагаемая дата начала реализации проекта
    implem_end DATE, -- предполагаемая дата окончания реализации проекта
    winner BOOLEAN, -- отметка о победе или проигрыше в конкурсе
    rate NUMERIC(6,2), -- рейтинг заявки (заполнен только у победителей)
    money_req_grant NUMERIC(15,2), -- размер гранта у победителей или запрашиваемая сумма у проигравших
    cofunding NUMERIC(15,2), -- сумма софинансирования
    total_money NUMERIC(15,2), -- общая сумма расходов на реализацию проекта
    description TEXT, -- авторское описание проекта
    goal TEXT, -- цель проекта
    tasks TEXT, -- задачи проекта
    soc_signif TEXT, -- обоснование социальной значимости
    pj_geo TEXT, -- география проекта
    target_groups TEXT, -- целевые группы проекта
    address TEXT, -- адрес организации
    web_site TEXT, -- сайт организации
    req_num TEXT, -- номер заявки (бизнес-ключ)
    link TEXT, -- ссылка на заявку на сайте Фонда
    okato TEXT, -- код ОКАТО региона
    oktmo TEXT, -- код ОКТМО региона
    level TEXT -- уровень географической единицы (например 'регион')
);

/* Измерение даты подачи заявки (календарь), SCD Type 0.
   Строка с date_reg_key = 0 - заглушка для неизвестной даты:
   на нее ссылается факт, если дата подачи отсутствует или не распознана. */
CREATE TABLE IF NOT EXISTS {dwh}.dim_date (
    date_reg_key INTEGER PRIMARY KEY, -- суррогатный ключ в формате ГГГГММДД (0 = дата не указана)
    full_date DATE UNIQUE, -- календарная дата (NULL у строки неизвестной даты)
    year INTEGER NOT NULL, -- год
    quarter INTEGER NOT NULL, -- квартал (1-4)
    month INTEGER NOT NULL, -- номер месяца (1-12)
    month_name TEXT NOT NULL, -- название месяца
    day INTEGER NOT NULL, -- день месяца
    day_of_week INTEGER NOT NULL, -- день недели (1 = понедельник)
    is_weekend BOOLEAN NOT NULL -- выходной ли день (суббота/воскресенье)
);

/* Измерение периода реализации проекта, SCD Type 0.
   Одна строка = уникальная пара дат старта и финиша; ключи старта и финиша -
   значения date_reg_key календаря dim_date (0 = дата не указана). */
CREATE TABLE IF NOT EXISTS {dwh}.dim_implementation (
    implem_date_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ периода
    implem_start_key INTEGER NOT NULL, -- ключ даты начала реализации (формат ГГГГММДД)
    implem_end_key INTEGER NOT NULL, -- ключ даты окончания реализации (формат ГГГГММДД)
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(), -- время загрузки строки
    UNIQUE (implem_start_key, implem_end_key) -- пара дат уникальна
);

/* Измерение описания заявки, SCD Type 0: контекст заявки фиксируется
   на момент подачи и не перезаписывается. Связь с фактом 1:1. */
CREATE TABLE IF NOT EXISTS {dwh}.dim_application (
    application_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ заявки
    req_num TEXT NOT NULL UNIQUE, -- естественный ключ: номер заявки (атрибут req_num датасета)
    link TEXT, -- ссылка на заявку на сайте Фонда
    name TEXT, -- название проекта
    description TEXT, -- авторское описание проекта
    goal TEXT, -- цель проекта
    tasks TEXT, -- задачи проекта
    soc_signif TEXT, -- обоснование социальной значимости
    pj_geo TEXT, -- география проекта
    target_groups TEXT, -- целевые группы проекта
    processed_at TIMESTAMP NOT NULL DEFAULT NOW() -- время загрузки строки
);

/* Измерение организации-заявителя (естественный ключ ИНН), SCD Type 1:
   одно актуальное значение атрибутов на ИНН, при изменении старое
   значение перезаписывается новым без сохранения истории. */
CREATE TABLE IF NOT EXISTS {dwh}.dim_org (
    org_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ
    inn TEXT NOT NULL UNIQUE, -- естественный ключ: ИНН организации (атрибут inn датасета)
    org_name TEXT, -- наименование организации (атрибут org датасета)
    ogrn TEXT, -- ОГРН организации
    address TEXT, -- адрес организации
    web_site TEXT, -- сайт организации
    okato TEXT, -- код ОКАТО региона
    oktmo TEXT, -- код ОКТМО региона
    geo_level TEXT, -- уровень географической единицы (атрибут level датасета)
    processed_at TIMESTAMP NOT NULL DEFAULT NOW() -- время загрузки строки
);

-- Измерение конкурса, SCD Type 0
CREATE TABLE IF NOT EXISTS {dwh}.dim_contest (
    contest_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ
    contest TEXT NOT NULL UNIQUE, -- естественный ключ: название конкурса (атрибут contest датасета)
    year INTEGER, -- год конкурса (атрибут year датасета)
    processed_at TIMESTAMP NOT NULL DEFAULT NOW() -- время загрузки строки
);

-- Измерение конкурсного направления, SCD Type 0
CREATE TABLE IF NOT EXISTS {dwh}.dim_direction (
    direction_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ
    direction TEXT NOT NULL UNIQUE, -- естественный ключ: название направления (атрибут direction датасета)
    processed_at TIMESTAMP NOT NULL DEFAULT NOW() -- время загрузки строки
);

-- Измерение региона, SCD Type 0
CREATE TABLE IF NOT EXISTS {dwh}.dim_region (
    region_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ
    region TEXT NOT NULL UNIQUE, -- естественный ключ: название региона (атрибут region датасета)
    processed_at TIMESTAMP NOT NULL DEFAULT NOW() -- время загрузки строки
);

/* Мини-измерение результата заявки, SCD Type 0:
   справочник закрыт, две строки - «Победа» и «Не победа». */
CREATE TABLE IF NOT EXISTS {dwh}.dim_winner_status (
    winner_key BIGSERIAL PRIMARY KEY, -- суррогатный ключ
    winner_status TEXT NOT NULL UNIQUE, -- результат заявки: 'Победа' или 'Не победа'
    processed_at TIMESTAMP NOT NULL DEFAULT NOW() -- время загрузки строки
);

/* Таблица фактов: одна строка = одна заявка на грант.
   Только метрики и внешние ключи; первичный ключ - составной из всех FK
   (по методологии Кимбалла зерно факта однозначно задается набором измерений).
   Все ключи NOT NULL: для отсутствующих дат используется строка-заглушка
   (date_reg_key = 0), справочники заполняются из того же стейджа. */
CREATE TABLE IF NOT EXISTS {dwh}.fact_grant (
    application_key BIGINT NOT NULL REFERENCES {dwh}.dim_application (application_key), -- FK: описание заявки
    date_reg_key INTEGER NOT NULL REFERENCES {dwh}.dim_date (date_reg_key), -- FK: дата подачи заявки
    implem_date_key BIGINT NOT NULL REFERENCES {dwh}.dim_implementation (implem_date_key), -- FK: период реализации
    org_key BIGINT NOT NULL REFERENCES {dwh}.dim_org (org_key), -- FK: организация-заявитель
    contest_key BIGINT NOT NULL REFERENCES {dwh}.dim_contest (contest_key), -- FK: конкурс
    direction_key BIGINT NOT NULL REFERENCES {dwh}.dim_direction (direction_key), -- FK: направление
    region_key BIGINT NOT NULL REFERENCES {dwh}.dim_region (region_key), -- FK: регион
    winner_key BIGINT NOT NULL REFERENCES {dwh}.dim_winner_status (winner_key), -- FK: результат (победа/не победа)
    rate NUMERIC(6,2), -- рейтинг заявки (только у победителей), метрика
    money_req_grant NUMERIC(15,2), -- размер гранта/запрашиваемая сумма, метрика
    cofunding NUMERIC(15,2), -- софинансирование, метрика
    total_money NUMERIC(15,2), -- общие расходы на проект, метрика
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(), -- время загрузки строки
    PRIMARY KEY (application_key, date_reg_key, implem_date_key, org_key,
                 contest_key, direction_key, region_key, winner_key) -- составной PK из всех FK
);

-- индексы по внешним ключам факта (ускоряют join'ы витрин);
-- application_key покрыт первичным ключом как его первое поле
CREATE INDEX IF NOT EXISTS idx_fact_grant_date ON {dwh}.fact_grant (date_reg_key);
CREATE INDEX IF NOT EXISTS idx_fact_grant_implem ON {dwh}.fact_grant (implem_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_grant_org ON {dwh}.fact_grant (org_key);
CREATE INDEX IF NOT EXISTS idx_fact_grant_contest ON {dwh}.fact_grant (contest_key);
CREATE INDEX IF NOT EXISTS idx_fact_grant_direction ON {dwh}.fact_grant (direction_key);
CREATE INDEX IF NOT EXISTS idx_fact_grant_region ON {dwh}.fact_grant (region_key);
CREATE INDEX IF NOT EXISTS idx_fact_grant_winner ON {dwh}.fact_grant (winner_key);
