-- Витрины слоя mart поверх звезды Кимбалла (финальная модель, 8 измерений).
-- Плейсхолдеры {dwh} и {mart} заменяются именами схем из config.json.

/* 1. Широкая витрина: одна строка = одна заявка со всеми измерениями.
   Даты реализации берутся из словаря периодов dim_implementation:
   его ключи старта и финиша - значения date_reg_key календаря dim_date. */
CREATE OR REPLACE VIEW {mart}.dm_grants_wide AS
SELECT
    a.req_num, -- номер заявки (req_num датасета)
    c.year AS contest_year, -- год конкурса (year датасета, из dim_contest)
    dk.full_date AS date_req, -- дата подачи заявки (date_req датасета, из календаря)
    ds.full_date AS implem_start, -- дата начала реализации проекта
    de.full_date AS implem_end, -- дата окончания реализации проекта
    w.winner_status, -- результат заявки: 'Победа' или 'Не победа'
    (w.winner_status = 'Победа') AS winner, -- признак победы (для совместимости агрегатов)
    f.rate, -- рейтинг заявки (только у победителей)
    f.money_req_grant, -- размер гранта / запрашиваемая сумма
    f.cofunding, -- софинансирование
    f.total_money, -- общие расходы на проект
    a.name, -- название проекта
    a.pj_geo, -- география проекта
    a.target_groups, -- целевые группы
    a.link, -- ссылка на заявку на сайте Фонда
    o.inn, -- ИНН организации
    o.org_name, -- название организации (org датасета)
    o.address, -- адрес организации
    o.web_site, -- сайт организации
    o.okato, -- код ОКАТО региона
    o.oktmo, -- код ОКТМО региона
    o.geo_level, -- уровень географической единицы (level датасета)
    c.contest, -- название конкурса
    d.direction, -- конкурсное направление
    r.region -- регион организации
FROM {dwh}.fact_grant f
JOIN {dwh}.dim_application a ON a.application_key = f.application_key
LEFT JOIN {dwh}.dim_date dk ON dk.date_reg_key = f.date_reg_key
LEFT JOIN {dwh}.dim_implementation imp ON imp.implem_date_key = f.implem_date_key
LEFT JOIN {dwh}.dim_date ds ON ds.date_reg_key = imp.implem_start_key
LEFT JOIN {dwh}.dim_date de ON de.date_reg_key = imp.implem_end_key
LEFT JOIN {dwh}.dim_org o ON o.org_key = f.org_key
LEFT JOIN {dwh}.dim_contest c ON c.contest_key = f.contest_key
LEFT JOIN {dwh}.dim_direction d ON d.direction_key = f.direction_key
LEFT JOIN {dwh}.dim_region r ON r.region_key = f.region_key
LEFT JOIN {dwh}.dim_winner_status w ON w.winner_key = f.winner_key;

-- 2. Витрина для доски «Статистика по регионам»
CREATE OR REPLACE VIEW {mart}.dm_grants_by_region_year AS
SELECT
    region, -- регион организации
    contest_year, -- год конкурса
    COUNT(*) AS applications, -- всего заявок
    COUNT(*) FILTER (WHERE winner) AS wins, -- из них побед
    ROUND(COUNT(*) FILTER (WHERE winner)::numeric / COUNT(*), 4) AS win_rate, -- доля побед (0-1)
    SUM(money_req_grant) FILTER (WHERE winner) AS granted_sum, -- сумма выданных грантов
    SUM(money_req_grant) FILTER (WHERE NOT winner) AS requested_sum, -- сумма запрошенного у проигравших
    SUM(cofunding) AS cofunding_sum, -- сумма софинансирования
    SUM(total_money) AS total_money_sum, -- сумма расходов проектов
    ROUND(AVG(rate) FILTER (WHERE winner), 2) AS avg_winner_rate -- средний рейтинг победителей
FROM {mart}.dm_grants_wide
GROUP BY region, contest_year;

-- 3. Витрина для доски «Конкурсные направления»
CREATE OR REPLACE VIEW {mart}.dm_grants_by_direction_year AS
SELECT
    direction, -- конкурсное направление
    contest_year, -- год конкурса
    COUNT(*) AS applications, -- всего заявок
    COUNT(*) FILTER (WHERE winner) AS wins, -- из них побед
    ROUND(COUNT(*) FILTER (WHERE winner)::numeric / COUNT(*), 4) AS win_rate, -- доля побед (0-1)
    SUM(money_req_grant) FILTER (WHERE winner) AS granted_sum, -- сумма выданных грантов
    ROUND(AVG(money_req_grant) FILTER (WHERE winner), 0) AS avg_grant, -- средний размер гранта
    ROUND(AVG(rate) FILTER (WHERE winner), 2) AS avg_winner_rate -- средний рейтинг победителей
FROM {mart}.dm_grants_wide
GROUP BY direction, contest_year;

-- 4. Витрина для доски «Конкурсы»
CREATE OR REPLACE VIEW {mart}.dm_grants_by_contest AS
SELECT
    contest, -- название конкурса
    MAX(contest_year) AS contest_year, -- год конкурса
    COUNT(*) AS applications, -- всего заявок
    COUNT(*) FILTER (WHERE winner) AS wins, -- из них побед
    ROUND(COUNT(*) FILTER (WHERE winner)::numeric / COUNT(*), 4) AS win_rate, -- доля побед (0-1)
    SUM(money_req_grant) FILTER (WHERE winner) AS granted_sum, -- сумма выданных грантов
    COUNT(DISTINCT inn) AS org_count, -- сколько организаций участвовало
    COUNT(DISTINCT region) AS region_count -- сколько регионов участвовало
FROM {mart}.dm_grants_wide
GROUP BY contest;

-- 5. Витрина для доски «Организации-лидеры»
CREATE OR REPLACE VIEW {mart}.dm_top_orgs AS
SELECT
    inn, -- ИНН организации
    MAX(org_name) AS org_name, -- название организации
    MAX(region) AS region, -- регион организации
    COUNT(*) AS applications, -- всего заявок
    COUNT(*) FILTER (WHERE winner) AS wins, -- из них побед
    ROUND(COUNT(*) FILTER (WHERE winner)::numeric / COUNT(*), 4) AS win_rate, -- доля побед (0-1)
    SUM(money_req_grant) FILTER (WHERE winner) AS granted_sum, -- сумма полученных грантов
    SUM(cofunding) FILTER (WHERE winner) AS cofunding_sum -- сумма софинансирования по победившим заявкам
FROM {mart}.dm_grants_wide
WHERE inn <> 'Не указан'
GROUP BY inn;
