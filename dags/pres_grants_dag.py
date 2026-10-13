"""
Airflow DAG ETL-процесса хранилища «Проекты конкурса Фонда президентских грантов».
Каждый этап etl.py - отдельный PythonOperator.
Загрузка разовая, поэтому schedule=None, запуск вручную.

Размещение: скопировать в $AIRFLOW_HOME/dags/, путь до проекта задать
переменной PRES_GRANTS_ETL_DIR (по умолчанию проект ищется рядом: ../pres_grants_dwh).
"""
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# путь до папки проекта с etl.py (можно задать переменной окружения)
ETL_DIR = os.environ.get(
    'PRES_GRANTS_ETL_DIR',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pres_grants_dwh'),
)
if ETL_DIR not in sys.path:
    sys.path.insert(0, ETL_DIR)

# этапы пайплайна: порядок задач в DAG совпадает с порядком функций в etl.py
STEPS = [
    'init_db',
    'extract_clean',
    'load_stage',
    'load_dims',
    'load_fact',
    'build_marts',
]


def run_step(step_name: str):
    """Запускает один этап ETL по имени функции из etl.py.

    step_name - имя функции-этапа, например 'load_dims' (берется из списка STEPS)
    """
    # импорт etl здесь, а не наверху: парсер DAG-ов не должен подключаться к БД
    import etl
    getattr(etl, step_name)()


# общие настройки задач: владелец и одна повторная попытка при сбое
default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay_minutes': 2,
}

with DAG(
    dag_id='pres_grants_dwh_etl',
    description='Загрузка данных Фонда президентских грантов в хранилище (звезда Кимбалла)',
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=['vkr', 'etl', 'kimball'],
) as dag:
    # задачи создаем циклом и выстраиваем в цепочку: каждая зависит от предыдущей
    previous = None
    for step in STEPS:
        task = PythonOperator(
            task_id=step,
            python_callable=run_step,
            op_kwargs={'step_name': step},
        )
        if previous is not None:
            previous >> task
        previous = task
