import logging
import os
from typing import Dict

import pandas as pd
from airflow.models import XCom
from airflow.utils.db import provide_session

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator  # isort:skip

logger = logging.getLogger("Airflow Utils")


@provide_session
def cleanup_xcom(context: Dict, session=None) -> None:
    """Clean up xcom, to avoid keep trash and use database space incorrectly.

    Args:
        context: Airflow context, where TaskInstance (ti) will be used.
        session: Airflow session to be deleted receives None, because TaskInstance will replace
        with the correct session.

    Returns:
        There is no Return.
    """
    ti = context["ti"]
    session.query(XCom).filter(
        XCom.dag_id == ti.dag_id, XCom.execution_date == ti.execution_date
    ).delete()


def alert_slack_channel(context: Dict) -> None:
    """Send message to slack if task fail, using function on_failure_callback.

    Args:
        context: It has all variables that will be used to create default message.
        context["task_instance"]: Class TaskInstance with all values that we need
        from airflow task operator.
        context["task_instance"]["task_id"]: Airflow Task name that failed.
        context["task_instance"]["dag_id"]: Airflow Task name that failed.
        context["execution_date"]: Execution date that task runs.
        context["exception"]: What was the exception that Task suffered.
        context["reason"]: What was the reason that Task suffered.
        context["task_instance"]["log_url"]: Log url to Airflow local.
        context["task_instance"]["duration"]: How many time that Task run before failed.

    Returns:
        There is no return
    """
    last_task = context.get("task_instance")
    message_variables = {
        "dag_id": last_task.dag_id,
        "task_id": last_task.task_id,
        "execution_date": context.get("execution_date"),
        "error_message": context.get("exception") or context.get("reason"),
        "log_url": last_task.log_url,
        "duration": last_task.duration,
    }
    title = (
        f':red_circle: AIRFLOW DAG *{message_variables["dag_id"]}*'
        f' - TASK *{message_variables["task_id"]}* has failed! :boom:'
    )
    msg_parts = {
        "Execution date": message_variables["execution_date"],
        "Error": message_variables["error_message"],
        "Log url": message_variables["log_url"],
        "Task Duration": message_variables["duration"],
    }
    msg = "\n".join([title, *[f"*{k}*: {v}" for k, v in msg_parts.items()]]).strip()
    SlackWebhookOperator(
        task_id="notify_slack_channel_alert", http_conn_id="slack_webhook", message=msg
    ).execute(context=None)


def read_parquet(**kwargs) -> pd.DataFrame:
    """Read data from parquet to call function to insert data into postgres database.

    Args:
        **relative_path: File path that parquet are stored, to read it on local folders.

    Returns:
        Only a f-string with which table were inserted. For example:
        acute_g_day_bw_all_days Done!
    """
    relative_path = kwargs.get("relative_path")
    home_path = os.environ["HOME"]
    file_path = os.path.join(home_path, "dags", relative_path)
    df = pd.read_parquet(file_path)
    return df
