import os
import time
import traceback
from datetime import datetime
import hashlib
import celery
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import worker_process_init
from celery.result import GroupResult
from celery.result import AsyncResult
from celery import Celery

from celery.utils.log import get_task_logger

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from log_manager import log

from event_manager.exceptions import WaitingRemoteData
from blacklist.models import BlackListRepository

app_celery = Celery("rs_schedule_threshold_task")
app_celery.config_from_object('async_process.celeryconfig')


def get_operation_id(sensor_code, phase_code, integrator_id=None):
    opid = f'{sensor_code}.{phase_code}.{integrator_id}.{datetime.now().strftime("%Y%m%d%H%M%S")}'
    os.environ['SENSOR_IDENTIFICATION'] = opid
    return opid


def get_task_status(task_id):
    status = ''
    task_status = (GroupResult.restore(task_id, backend=app_celery.backend) or
                   AsyncResult(task_id, backend=app_celery.backend))
    if ((isinstance(task_status, GroupResult) and task_status.waiting()) or
        (isinstance(task_status, AsyncResult) and
         task_status.status == 'PENDING')):
        status = 'WAITING/PENDING'
    elif ((isinstance(task_status, GroupResult) and task_status.successful()) or
          (isinstance(task_status, AsyncResult) and
           task_status.status == 'SUCCESS')):
        status = 'SUCCESS'
    elif ((isinstance(task_status, GroupResult) and task_status.failed()) or
          (isinstance(task_status, AsyncResult) and
           task_status.status == 'FAILURE')):
        status = 'FAILURE'
    return status


@app_celery.task(
    bind=True,
    soft_time_limit=10800,  # This will raise an SoftTimeLimitExceeded
    hard_time_limit=14400,  # This will kill the process
    max_retries=3,
    autoretry_for=(BaseException,))  # timeout 30 min, 30 min
def call_ml_model_fit(self,
                      sensor_code,
                      phase_code,
                      model_code='',
                      integrator_id='voltaware'):

    # TODO: Remove model_code='' after deploy to prod.
    if BlackListRepository(sensor_code,
                           integrator_id=integrator_id).is_blacklisted():
        log.info(
            "Sensor {} is blacklisted: skipping model fit".format(sensor_code))
        return

    operation_id = get_operation_id(sensor_code, phase_code, integrator_id)
    try:
        from ml_models.ml_daily_disaggregation import DailyDisaggregation
        log.info((
            "call_ml_model_fit: Calling fit for sensor {}, phase {}, model_code {}"
            "trial {}/{} operationid: {}").format(sensor_code, phase_code,
                                                  model_code,
                                                  self.request.retries,
                                                  self.max_retries,
                                                  operation_id))

        daily_disg = DailyDisaggregation(sensor_code,
                                         phase_code,
                                         operation_id=operation_id,
                                         integrator_id=integrator_id)
        daily_disg.fit()
    except SoftTimeLimitExceeded:
        log.error(('SensorCode={} PhaseCode={} TaskType=ml_model_fit TaskId={}'
                   'ml_model_fit Received SoftTimeLimitExceeded').format(
                       sensor_code, phase_code, self.request.id))
        raise
    except BaseException:
        log.error(
            'Exception at call_ml_model_fit Sensor:{} Phase:{} Error:{}'.format(
                sensor_code, phase_code, str(traceback.format_exc())))
        raise


@app_celery.task(bind=True)
def call_dummy(self):
    return


@app_celery.task(
    bind=True,
    soft_time_limit=10800,  # 2h
    hard_time_limit=14400,
    max_retries=3,
    autoretry_for=(BaseException,))  # timeout 10 min, 10min
def call_ml_model_estimate(self,
                           chain_result,
                           sensor_code,
                           phase_code,
                           model_code='',
                           integrator_id='voltaware'):

    # TODO: Remove model_code='' after deploy to prod.
    if BlackListRepository(sensor_code,
                           integrator_id=integrator_id).is_blacklisted():
        log.info("Sensor {} is blacklisted: skipping model estimate".format(
            sensor_code))
        return

    operation_id = get_operation_id(sensor_code, phase_code, integrator_id)
    try:
        from ml_models.ml_daily_disaggregation import DailyDisaggregation
        log.info(
            "Executing call_ml_model_estimate for sensor {} phase {} trial {}/{}"
            .format(sensor_code, phase_code, self.request.retries,
                    self.max_retries))
        daily_disg = DailyDisaggregation(sensor_code,
                                         phase_code,
                                         operation_id=operation_id,
                                         integrator_id=integrator_id)
        daily_disg.estimate()
    except SoftTimeLimitExceeded:
        log.error(
            ('SensorCode={} PhaseCode={} TaskType=ml_model_estimate TaskId={}'
             'ml_model_estimate Received SoftTimeLimitExceeded').format(
                 sensor_code, phase_code, self.request.id))
        raise
    except BaseException:
        log.error(
            'Exception at call_ml_model_estimate Sensor:{} Phase:{} Error:{}'.
            format(sensor_code, phase_code, str(traceback.format_exc())))
        raise


@app_celery.task(
    bind=True,
    soft_time_limit=10800,  # This will raise an SoftTimeLimitExceeded
    hard_time_limit=14400,  # This will kill the process
    max_retries=3,
    autoretry_for=(BaseException,))
def call_retrain(
    self,
    sensor_code,
    phase_code,
    sensor_timezone,
    from_date,
    to_date,
    integrator_id='voltaware',
):

    if BlackListRepository(sensor_code,
                           integrator_id=integrator_id).is_blacklisted():
        log.info(
            "Sensor {} is blacklisted: skipping retrain".format(sensor_code))
        return

    operation_id = get_operation_id(sensor_code, phase_code, integrator_id)
    try:
        from recalculate_disaggregation.actions import retrain

        log.info(
            "Executing call_retrain for sensor {} phase {} from {} to {} trial {}/{} OperationId: {}"
            .format(sensor_code, phase_code, from_date, to_date,
                    self.request.retries, self.max_retries, operation_id))

        retrain(
            sensor_code,
            phase_code,
            sensor_timezone,
            from_date,
            to_date,
            operation_id,
            integrator_id=integrator_id,
        )
    except SoftTimeLimitExceeded:
        log.error((
            'SensorCode={} PhaseCode={} SensorTimezone={} FromDate={} TaskType=Retrain TaskId={}  OperationId: {}'
            'Retrain Received SoftTimeLimitExceeded').format(
                sensor_code, phase_code, sensor_timezone, from_date,
                self.request.id, operation_id))
        raise
    except BaseException:
        log.error(
            'SensorCode={} PhaseCode={} SensorTimezone={} FromDate={} TaskType=Retrain TaskId={} OperationId: {}: {}'
            .format(sensor_code, phase_code, sensor_timezone, from_date,
                    self.request.id, operation_id, str(traceback.format_exc())))
        raise


@app_celery.task(bind=True,
                 soft_time_limit=1800,
                 hard_time_limit=1800,
                 max_retries=3,
                 autoretry_for=(BaseException,))
def call_dummy_retrain(self, sensor_code, phase_code, sensor_timezone,
                       from_date, to_date):
    operation_id = get_operation_id(sensor_code, phase_code)
    log.info(
        "Called dummy retrain on sensor {} phase {} OperationId: {}".format(
            sensor_code, phase_code, operation_id))
    return


@app_celery.task(
    bind=True,
    soft_time_limit=10800,  # 2h
    hard_time_limit=14400,  # 2h
    max_retries=3,
    autoretry_for=(BaseException,))
def call_reestimate(
    self,
    chain_result,
    sensor_code,
    phase_code,
    sensor_timezone,
    date,
    recalc_day,
    integrator_id='voltaware',
):

    if BlackListRepository(sensor_code,
                           integrator_id=integrator_id).is_blacklisted():
        log.info(
            "Sensor {} is blacklisted: skipping reestimate".format(sensor_code))
        return

    operation_id = get_operation_id(sensor_code, phase_code, integrator_id)
    try:
        from recalculate_disaggregation.actions import reestimate

        log.info(
            "Executing call_reestimate for sensor {} phase {} trial {}/{}  OperationId: {}"
            .format(sensor_code, phase_code, self.request.retries,
                    self.max_retries, operation_id))

        reestimate(
            sensor_code,
            phase_code,
            sensor_timezone,
            date,
            recalc_day,
            operation_id=operation_id,
            integrator_id=integrator_id,
        )

    except SoftTimeLimitExceeded:
        log.error((
            'SensorCode={} PhaseCode={} SensorTimezone={} TaskType=ReEstimate TaskId={} OperationId: {}'
            'ReEstimate Received SoftTimeLimitExceeded').format(
                sensor_code, phase_code, sensor_timezone, self.request.id,
                operation_id))
        raise
    except BaseException:
        log.error(
            'SensorCode={} PhaseCode={} SensorTimezone={} FromDate={} TaskType=ReEstimate TaskId={} {} OperationId: {}'
            .format(sensor_code, phase_code, sensor_timezone, date,
                    self.request.id, str(traceback.format_exc()), operation_id))
        raise


@app_celery.task(bind=True,
                 soft_time_limit=30,
                 hard_time_limit=1800,
                 max_retries=3,
                 autoretry_for=(BaseException,))
def call_recalculation_success(self, chain_result, sensor_code, phase_code,
                               sensor_timezone, from_date, to_date):
    operation_id = get_operation_id(
        sensor_code,
        phase_code,
    )
    log.info((
        'Success in recalculation: SensorCode={} PhaseCode={} SensorTimezone={} FromDate={} ToDate={}'
        'TaskType=RecalculationSuccess TaskId={} OperationId={}.').format(
            sensor_code, phase_code, sensor_timezone, from_date, to_date,
            self.request.id, operation_id))


@app_celery.task(bind=True,
                 soft_time_limit=30,
                 hard_time_limit=1800,
                 max_retries=3,
                 autoretry_for=(Exception,))
def call_recalculation_error(self, chain_result, sensor_code, phase_code,
                             sensor_timezone, from_date, to_date):
    operation_id = get_operation_id(sensor_code, phase_code)
    result = AsyncResult(id=chain_result)
    log.error((
        'Failure in recalculation: SensorCode={} PhaseCode={} SensorTimezone={} FromDate={} ToDate={} '
        'TaskType=RecalculationError TaskId={}: Failure ({} {}) OperationId={}.'
    ).format(sensor_code, phase_code, sensor_timezone, from_date, to_date,
             self.request.id, chain_result, result.traceback, operation_id))
    raise result.result


@app_celery.task(bind=True,
                 soft_time_limit=300,
                 hard_time_limit=900,
                 max_retries=3,
                 autoretry_for=(BaseException,))
def call_schedule_daily_trigger(self, integration_type):
    # We had to use this, since global import will cause circular dependency
    # SchedulingManager imports MLManagerStorage which imports this file
    from scheduling.actions import SchedulingManager

    operation_id = get_operation_id(-1, -1)
    try:
        start = time.time()
        SchedulingManager(integrator_id=integration_type,
                          operation_id=operation_id).run_daily_schedule_sync()
        end = time.time()
        log.info(
            "TRIGGER - [integration: {}], elapsed time: {} OperationId: {}".
            format(integration_type, end - start, operation_id))
    except BaseException:
        log.error(
            "TRIGGER - [integration: {}] Error triggering appliances: {} OperationId: {}"
            .format(integration_type, traceback.format_exc(), operation_id))
        raise
