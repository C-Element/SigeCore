# Copyright (C) 2015 Clemente Junior
#
# This file is part of SigeCore
#
# SigeCore is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# SigeCore is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with SigeCore.  If not, see <http://www.gnu.org/licenses/>.

from datetime import datetime, date, time, timedelta
import sys
from multiprocessing import Process
import logging

from redis import ConnectionPool

sys.path.append('../SigeLib/')
sys.path.append('../BifrostDB/')
sys.path.append('.')

from sigecore.functions import verify_all_checks, verify_automatic_reports, \
    verify_extra_hours, verify_os_productivity, verify_gate_adjusts, \
    verify_week_journey, verify_drivers, verify_faults
from sigelib.models import Report, WidgetsData

from bifrost.db import Query
from sigelib import Environment, RedisData

logging.basicConfig(filename='/var/log/sige/sige_core.log',
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s]: %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S')

ENV = Environment(True)
CPOOL = ConnectionPool(host='localhost', port=6669, db=0)


def msg_has_been_sent(name_part, dtime_part):
    """
    Verify if the message has been sent.
    """
    data = RedisData(connection_pool=CPOOL)
    if data.get(dtime_part + name_part):
        return True
    return False


def send_msg(name_part, dtime_part, func):
    """
    Register the execution on redis server and if the func is not null
    execute func like func[0](*func[1]).
    """
    data = RedisData(connection_pool=CPOOL)
    try:
        if func:
            func[0](*func[1])
        data.set(dtime_part + name_part, 1)
    except Exception as ex:
        logging.exception(ex)
        print(ex)


def heimdall_verification():
    """
Execute all verifications of old HeimdallWeb Application.
    """
    today = date.today()
    now = datetime.now()
    now_time = datetime.now().time()
    drvrs = 'drivers'
    flts = 'faults'
    atmtc = 'automatic_reports'
    prdctvt = 'os_productivity'
    gtdjsts = 'gate_adjusts'
    mpls = 'employees'
    jrn = 'journey'
    wdgts = 'widgets'
    data = RedisData()
    many_results = verify_all_checks()
    data.set('portal_report_hours', many_results[0],
             expire=timedelta(minutes=5))
    for emp in many_results[1].keys():
        for this_time in many_results[1][emp].keys():
            if not msg_has_been_sent(mpls + emp, this_time.strftime('%H:%M')):
                for result in many_results[1][emp][this_time]:
                    send_msg(mpls + emp, this_time.strftime('%H:%M'), result)
    tmp = list(data.get_all(today.strftime('%Y%m%d') + atmtc).values())
    result = verify_automatic_reports(now, tmp)
    for key in result[0]:
        data.set(today.strftime('%Y%m%d') + atmtc + str(key), key)
    for func in result[1]:
        try:
            func[0](*func[1])
        except Exception as err:
            logging.exception(err)
            print(err)
    extra_hours = verify_extra_hours()
    for emp in extra_hours.keys():
        for this_time in extra_hours[emp].keys():
            if not msg_has_been_sent(mpls + emp,
                                     this_time.strftime('%H:%M')):
                try:
                    send_msg(mpls + emp, this_time.strftime('%H:%M'),
                             extra_hours[emp][this_time])
                except Exception as err:
                    logging.exception(err)
                    print(err)
    if now_time >= time(6, 0):
        if not msg_has_been_sent(prdctvt, today.strftime('%Y%m%d')):
            try:
                result = verify_os_productivity()
                send_msg(prdctvt, today.strftime('%Y%m%d'), result)
            except Exception as err:
                logging.exception(err)
                print(err)
    if now_time >= time(6, 10):
        if not msg_has_been_sent(gtdjsts, today.strftime('%Y%m%d')):
            try:
                data.set(today.strftime('%Y%m%d') + gtdjsts, 1)
                verify_gate_adjusts()
            except Exception as err:
                logging.exception(err)
                print(err)
    if now_time >= time(7, 0):
        if not msg_has_been_sent(wdgts, today.strftime('%Y%m%d')):
            data.set(today.strftime('%Y%m%d') + wdgts, 1)
            for w in Query(Report).get(is_widget=True):
                Process(target=WidgetsData().update_data,
                        args=[w.id]).start()
    if now_time >= time(8, 0):
        if not msg_has_been_sent(jrn, today.strftime('%Y%m%d')):
            try:
                result = verify_week_journey()
                send_msg(jrn, today.strftime('%Y%m%d'), result)
            except Exception as err:
                logging.exception(err)
                print(err)
    if now_time >= time(8, 3):
        if not msg_has_been_sent(drvrs, today.strftime('%Y%m%d')):
            try:
                result = verify_drivers()
                send_msg(drvrs, today.strftime('%Y%m%d'), result)
            except Exception as err:
                logging.exception(err)
                print(err)
    if now_time >= time(8, 6):
        if not msg_has_been_sent(flts, today.strftime('%Y%m%d')):
            try:
                result = verify_faults()
                send_msg(flts, today.strftime('%Y%m%d'), result)
            except Exception as err:
                logging.exception(err)
                print(err)
    logging.threading.Timer(30, heimdall_verification).start()


logging.info(
    'Iniciando em modo: {}'.format('PRODUÇÃO' if ENV.production else 'TESTE'))
try:
    heimdall_verification()
except Exception as ex:
    logging.exception(ex)
