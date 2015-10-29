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


import sys

if '../SigeLib/' not in sys.path:
    sys.path.append('../SigeLib/')

from datetime import datetime, time, date
from datetime import timedelta

from sigelib.connections import create_mss_inner, create_mss_gate, \
    create_mss_driver, create_mss_top, create_oracle, create_sige
from sigelib.consts import QUERY_GATE, QUERY_JOURNEY, QUERY_HOURS_TABLE, \
    QUERY_PIS, EHIS, INTERJOURNEY_ALERT, REST_ALERT, \
    FIRST_JOURNEY_EXTRAPOLATED_ALERT, EXTRA_HOUR_ALERT, \
    SECOND_JOURNEY_EXTRAPOLATED_ALERT, WITHOUT_CHECK_ENTRY_ALERT, \
    WITHOUT_CHECK_EXIT_ALERT, WITHOUT_CHECK_ON_40M_ALERT, QUERY_CHECKS, \
    QUERY_VERIFY_DRIVERS, QUERY_VERIFY_DRIVERS_WO_CHECKS, QUERY_VERIFY_FAULTS, \
    SCRIPTS_DAILY_UPDATE, QUERY_OS_PRODUCTIVITY, QUERY_OCCURRENCE_HOURS_TABLE, \
    QUERY_WEEK_JOURNEY, QUERY_GATE2
from bifrost.db.query import Query
from sigelib.models import Report
from sigelib.utils import send_normal_xmpp_message, send_gleyber_xmpp_message, \
    HDateTime, time_between_430_610, time_between_115_205, if_not_time, \
    send_gleyber_mail, send_managers_mail, \
    time_between_10_120, send_managers_xmpp_message, send_mail, \
    send_xmpp_message, all_as_str


def verify_all_checks():
    """
Verify checks in real-time.
    :return: a list like [employees_hours_table,
                          occurences_to_be_sended_by_XMPP_messages].
    """
    d1 = (datetime.now() - timedelta(days=3)).strftime('%y%m%d%H%M')
    d2 = datetime.now().strftime('%y%m%d%H%M')
    dt1 = (datetime.now() - timedelta(hours=3)).strftime('%m/%d/%Y %H:%M:00')
    dt2 = datetime.now().strftime('%m/%d/%Y %H:%M:00')
    gate_checks = {}
    gate_checks2 = {}
    clock_checks = {}
    empis = {}
    emp_data = {}
    date_lists = {}
    hours_table = {}
    journey_tables = {}
    connection_clock = create_mss_inner()
    connection_gate = create_mss_gate()
    portal_table = []
    rset = connection_gate.query(QUERY_GATE.format(dt1, dt2))
    to_return = {}
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in gate_checks.keys():
            gate_checks[data[0]] = {}
        if not data[2] in gate_checks[data[0]].keys():
            gate_checks[data[0]][data[2]] = []
        gate_checks[data[0]][data[2]].append(data[1].replace(second=0,
                                                             microsecond=0))
    rset = connection_gate.query(QUERY_GATE2.format(dt1, dt2))
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in gate_checks2.keys():
            gate_checks2[data[0]] = {}
        if not data[2] in gate_checks2[data[0]].keys():
            gate_checks2[data[0]][data[2]] = []
        gate_checks2[data[0]][data[2]].append(data[1].replace(second=0,
                                                              microsecond=0))
    rset = connection_clock.query(QUERY_JOURNEY)
    while len(rset) > 0:
        data = rset.pop(0)
        journey_tables[data[0]] = data[1]
    rset = connection_clock.query(QUERY_HOURS_TABLE)
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in hours_table.keys():
            hours_table[data[0]] = {}
        hours_table[data[0]][data[1]] = data[2]
    rset = connection_clock.query(QUERY_PIS)
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in empis.keys():
            empis[data[0]] = data[1]
    rset = connection_clock.query(QUERY_CHECKS.format(d1, d2))
    while len(rset) > 0:
        data = rset.pop(0)
        str_dep = " [{}]".format(data[2] if data[2] else "Sem Departamento")
        if data[0] + str_dep in date_lists.keys():
            date_lists[data[0] + str_dep]['batidas'].append(
                datetime.strptime((data[1]), '%y%m%d%H%M'))
        else:
            date_lists[data[0] + str_dep] = {}
            date_lists[data[0] +
                       str_dep]['batidas'] = [datetime.strptime((data[1]),
                                                                '%y%m%d%H%M')]
            date_lists[data[0] + str_dep]['cartao'] = data[3]
        if data[4] in clock_checks.keys():
            clock_checks[data[4]].append(datetime.strptime((data[1]),
                                                           '%y%m%d%H%M'))
        else:
            clock_checks[data[4]] = [datetime.strptime((data[1]),
                                                       '%y%m%d%H%M')]
    for emp in date_lists.keys():
        newer = date_lists[emp]['batidas'][-1]
        last_check_last_day = None
        first_check_today = None
        while len(date_lists[emp]['batidas']) > 0:
            data = date_lists[emp]['batidas'].pop(0)
            if (data >= (newer - timedelta(hours=12)) and
                        newer >= (datetime.now() - timedelta(minutes=650))):
                if emp in emp_data.keys():
                    if (len(emp_data[emp]['batidas']) == 1 and
                                (data - first_check_today) > timedelta(
                                hours=7)):
                        last_check_last_day = first_check_today
                        emp_data[emp]['batidas'] = [data.time()]
                        first_check_today = data
                    else:
                        emp_data[emp]['batidas'].append(
                            data.time())
                else:
                    emp_data[emp] = {}
                    emp_data[emp]['batidas'] = [data.time()]
                    emp_data[emp]['trabalhada'] = time(0)
                    emp_data[emp]['extra'] = time(0)
                    emp_data[emp]['cartao'] = date_lists[emp]['cartao']
                    first_check_today = data
            else:
                last_check_last_day = data
        if (last_check_last_day is not None and
                    first_check_today is not None):
            this_rest = (first_check_today - last_check_last_day)
            if this_rest.total_seconds() <= EHIS:
                interjourney = time(int(this_rest.total_seconds() / 3600),
                                    int(this_rest.total_seconds() % 3600 / 60))
                message = INTERJOURNEY_ALERT.format(emp, interjourney)
                if emp not in to_return.keys():
                    to_return[emp] = {}
                to_return[emp][interjourney] = (
                    (send_normal_xmpp_message, [message]),
                    (send_gleyber_xmpp_message, [message]))
    for emp in emp_data.keys():
        hours_list = emp_data[emp]['batidas']
        last_hour = None
        count = 0
        day_journey = None
        if emp_data[emp]['cartao'] in hours_table.keys():
            if hours_list[0] < datetime.now().time():
                seq = date.today().weekday() + 1
            else:
                seq = date.today().weekday()
            if seq in hours_table[emp_data[emp]['cartao']].keys():
                if (hours_table[emp_data[emp]['cartao']][seq] in
                        journey_tables.keys()):
                    day_journey = \
                        datetime.strptime('1/1/1111 ' + journey_tables[
                            hours_table[emp_data[emp]['cartao']][seq]
                        ].split('.')[0], '%d/%m/%Y %H:%M:%S').time()
                else:
                    day_journey = time(8)
            else:
                day_journey = time(8)
        else:
            day_journey = time(8)
        original_len = len(hours_list)
        if original_len > 2:
            rest_time = HDateTime.dif_time(hours_list[2], hours_list[1])
            if rest_time < time(1) or rest_time >= time(2, 11):
                message = REST_ALERT.format(emp,
                                            rest_time.strftime('%H:%M:%S'))
                if emp not in to_return.keys():
                    to_return[emp] = {}
                to_return[emp][rest_time] = (
                    (send_normal_xmpp_message, [message]),
                    (send_gleyber_xmpp_message, [message]))
        while count < len(hours_list):
            if count % 2 == 0:
                last_hour = hours_list[count]
            else:
                dif = HDateTime.dif_time(hours_list[count], last_hour)
                emp_data[emp]['trabalhada'] = \
                    HDateTime.sum_times(emp_data[emp]['trabalhada'], dif)
            count += 1
        if count % 2 != 0 and count in (1, 3, 5):
            emp_data[emp]['trabalhada'] = \
                HDateTime.sum_times(
                    emp_data[emp]['trabalhada'],
                    HDateTime.dif_time(HDateTime.time_now(), last_hour))
        if (emp_data[emp]['trabalhada'] >= day_journey and
                    emp_data[emp]['trabalhada'].minute > 0):
            emp_data[emp]['extra'] = HDateTime.dif_time(
                emp_data[emp]['trabalhada'], day_journey)
        if (original_len == 1 and day_journey != time(4) and
                    emp_data[emp]['trabalhada'] > time(4, 30)):
            this_time = time_between_430_610(emp_data[emp]['trabalhada'])
            if this_time:
                message = FIRST_JOURNEY_EXTRAPOLATED_ALERT.format(
                    emp, emp_data[emp]['trabalhada'].strftime('%H:%M:%S'),
                    day_journey.strftime('%H:%M'))
                if emp not in to_return.keys():
                    to_return[emp] = {}
                arr = [(send_normal_xmpp_message, [message])]
                if this_time > time(4, 50):
                    arr.append((send_gleyber_xmpp_message, [message]))
                to_return[emp][this_time] = tuple(arr)
        while len(hours_list) < 6:
            hours_list.append('')
        this_line = [emp]
        for h in hours_list:
            this_line.append(h.strftime('%H:%M:%S') if h is not '' else '')
        this_line.append(emp_data[emp]['trabalhada'].strftime('%H:%M:%S'))
        this_line.append(emp_data[emp]['extra'].strftime('%H:%M:%S'))
        this_line.append(day_journey.strftime('%H:%M:%S'))
        if emp_data[emp]['extra'] >= time(1, 15) and original_len % 2 != 0:
            this_time = time_between_115_205(emp_data[emp]['extra'])
            if this_time:
                message = EXTRA_HOUR_ALERT.format(
                    emp, emp_data[emp]['trabalhada'].strftime('%H:%M:%S'),
                    day_journey.strftime('%H:%M'))
                if emp not in to_return.keys():
                    to_return[emp] = {}
                to_return[emp][this_time] = (
                    (send_normal_xmpp_message, [message]),
                    (send_gleyber_xmpp_message, [message]))
        if original_len == 3:
            second_exp = HDateTime.dif_time(HDateTime.time_now(),
                                            hours_list[2])
            this_time = time_between_430_610(second_exp)
            if this_time:
                message = SECOND_JOURNEY_EXTRAPOLATED_ALERT.format(
                    emp, second_exp.strftime('%H:%M:%S'),
                    day_journey.strftime('%H:%M'))
                if emp not in to_return.keys():
                    to_return[emp] = {}
                arr = [(send_normal_xmpp_message, [message])]
                if this_time > time(4, 50):
                    arr.append((send_gleyber_xmpp_message, [message]))
                to_return[emp][this_time] = tuple(arr)
        portal_table.append(this_line)
    for pis in gate_checks.keys():
        if pis in clock_checks.keys():
            if 'E' in gate_checks[pis].keys():
                for gate in gate_checks[pis]['E']:
                    if ((datetime.now() - timedelta(hours=1)) <= gate <
                            (datetime.now() - timedelta(minutes=12))):
                        exists = None
                        for clock in clock_checks[pis]:
                            if ((gate <= clock <= (gate + timedelta(
                                    minutes=10))) and not exists):
                                exists = clock
                        if not exists:
                            message = WITHOUT_CHECK_ENTRY_ALERT.format(
                                empis[pis], gate.strftime('%d/%m/%Y %H:%M'))
                            if emp not in to_return.keys():
                                to_return[emp] = {}
                            to_return[emp][gate] = (
                                (send_normal_xmpp_message, [message]),
                                (send_gleyber_xmpp_message, [message]))
            if 'S' in gate_checks[pis].keys():
                for gate in gate_checks[pis]['S']:
                    if ((datetime.now() - timedelta(hours=1)) <= gate <
                            (datetime.now() - timedelta(minutes=12))):
                        exists = None
                        for clock in clock_checks[pis]:
                            if (clock <= gate <=
                                    (clock + timedelta(minutes=10)) and
                                    not exists):
                                exists = clock
                        if not exists:
                            message = WITHOUT_CHECK_EXIT_ALERT.format(
                                empis[pis], gate.strftime('%d/%m/%Y %H:%M'))
                            if emp not in to_return.keys():
                                to_return[emp] = {}
                            to_return[emp][gate] = (
                                (send_normal_xmpp_message, [message]),
                                (send_gleyber_xmpp_message, [message]))
    for pis in gate_checks2.keys():
        if pis in clock_checks.keys():
            if 'E' in gate_checks2[pis].keys():
                for gate in gate_checks2[pis]['E']:
                    if ((datetime.now() - timedelta(hours=1)) <= gate <
                            (datetime.now() - timedelta(minutes=40))):
                        exists = None
                        for clock in clock_checks[pis]:
                            if ((gate <= clock <= (gate + timedelta(
                                    minutes=40))) and not exists):
                                exists = clock
                        if not exists:
                            message = WITHOUT_CHECK_ON_40M_ALERT.format(
                                empis[pis], gate.strftime('%d/%m/%Y %H:%M'))
                            if emp not in to_return.keys():
                                to_return[emp] = {}
                            to_return[emp][gate] = (
                                (send_normal_xmpp_message, [message]),
                                (send_gleyber_xmpp_message, [message]))
    return sorted(portal_table, key=lambda x: x[0]), to_return


def verify_automatic_reports(now, executed):
    """
Verify if exists any automatic report to be executed.
    :param now: hour to search report
    :param executed: array contening executed reports
    :return: data to be sended through e-mail or xmpp.
    """
    to_return = []
    qry = Query(Report)
    if not executed:
        qry.get(execution_start__lte=(now + timedelta(minutes=1)).time(),
                recipients__not=None,
                is_widget=False)
    else:
        qry.get(execution_start__gte=(now - timedelta(minutes=1)).time(),
                execution_start__lte=(now + timedelta(minutes=1)).time(),
                recipients__not=None, is_widget=False,
                id__not_in=tuple(executed))
    for report in qry:
        executed.append(report.id)
        connection = report.report_connection()
        rset = connection.query_with_columns(report.script)
        if len(rset[1]) > 0:
            header = ';'.join(rset[0]) + '\n'
            body_email = ''
            messages = []
            for row in rset[1]:
                body_email += ';'.join(all_as_str(row)) + '\n'
                messages.append(' '.join(all_as_str(row)))
            content = header + body_email
            for rec in report.recipients.replace(' ', '').split(','):
                if '@' in rec:
                    domain = rec.split('@')[1]
                    if domain == 'casanorte.com.br':
                        to_return.append([send_mail,
                                          [[rec], content, 'relatorio.csv',
                                           report.title]])
                    elif domain == 'casanorte.vpn':
                        for msg in messages:
                            to_return.append(
                                [send_xmpp_message,
                                 [[rec], '[' + report.title + ']\n' + msg]])
    return executed, to_return


def verify_drivers():
    """
Verify drivers occurences.
    :return: data to be sended to e-mail.
    """
    drvrs_chcks = {}
    header_content = ('Data;Funcionário;Entrada 1;Saída 1;Entrada 2;'
                      'Saída 2;Ocorrência\n')
    attachment_content = ''
    connection = create_mss_driver()
    d1 = (date.today() - timedelta(days=30)).strftime('%m/%d/%Y')
    d2 = (date.today() - timedelta(days=1)).strftime('%m/%d/%Y')
    rset = connection.query(QUERY_VERIFY_DRIVERS.format(d1, d2))
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in drvrs_chcks.keys():
            drvrs_chcks[data[0]] = {}
        if not data[1].date() in drvrs_chcks[data[0]].keys():
            drvrs_chcks[data[0]][data[1].date()] = []
        drvrs_chcks[data[0]][data[1].date()].append(data[1].time())
    for drive in sorted(drvrs_chcks.keys()):
        for dt in sorted(drvrs_chcks[drive]):
            error = None
            journey_1 = HDateTime.dif_time(
                drvrs_chcks[drive][dt][1],
                drvrs_chcks[drive][dt][0])
            journey_2 = time(0)
            rest = time(0)
            if len(drvrs_chcks[drive][dt]) == 4:
                journey_2 = HDateTime.dif_time(
                    drvrs_chcks[drive][dt][3],
                    drvrs_chcks[drive][dt][2])
                rest = HDateTime.dif_time(drvrs_chcks[drive][dt][2],
                                          drvrs_chcks[drive][dt][1])
            else:
                drvrs_chcks[drive][dt].append('')
                drvrs_chcks[drive][dt].append('')
            worked = HDateTime.sum_times(journey_1, journey_2)
            if journey_2 != time(0) and worked > time(9, 50):
                error = ("Extrapolou jornada de trabalho. Trabalhou "
                         "%s Horas." % (worked.strftime("%H:%M")))
            elif journey_1 > time(5, 50):
                error = ("Extrapolou jornada 1 de trabalho. "
                         "Trabalhou %s Horas." %
                         (journey_1.strftime("%H:%M")))
            elif journey_2 > time(5, 50):
                error = ("Extrapolou jornada 2 de trabalho. "
                         "Trabalhou %s Horas." %
                         (journey_2.strftime("%H:%M")))
            if rest != time(0) and rest < time(1, 20):
                error = ("Encurtou horário de almoço. "
                         "Descansou %s Horas." % (rest.strftime("%H:%M")))
            if error:
                attachment_content += '{};{};{};{};{};{};{}\n'.format(
                    dt.strftime("%d/%m/%Y"), drive,
                    if_not_time(drvrs_chcks[drive][dt][0], ''),
                    if_not_time(drvrs_chcks[drive][dt][1], ''),
                    if_not_time(drvrs_chcks[drive][dt][2], ''),
                    if_not_time(drvrs_chcks[drive][dt][3], ''), error)
    rset = connection.query(QUERY_VERIFY_DRIVERS_WO_CHECKS)
    while len(rset) > 0:
        data = rset.pop(0)
        attachment_content += '{};{};{};{};{};{};{}\n'.format(
            '', data[1], '', '', '', '', ('Não Tem marcação de ponto nos '
                                          'últimos 4 dias!'))
    connection.close()
    if attachment_content != '':
        header_content += '\n'.join(
            sorted(attachment_content.split('\n'),
                   key=lambda x:
                   (datetime.strptime(x.split(';')[0], '%d/%m/%Y').date()
                    if x.split(';')[0] != '' else date.today())))
        return (send_gleyber_mail,
                [header_content, 'Relatorio Motoristas.csv',
                 'Relatório diário de horário dos motoristas'])
    else:
        return None


def verify_drivers_occurrence(data_inicio, data_fim):
    """
Get occurrences into period.
    :return: data to be displayed at web page.
    """
    d1 = datetime.combine(data_inicio - timedelta(days=3),
                          time(0)).strftime('%Y-%m-%d %H:%M:%S')
    d2 = datetime.combine(data_fim + timedelta(days=3),
                          time(23, 59)).strftime('%Y-%m-%d %H:%M:%S')
    emp_data = {}
    dates_list = {}
    hours_table = {}
    journey_table = {}
    exclusion_table = {}
    connection = create_mss_driver()
    rset = connection.query(QUERY_JOURNEY)
    to_return = []
    while len(rset) > 0:
        data = rset.pop(0)
        journey_table[data[0]] = datetime.strptime(
            '01/01/11 ' + data[1].split('.')[0], '%d/%m/%y %H:%M:%S').time()
    rset = connection.query(QUERY_OCCURRENCE_HOURS_TABLE)
    while len(rset) > 0:
        data = rset.pop(0)
        dia = datetime.strptime(data[1], '%Y-%m-%d').date()
        if not data[0] in hours_table.keys():
            hours_table[data[0]] = {}
        if not dia in hours_table[data[0]].keys():
            hours_table[data[0]][dia] = {}
        if (dia.weekday() + 1) != data[4]:
            if (dia.weekday() + 1) < data[2]:
                continue
            hours_table[data[0]][dia][(dia.weekday() + 1)] = (
                data[3], data[4])
        else:
            hours_table[data[0]][dia][data[2]] = (data[3], data[4])
    rset = connection.query("""
SELECT F.nome,
       B.datahora,
       D.descricao,
       f.codfunc
FROM   Bilhetes B
       INNER JOIN funcionarios F ON b.codfunc = f.codfunc
       INNER JOIN departamentos D ON ( F.coddepto = D.coddepto )
WHERE  B.datahora between '{}' and '{}'
    """.format(d1, d2))
    while len(rset) > 0:
        data = rset.pop(0)
        str_dep = " [" + (
            (data[2]) if not data[2] is None else "Sem Departamento") + "]"
        if data[0] + str_dep in dates_list.keys():
            dates_list[data[0] + str_dep]['batidas'].append((data[1]))
        else:
            dates_list[data[0] + str_dep] = {}
            dates_list[data[0] + str_dep]['batidas'] = [(data[1])]
            dates_list[data[0] + str_dep]['cartao'] = data[3]
    for emp in dates_list.keys():
        while len(dates_list[emp]['batidas']) > 0:
            last = dates_list[emp]['batidas'][-1]
            array_aux = []
            while len(dates_list[emp]['batidas']) > 0:
                date_hour = dates_list[emp]['batidas'].pop(0)
                if (date_hour >= (last - timedelta(hours=12)) and
                            last >= (datetime.combine(
                            data_inicio,
                            time(0)) - timedelta(hours=10, minutes=50))):
                    if emp in emp_data.keys():
                        if last in emp_data[emp].keys():
                            emp_data[emp][last]['batidas'].append(date_hour)
                        else:
                            emp_data[emp][last] = {}
                            emp_data[emp][last]['batidas'] = [date_hour]
                            emp_data[emp][last]['erros'] = ''
                            emp_data[emp][last]['jornada'] = time(0)
                    else:
                        emp_data[emp] = {}
                        emp_data[emp][last] = {}
                        emp_data[emp][last]['batidas'] = [date_hour]
                        emp_data[emp][last]['erros'] = ''
                        emp_data[emp][last]['jornada'] = time(0)
                        emp_data[emp]['cartao'] = dates_list[emp]['cartao']
                else:
                    array_aux.append(date_hour)
            if emp in emp_data.keys():
                if (emp_data[emp]['cartao'] in hours_table.keys() and
                            last in emp_data[emp].keys()):
                    array_datas_iniciais = []
                    for data_inicial in \
                            hours_table[emp_data[emp]['cartao']].keys():
                        if emp_data[emp][last]['batidas'][
                            0].date() >= data_inicial:
                            array_datas_iniciais.append(data_inicial)
                    array_datas_iniciais.sort()
                    count = 0
                    while count != (len(array_datas_iniciais) * -1):
                        count -= 1
                        if emp_data[emp][last]['batidas'][0].date() == \
                                array_datas_iniciais[count]:
                            if (emp_data[emp][last]['batidas'][
                                    0].date().weekday() + 1) in \
                                    hours_table[emp_data[emp]['cartao']][
                                        array_datas_iniciais[count]].keys():
                                emp_data[emp][last]['jornada'] = \
                                    journey_table[
                                        hours_table[
                                            emp_data[emp]['cartao']][
                                            array_datas_iniciais[count]][
                                            (emp_data[emp][last][
                                                 'batidas'][
                                                 0].date().weekday() + 1)][
                                            0]]
                                count = (len(array_datas_iniciais) * -1)
                        elif (emp_data[emp][last]['batidas'][
                                  0].date().weekday() + 1) in \
                                hours_table[emp_data[emp]['cartao']][
                                    array_datas_iniciais[count]].keys():
                            emp_data[emp][last]['jornada'] = \
                                journey_table[
                                    hours_table[emp_data[emp]['cartao']][
                                        array_datas_iniciais[count]][
                                        (emp_data[emp][last]['batidas'][
                                             0].date().weekday() + 1)][0]]
                            count = (len(array_datas_iniciais) * -1)
            if emp in exclusion_table.keys():
                if exclusion_table[emp] == len(array_aux):
                    break
                else:
                    exclusion_table[emp] = len(array_aux)
            else:
                exclusion_table[emp] = len(array_aux)
            dates_list[emp]['batidas'] = array_aux
        if emp in emp_data.keys():
            emp_data[emp].pop('cartao')
            for dia in emp_data[emp].keys():
                emp_data[emp][dia]['n_checks'] = []
                for hora_agora in emp_data[emp][dia]['batidas']:
                    emp_data[emp][dia]['n_checks'].append(
                        hora_agora.strftime('%H:%M:%S'))
                while len(emp_data[emp][dia]['n_checks']) < 4:
                    emp_data[emp][dia]['n_checks'].append('')
                for algum_dia in emp_data[emp].keys():
                    if (algum_dia < dia and
                                (emp_data[emp][dia]['batidas'][0] -
                                     emp_data[emp][algum_dia]['batidas'][
                                         -1]).total_seconds() < EHIS):
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Interjornada < 11H'])
                if len(emp_data[emp][dia]['batidas']) in (1, 3):
                    to_return.append(
                        [emp_data[emp][dia]['batidas'][0].strftime(
                            '%d/%m/%Y'), emp,
                            emp_data[emp][dia]['n_checks'][0],
                            emp_data[emp][dia]['n_checks'][1],
                            emp_data[emp][dia]['n_checks'][2],
                            emp_data[emp][dia]['n_checks'][3],
                            'Falta marcação'])
                elif len(emp_data[emp][dia]['batidas']) == 2:
                    worked_time = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][1] -
                         emp_data[emp][dia]['batidas'][0]).total_seconds())
                    if (emp_data[emp][dia]['batidas'][1] -
                            emp_data[emp][dia]['batidas'][
                                0]).total_seconds() / 3600 > 6:
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Excedeu o limite de 2H [{} - jornada {'
                                '}]'.format(
                                    HDateTime.timefsecs(
                                        (emp_data[emp][dia]['batidas'][1] -
                                         emp_data[emp][dia]['batidas'][
                                             0]).total_seconds()).strftime(
                                        '%H:%M:%S'),
                                    emp_data[emp][dia]['jornada'].strftime(
                                        '%H:%M:%S'))])
                elif len(emp_data[emp][dia]['batidas']) >= 4:
                    descanco = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][2] -
                         emp_data[emp][dia]['batidas'][1]).total_seconds())
                    worked_time = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][1] -
                         emp_data[emp][dia]['batidas'][0]).total_seconds() +
                        (emp_data[emp][dia]['batidas'][3] -
                         emp_data[emp][dia]['batidas'][2]).total_seconds())
                    try:
                        hora_extra = HDateTime.dif_time(
                            worked_time,
                            emp_data[emp][dia][
                                'jornada']) if worked_time > \
                                               emp_data[emp][dia][
                                                   'jornada'] else time(0)
                    except Exception as e:
                        print(e)
                    if time(1) > descanco or descanco > time(2, 10):
                        emp_data[emp][dia][
                            'erros'] += ' Intervalo com erro [{' \
                                        '}]|'.format(
                            descanco.strftime('%H:%M:%S'))
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Intervalo com erro [{}]'.format(
                                    descanco.strftime('%H:%M:%S'))])
                    if hora_extra > time(2):
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Excedeu o limite de 2H [{} - jornada {'
                                '}]'.format(
                                    hora_extra.strftime('%H:%M:%S'),
                                    emp_data[emp][dia][
                                        'jornada'].strftime(
                                        '%H:%M:%S'))])
                    worked_time = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][1] -
                         emp_data[emp][dia]['batidas'][0]).total_seconds())
                    if (emp_data[emp][dia]['batidas'][3] -
                            emp_data[emp][dia]['batidas'][
                                2]).total_seconds() / 3600 > 6:
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                ('Excedeu o limite de 2H (segunda jornada) '
                                 '[{} - jornada {}]').format(
                                    HDateTime.timefsecs(
                                        (emp_data[emp][dia]['batidas'][3] -
                                         emp_data[emp][dia]['batidas'][
                                             2]).total_seconds()).strftime(
                                        '%H:%M:%S'),
                                    emp_data[emp][dia]['jornada'].strftime(
                                        '%H:%M:%S'))])
    return to_return


def verify_extra_hours():
    """
Verify extra hours.
    :return: occurences to be sended by XMPP messages.
    """
    d1 = (datetime.now() - timedelta(days=3)).strftime('%y%m%d%H%M')
    d2 = datetime.now().strftime('%y%m%d%H%M')
    clock_checks = {}
    empis = {}
    emp_data = {}
    date_lists = {}
    hours_table = {}
    journey_tables = {}
    women_pis = []
    connection_clock = create_mss_inner()
    connection_bifrost = create_sige()
    to_return = {}
    rset = connection_bifrost.query("SELECT PIS_NUMBER FROM BF_EMPLOYEES "
                                    "WHERE SEX = 'F'")
    while len(rset) > 0:
        data = rset.pop(0)
        women_pis.append(str(data[0]))
    rset = connection_clock.query(QUERY_PIS)
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in empis.keys() and data[0] in women_pis:
            empis[data[1]] = data[0]
    rset = connection_clock.query(QUERY_JOURNEY)
    while len(rset) > 0:
        data = rset.pop(0)
        journey_tables[data[0]] = data[1]
    rset = connection_clock.query(QUERY_HOURS_TABLE)
    while len(rset) > 0:
        data = rset.pop(0)
        if not data[0] in hours_table.keys():
            hours_table[data[0]] = {}
        hours_table[data[0]][data[1]] = data[2]
    rset = connection_clock.query(QUERY_CHECKS.format(d1, d2))
    while len(rset) > 0:
        data = rset.pop(0)
        str_dep = " [{}]".format(data[2] if data[2] else "Sem Departamento")
        if not data[0] + str_dep in empis.keys():
            continue
        if data[0] + str_dep in date_lists.keys():
            date_lists[data[0] + str_dep]['batidas'].append(
                datetime.strptime((data[1]), '%y%m%d%H%M'))
        else:
            date_lists[data[0] + str_dep] = {}
            date_lists[data[0] +
                       str_dep]['batidas'] = [datetime.strptime((data[1]),
                                                                '%y%m%d%H%M')]
            date_lists[data[0] + str_dep]['cartao'] = data[3]
        if data[4] in clock_checks.keys():
            clock_checks[data[4]].append(datetime.strptime((data[1]),
                                                           '%y%m%d%H%M'))
        else:
            clock_checks[data[4]] = [datetime.strptime((data[1]),
                                                       '%y%m%d%H%M')]
    for emp in date_lists.keys():
        newer = date_lists[emp]['batidas'][-1]
        last_check_last_day = None
        first_check_today = None
        while len(date_lists[emp]['batidas']) > 0:
            data = date_lists[emp]['batidas'].pop(0)
            if (data >= (newer - timedelta(hours=12)) and
                        newer >= (datetime.now() - timedelta(minutes=650))):
                if emp in emp_data.keys():
                    if (len(emp_data[emp]['batidas']) == 1 and
                                (data - first_check_today) > timedelta(
                                hours=7)):
                        last_check_last_day = first_check_today
                        emp_data[emp]['batidas'] = [data.time()]
                        first_check_today = data
                    else:
                        emp_data[emp]['batidas'].append(
                            data.time())
                else:
                    emp_data[emp] = {}
                    emp_data[emp]['batidas'] = [data.time()]
                    emp_data[emp]['trabalhada'] = time(0)
                    emp_data[emp]['extra'] = time(0)
                    emp_data[emp]['cartao'] = date_lists[emp]['cartao']
                    first_check_today = data
            else:
                last_check_last_day = data
    for emp in emp_data.keys():
        hours_list = emp_data[emp]['batidas']
        last_hour = None
        count = 0
        day_journey = None
        if emp_data[emp]['cartao'] in hours_table.keys():
            if hours_list[0] < datetime.now().time():
                seq = date.today().weekday() + 1
            else:
                seq = date.today().weekday()
            if seq in hours_table[emp_data[emp]['cartao']].keys():
                if (hours_table[emp_data[emp]['cartao']][seq] in
                        journey_tables.keys()):
                    day_journey = \
                        datetime.strptime('1/1/1111 ' + journey_tables[
                            hours_table[emp_data[emp]['cartao']][seq]
                        ].split('.')[0], '%d/%m/%Y %H:%M:%S').time()
                else:
                    day_journey = time(8)
            else:
                day_journey = time(8)
        else:
            day_journey = time(8)
        original_len = len(hours_list)
        while count < len(hours_list):
            if count % 2 == 0:
                last_hour = hours_list[count]
            else:
                dif = HDateTime.dif_time(hours_list[count], last_hour)
                emp_data[emp]['trabalhada'] = \
                    HDateTime.sum_times(emp_data[emp]['trabalhada'], dif)
            count += 1
        if count % 2 != 0 and count in (1, 3, 5):
            emp_data[emp]['trabalhada'] = \
                HDateTime.sum_times(
                    emp_data[emp]['trabalhada'],
                    HDateTime.dif_time(HDateTime.time_now(), last_hour))
        if (emp_data[emp]['trabalhada'] >= day_journey and
                    emp_data[emp]['trabalhada'].minute > 0):
            emp_data[emp]['extra'] = HDateTime.dif_time(
                emp_data[emp]['trabalhada'], day_journey)
        while len(hours_list) < 6:
            hours_list.append('')
        this_line = [emp]
        for h in hours_list:
            this_line.append(h.strftime('%H:%M:%S') if h is not '' else '')
        this_line.append(emp_data[emp]['trabalhada'].strftime('%H:%M:%S'))
        this_line.append(emp_data[emp]['extra'].strftime('%H:%M:%S'))
        this_line.append(day_journey.strftime('%H:%M:%S'))
        if emp_data[emp]['extra'] >= time(0, 10) and original_len % 2 != 0:
            this_time = time_between_10_120(emp_data[emp]['extra'])
            if this_time:
                message = EXTRA_HOUR_ALERT.format(
                    emp, emp_data[emp]['trabalhada'].strftime('%H:%M:%S'),
                    day_journey.strftime('%H:%M'))
                if emp not in to_return.keys():
                    to_return[emp] = {}
                to_return[emp][this_time] = (
                    (send_managers_xmpp_message, [message]))
    return to_return


def verify_faults():
    """
Verify employee faults.
    :return: data to be sended to e-mail.
    """
    header_content = 'DATA;NOME;TIPO;CARGO;DEPARTAMENTO\n'
    content = ''
    connection = create_mss_top()
    rset = connection.query(QUERY_VERIFY_FAULTS)
    while len(rset) > 0:
        data = rset.pop(0)
        content += '{};{};{};{};{}\n'.format(*data)
        connection.close()
    if content != '':
        return (send_gleyber_mail, [header_content + content, 'Faltas.csv',
                                    'Relatório diário de Faltas'])


def verify_gate_adjusts():
    """
Execute adjusts into gate database.
    """
    count = 0
    for script in SCRIPTS_DAILY_UPDATE:
        try:
            count += 1
            connection = create_mss_top()
            connection.command("SET XACT_ABORT ON")
            connection.command(script)
            connection.close()
        except:
            print("Script {} Não foi executado!".format(count))
            print(script)


def verify_occurrence(data_inicio, data_fim):
    """
Verify occurrences into period.
    :return: data to be displayed at web page.
    """
    d1 = datetime.combine(data_inicio - timedelta(days=3),
                          time(0)).strftime('%y%m%d%H%M')
    d2 = datetime.combine(data_fim + timedelta(days=3),
                          time(23, 59)).strftime('%y%m%d%H%M')
    emp_data = {}
    dates_list = {}
    hours_table = {}
    journey_table = {}
    exclusion_table = {}
    connection = create_mss_inner()
    rset = connection.query(QUERY_JOURNEY)
    to_return = []
    while len(rset) > 0:
        data = rset.pop(0)
        journey_table[data[0]] = datetime.strptime(
            '01/01/11 ' + data[1].split('.')[0], '%d/%m/%y %H:%M:%S').time()
    rset = connection.query(QUERY_OCCURRENCE_HOURS_TABLE)
    while len(rset) > 0:
        data = rset.pop(0)
        dia = datetime.strptime(data[1], '%Y-%m-%d').date()
        if not data[0] in hours_table.keys():
            hours_table[data[0]] = {}
        if not dia in hours_table[data[0]].keys():
            hours_table[data[0]][dia] = {}
        if (dia.weekday() + 1) != data[4]:
            if (dia.weekday() + 1) < data[2]:
                continue
            hours_table[data[0]][dia][(dia.weekday() + 1)] = (
                data[3], data[4])
        else:
            hours_table[data[0]][dia][data[2]] = (data[3], data[4])
    rset = connection.query(QUERY_CHECKS.format(d1, d2))
    while len(rset) > 0:
        data = rset.pop(0)
        str_dep = " [" + (
            (data[2]) if not data[2] is None else "Sem Departamento") + "]"
        if data[0] + str_dep in dates_list.keys():
            dates_list[data[0] + str_dep]['batidas'].append(
                datetime.strptime((data[1]), '%y%m%d%H%M'))
        else:
            dates_list[data[0] + str_dep] = {}
            dates_list[data[0] + str_dep]['batidas'] = [
                datetime.strptime((data[1]), '%y%m%d%H%M')]
            dates_list[data[0] + str_dep]['cartao'] = data[3]
    for emp in dates_list.keys():
        while len(dates_list[emp]['batidas']) > 0:
            last = dates_list[emp]['batidas'][-1]
            array_aux = []
            while len(dates_list[emp]['batidas']) > 0:
                date_hour = dates_list[emp]['batidas'].pop(0)
                if (date_hour >= (last - timedelta(hours=12)) and
                            last >= (datetime.combine(
                            data_inicio,
                            time(0)) - timedelta(hours=10, minutes=50))):
                    if emp in emp_data.keys():
                        if last in emp_data[emp].keys():
                            emp_data[emp][last]['batidas'].append(date_hour)
                        else:
                            emp_data[emp][last] = {}
                            emp_data[emp][last]['batidas'] = [date_hour]
                            emp_data[emp][last]['erros'] = ''
                            emp_data[emp][last]['jornada'] = time(0)
                    else:
                        emp_data[emp] = {}
                        emp_data[emp][last] = {}
                        emp_data[emp][last]['batidas'] = [date_hour]
                        emp_data[emp][last]['erros'] = ''
                        emp_data[emp][last]['jornada'] = time(0)
                        emp_data[emp]['cartao'] = dates_list[emp]['cartao']
                else:
                    array_aux.append(date_hour)
            if emp in emp_data.keys():
                if (emp_data[emp]['cartao'] in hours_table.keys() and
                            last in emp_data[emp].keys()):
                    array_datas_iniciais = []
                    for data_inicial in \
                            hours_table[emp_data[emp]['cartao']].keys():
                        if emp_data[emp][last]['batidas'][
                            0].date() >= data_inicial:
                            array_datas_iniciais.append(data_inicial)
                    array_datas_iniciais.sort()
                    count = 0
                    while count != (len(array_datas_iniciais) * -1):
                        count -= 1
                        if emp_data[emp][last]['batidas'][0].date() == \
                                array_datas_iniciais[count]:
                            if (emp_data[emp][last]['batidas'][
                                    0].date().weekday() + 1) in \
                                    hours_table[emp_data[emp]['cartao']][
                                        array_datas_iniciais[count]].keys():
                                emp_data[emp][last]['jornada'] = \
                                    journey_table[
                                        hours_table[
                                            emp_data[emp]['cartao']][
                                            array_datas_iniciais[count]][
                                            (emp_data[emp][last][
                                                 'batidas'][
                                                 0].date().weekday() + 1)][
                                            0]]
                                count = (len(array_datas_iniciais) * -1)
                        elif (emp_data[emp][last]['batidas'][
                                  0].date().weekday() + 1) in \
                                hours_table[emp_data[emp]['cartao']][
                                    array_datas_iniciais[count]].keys():
                            emp_data[emp][last]['jornada'] = \
                                journey_table[
                                    hours_table[emp_data[emp]['cartao']][
                                        array_datas_iniciais[count]][
                                        (emp_data[emp][last]['batidas'][
                                             0].date().weekday() + 1)][0]]
                            count = (len(array_datas_iniciais) * -1)
            if emp in exclusion_table.keys():
                if exclusion_table[emp] == len(array_aux):
                    break
                else:
                    exclusion_table[emp] = len(array_aux)
            else:
                exclusion_table[emp] = len(array_aux)
            dates_list[emp]['batidas'] = array_aux
        if emp in emp_data.keys():
            emp_data[emp].pop('cartao')
            for dia in emp_data[emp].keys():
                emp_data[emp][dia]['n_checks'] = []
                for hora_agora in emp_data[emp][dia]['batidas']:
                    emp_data[emp][dia]['n_checks'].append(
                        hora_agora.strftime('%H:%M:%S'))
                while len(emp_data[emp][dia]['n_checks']) < 4:
                    emp_data[emp][dia]['n_checks'].append('')
                for algum_dia in emp_data[emp].keys():
                    if (algum_dia < dia and
                                (emp_data[emp][dia]['batidas'][0] -
                                     emp_data[emp][algum_dia]['batidas'][
                                         -1]).total_seconds() < EHIS):
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Interjornada < 11H'])
                if len(emp_data[emp][dia]['batidas']) in (1, 3):
                    to_return.append(
                        [emp_data[emp][dia]['batidas'][0].strftime(
                            '%d/%m/%Y'), emp,
                            emp_data[emp][dia]['n_checks'][0],
                            emp_data[emp][dia]['n_checks'][1],
                            emp_data[emp][dia]['n_checks'][2],
                            emp_data[emp][dia]['n_checks'][3],
                            'Falta marcação'])
                elif len(emp_data[emp][dia]['batidas']) == 2:
                    worked_time = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][1] -
                         emp_data[emp][dia]['batidas'][0]).total_seconds())
                    if (emp_data[emp][dia]['batidas'][1] -
                            emp_data[emp][dia]['batidas'][
                                0]).total_seconds() / 3600 > 6:
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Excedeu o limite de 2H [{} - jornada {'
                                '}]'.format(
                                    HDateTime.timefsecs(
                                        (emp_data[emp][dia]['batidas'][1] -
                                         emp_data[emp][dia]['batidas'][
                                             0]).total_seconds()).strftime(
                                        '%H:%M:%S'),
                                    emp_data[emp][dia]['jornada'].strftime(
                                        '%H:%M:%S'))])
                elif len(emp_data[emp][dia]['batidas']) >= 4:
                    descanco = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][2] -
                         emp_data[emp][dia]['batidas'][1]).total_seconds())
                    worked_time = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][1] -
                         emp_data[emp][dia]['batidas'][0]).total_seconds() +
                        (emp_data[emp][dia]['batidas'][3] -
                         emp_data[emp][dia]['batidas'][2]).total_seconds())
                    try:
                        hora_extra = HDateTime.dif_time(
                            worked_time,
                            emp_data[emp][dia][
                                'jornada']) if worked_time > \
                                               emp_data[emp][dia][
                                                   'jornada'] else time(0)
                    except Exception as e:
                        print(e)
                    if time(1) > descanco or descanco > time(2, 10):
                        emp_data[emp][dia][
                            'erros'] += ' Intervalo com erro [{' \
                                        '}]|'.format(
                            descanco.strftime('%H:%M:%S'))
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Intervalo com erro [{}]'.format(
                                    descanco.strftime('%H:%M:%S'))])
                    if hora_extra > time(2):
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                'Excedeu o limite de 2H [{} - jornada {'
                                '}]'.format(
                                    hora_extra.strftime('%H:%M:%S'),
                                    emp_data[emp][dia][
                                        'jornada'].strftime(
                                        '%H:%M:%S'))])
                    worked_time = HDateTime.timefsecs(
                        (emp_data[emp][dia]['batidas'][1] -
                         emp_data[emp][dia]['batidas'][0]).total_seconds())
                    if (emp_data[emp][dia]['batidas'][3] -
                            emp_data[emp][dia]['batidas'][
                                2]).total_seconds() / 3600 > 6:
                        to_return.append(
                            [emp_data[emp][dia]['batidas'][0].strftime(
                                '%d/%m/%Y'), emp,
                                emp_data[emp][dia]['n_checks'][0],
                                emp_data[emp][dia]['n_checks'][1],
                                emp_data[emp][dia]['n_checks'][2],
                                emp_data[emp][dia]['n_checks'][3],
                                ('Excedeu o limite de 2H (segunda jornada) '
                                 '[{} - jornada {}]').format(
                                    HDateTime.timefsecs(
                                        (emp_data[emp][dia]['batidas'][3] -
                                         emp_data[emp][dia]['batidas'][
                                             2]).total_seconds()).strftime(
                                        '%H:%M:%S'),
                                    emp_data[emp][dia]['jornada'].strftime(
                                        '%H:%M:%S'))])
    return to_return


def verify_os_productivity():
    """
Get employees OS productivity.
    :return: data to be sended to e-mail
    """
    connection = create_oracle()
    employees = {'CONFERENCIA': {}, 'EMBALADOR': {}, 'SEPARACAO': {}}
    dates = []
    result = connection.query(QUERY_OS_PRODUCTIVITY)
    lines = 'TIPO;NOME;FUNCAO;'
    while len(result) > 0:
        data = result.pop(0)
        if not data[2] in dates:
            dates.append(data[2])
        if not data[0] in employees[data[3]].keys():
            employees[data[3]][data[0]] = {}
        employees[data[3]][data[0]][data[2]] = data[1]
    lines += ';'.join([x.strftime('%d/%m/%Y') for x in sorted(dates)]) + '\n'
    for chave in sorted(employees.keys()):
        for chave2 in sorted(employees[chave].keys()):
            lines += '{};{};'.format(chave, chave2)
            for data in sorted(dates):
                if data not in employees[chave][chave2].keys():
                    employees[chave][chave2][data] = ''
                lines += str(employees[chave][chave2][data]) + ';'
            lines += '\n'
    connection.close()
    if lines != 'TIPO;NOME;FUNCAO;':
        return (send_gleyber_mail, [lines, 'produtividade_30_dias.csv',
                                    'produtividade dos últimos 30 dias'])


def verify_week_journey():
    """
Verify if exists week journey different of 44H.
    :return: data to be sended to e-mail
    """
    connection = create_mss_top()
    content = header = 'Funcionário;Jornada Semanal\n'
    rset = connection.query(QUERY_WEEK_JOURNEY)
    for row in rset:
        employee = row.pop(0)
        tmp = []
        for hour in row:
            if isinstance(hour, str):
                tmp.append(datetime.strptime(hour.split('.')[0],
                                             '%H:%M:%S'))
        total_hour = [str(x) for x in HDateTime.amount_hours(*tmp)]
        if total_hour != ['44', '0', '0']:
            content += '{};{}\n'.format(employee, ':'.join(total_hour))
    if content != header:
        return (send_managers_mail, [content, 'Jornadas.csv',
                                     'Jornadas Diferentes de 44H'])
