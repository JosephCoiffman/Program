import os
import jaydebeapi
import argparse
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

parser = argparse.ArgumentParser(description='Generate course data conforming with Renewal Central API format')
parser.add_argument('--date', '-d', metavar='Date', type=str, nargs='?', default=None,
                    help='export records specific to a date of the course [YYYY-mm-dd] e.g. 1997-09-29')
parser.add_argument('--db_path', '-db', metavar='Database Path', type=str, nargs='?', default='db.accdb',
                    help='Path to the database file (.accdb), default to db.accdb')
parser.add_argument('--out', '-o', metavar='Output Path', type=str, nargs='?', default='out.txt',
                    help='Path to the output file. Default to out.txt')
parser.add_argument('--password', '-p', metavar='Output Path', type=str, nargs='?', default='',
                    help='Password of the database')
parser.add_argument('--demo', dest='demo', action='store_true',
                    help='Activate demo mode, use course information from 2005-07-26 00:00:00 to fulfil all record lookup')
args = parser.parse_args()

# DB_PATH = os.getenv('DB_PATH') or 'db.accdb'
DB_PATH = args.db_path
MDB_JAR_PATH = os.getenv('MDB_JAR_PATH') or os.path.join(os.path.curdir, 'ucanaccess')
classpath_separator = ';' if os.name == 'nt' else ':'
ctx = jaydebeapi.connect(
    "net.ucanaccess.jdbc.UcanaccessDriver",
    f"jdbc:ucanaccess://{DB_PATH};newDatabaseVersion=V2010",
    ['', args.password],
    classpath_separator.join([
        os.path.join(MDB_JAR_PATH, 'ucanaccess-5.0.1.jar'),
        os.path.join(MDB_JAR_PATH, 'lib', 'commons-lang3-3.8.1.jar'),
        os.path.join(MDB_JAR_PATH, 'lib', 'commons-logging-1.2.jar'),
        os.path.join(MDB_JAR_PATH, 'lib', 'hsqldb-2.5.0.jar'),
        os.path.join(MDB_JAR_PATH, 'lib', 'jackcess-3.0.1.jar'),
    ])
)


def get_course_info(date: str):
    if args.demo:
        date = '2005-07-26 00:00:00'

    ret = []
    with ctx.cursor() as cursor:
        try:
            cursor.execute(f'SELECT * from COURSESKED WHERE course="{date}"')
            rows = cursor.fetchall()
            for r in rows:
                courseid, course, hours, day1, hours1, day2, hours2, day3, hours3, day4, hours4, title, comments, instructor = r
                ret.append({
                    'id': courseid,
                    'course': course,
                    'title': title,
                    'description': comments,
                    'type': instructor,
                    'hours': hours,
                    'completion_date': day1,
                    'renewal_date': day4
                })
        except Exception as ex:
            print(ex)
    return ret


def generate(license, name, title, description, hours, completion_date, course_type, renewal_date):
    if renewal_date is None or renewal_date == '':
        renewal_date = datetime.strftime(
            datetime.strptime(completion_date, '%Y-%m-%d %H:%M:%S') + relativedelta(years=2),
            '%Y-%m-%d %H:%M:%S'
        )
    cycle = str(int(renewal_date[:5]) - 2) + '-' + renewal_date[:5]
    [lastname, firstname] = [s.strip() for s in name.split(',')]
    return {
        'licensee_id': license,
        'licensee_name': f'{firstname} {lastname}',
        'course_title': title,
        'course_description': description,
        'course_hours': int(hours),
        'completion_date': completion_date,
        'cycle': cycle
    }


def get_courses(date=None):
    ret = []
    with ctx.cursor() as cursor:
        if date is not None:
            cursor.execute(f'SELECT * from COURSE WHERE course="{date} 00:00:00"')
        else:
            cursor.execute(f'SELECT * from COURSE')
        rows = cursor.fetchall()
        for r in rows:
            control_nbr, name, company, area, phone, address, city_zip, license, social, course, hours, fee, commission, payment, charged, sold_by, date1, hr1, date2, hr2, date3, hr3, date4, hr4, instructor, ready_to_print, certificate, printed, paid, state, fax = r
            if license is not None:
                license_match = re.match(r'.*PE([0-9]+)', license)
                if license_match is None:
                    continue
                license_number = license_match.group(1)
                license_number = license_number.zfill(7)
                license = f'PE{license_number}'
            infos = get_course_info(course)
            for info in infos:
                entry = generate(license=license, name=name, title=info['title'], description=info['description'],
                                 hours=info['hours'] or hours, completion_date=info['completion_date'] or course,
                                 course_type=info['type'], renewal_date=info['renewal_date'] or date4)
                ret.append(entry)

    return ret


if __name__ == '__main__':
    if args.demo:
        print('Demo mode activated')
    print(f'Date={args.date}')
    results = get_courses(date=args.date)
    with open(args.out, 'w') as f:
        json.dump(results, f, indent=4)
    print(f'Output written to {args.out}')
