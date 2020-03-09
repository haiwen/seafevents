import os
import sys
import configparser
import subprocess

from sqlalchemy import create_engine, text
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.event import contains as has_event_listener, listen as add_event_listener
from urllib.parse import quote_plus
from pytest import yield_fixture
from sqlalchemy.pool import Pool
from sqlalchemy.orm import sessionmaker

SEAHUB_DBNAME = ''
SEAFEVENTS_DBNAME = ''
TEST_DBNAME = ''


@yield_fixture(scope="module")
def test_db():
    delete_all_table_if_exists()
    # copy_db_from_seahub_with_no_data()
    # copy_db_from_seafevent_with_no_data()
    apply_tables()
    yield None
    # delete_all_table_if_exists()

def generate_tables_sql():
    seahub_db = read_db_conf('SEAHUBDB')
    seafevents_db = read_db_conf('SEAFEVENTSDB')
    connection_data = [seahub_db[0]]
    connection_data.extend(seahub_db[2:])
    connection_data = tuple(connection_data)
    cmd = "mysqldump -h%s -u%s -p%s --skip-add-locks --no-data --skip-add-drop-table --skip-comments %s > seahub.sql" % connection_data
    cwd = ["bash", "-c", cmd]
    subprocess.check_call(cwd, stdout=None, stderr=None)
    connection_data = [seafevents_db[0]]
    connection_data.extend(seafevents_db[2:])
    connection_data = tuple(connection_data)
    cmd = "mysqldump -h%s -u%s -p%s --skip-add-locks --no-data --skip-add-drop-table --skip-comments %s > seafevents.sql" % connection_data
    cwd = ["bash", "-c", cmd]
    subprocess.check_call(cwd, stdout=None, stderr=None)
    merge_sql_file('raw_table_sql.sql')

def merge_sql_file(filename):
    with open(filename, 'w') as fp:
        for fname in ['seahub.sql', 'seafevents.sql']:
            with open(fname) as tfp:
                fp.write(tfp.read())
                fp.write('\n')

def apply_tables():
    seafevents_db = read_db_conf('TESTDB')
    full_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'raw_table_sql.sql')
    cmd = "mysql -h %s -u%s -p%s %s < %s" % (seafevents_db[0], seafevents_db[2], seafevents_db[3], seafevents_db[4], full_path)
    cwd = ["bash", "-c", cmd]
    try:
        subprocess.check_call(cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as e:
        print(e.output)

def delete_all_table_if_exists():
    session = None


    try:
        session = get_db_session('TESTDB')
        session = get_db_session('TESTDB')
        sql = text('SET FOREIGN_KEY_CHECKS = 0;')
        session.execute(sql)
        sql = text('SELECT table_name FROM information_schema.tables where table_schema= :db;')
        tables = session.execute(sql, {'db': TEST_DBNAME}).fetchall()
        if tables:
            for tablename in tables:
                del_sql = text('drop table %s' % tablename[0])
                session.execute(del_sql)
        sql = text('SET FOREIGN_KEY_CHECKS = 1;')
        session.execute(sql)
    except Exception as e:
        sys.stdout.write(str(e))
    finally:
        if session:
            session.close()

def copy_db_from_seahub_with_no_data():
    test_session = None
    seahub_session = None
    try:
        test_session = get_db_session('TESTDB')
        seahub_session = get_db_session('SEAHUBDB')
        sql = text('SELECT table_name FROM information_schema.tables where table_schema= :db')
        tables = seahub_session.execute(sql, {'db': SEAHUB_DBNAME}).fetchall()
        if tables:
            for t_name in tables:
                create_sql = text('create table %s like %s' % (t_name[0], "{0}.{1}".format(SEAHUB_DBNAME, t_name[0])))
                test_session.execute(create_sql)
    except Exception as e:
        sys.stdout.write(str(e))
    finally:
        if seahub_session:
            seahub_session.close()
        if test_session:
            test_session.close()

def copy_db_from_seafevent_with_no_data():
    test_session = None
    seahub_session = None
    try:
        test_session = get_db_session('TESTDB')
        seahub_session = get_db_session('SEAFEVENTSDB')
        sql = text('SELECT table_name FROM information_schema.tables where table_schema= :db')
        tables = seahub_session.execute(sql, {'db': SEAFEVENTS_DBNAME}).fetchall()
        if tables:
            for t_name in tables:
                create_sql = text('create table %s like %s' % (t_name[0], "{0}.{1}".format(SEAFEVENTS_DBNAME, t_name[0])))
                test_session.execute(create_sql)
    except Exception as e:
        sys.stdout.write(str(e))
    finally:
        if seahub_session:
            seahub_session.close()
        if test_session:
            test_session.close()

def get_db_session(section):
    config = configparser.ConfigParser()
    config.read('./db.conf')
    if not config.has_section(section):
        sys.stdout.write("no section: %s" % section)
        return

    host, port, username, passwd, dbname = read_db_conf(section)
    db_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (username, quote_plus(passwd), host, port, dbname)
    global SEAHUB_DBNAME, SEAFEVENTS_DBNAME, TEST_DBNAME
    if section == 'TESTDB':
        TEST_DBNAME = dbname
    elif section == 'SEAFEVENTSDB':
        SEAFEVENTS_DBNAME = dbname
    elif section == 'SEAHUBDB':
        SEAHUB_DBNAME = dbname

    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)
    engine = create_engine(db_url, **kwargs)
    if not has_event_listener(Pool, 'checkout', ping_connection):
        add_event_listener(Pool, 'checkout', ping_connection)

    Session = sessionmaker(bind=engine)
    return Session()

def read_db_conf(section):
    config = configparser.ConfigParser()
    config.read('./db.conf')
    if not config.has_section(section):
        sys.stdout.write("no section: %s" % section)
        return
    if config.has_option(section, 'host'):
        host = config.get(section, 'host').lower()
    else:
        host = 'localhost'
    if config.has_option(section, 'port'):
        port = config.getint(section, 'port')
    else:
        port = 3306
    username = config.get(section, 'username')
    passwd = config.get(section, 'password')
    dbname = config.get(section, 'name')
    return (host, port, username, passwd, dbname)

def ping_connection(dbapi_connection, connection_record, connection_proxy): # pylint: disable=unused-argument
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.close()
    except:
        connection_proxy._pool.dispose() # pylint: disable=protected-access

        # Raise DisconnectionError so the pool would create a new connection
        raise DisconnectionError()
