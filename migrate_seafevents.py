import os
import sys


if len(sys.argv) != 2:
    print ('usage: python migrate_seafevents.py filename')
    sys.exit(-1)

install_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
top_dir = os.path.dirname(install_path)
central_config_dir = os.path.join(top_dir, 'conf')
sys.path.append (central_config_dir)

def init_seahub_db():
    try:
        import MySQLdb
        import seahub_settings
    except ImportError as e:
        print('Failed to init seahub db: %s.' %  e)
        return None
    try:
        db_infos = seahub_settings.DATABASES['default']
    except KeyError as e:
        print('Failed to init seahub db, can not find db info in seahub settings.')
        return None

    if db_infos.get('ENGINE') != 'django.db.backends.mysql':
        print('Failed to init seahub db, only mysql db supported.')
        return None

    db_host = db_infos.get('HOST', '127.0.0.1')
    db_port = int(db_infos.get('PORT', '3306'))
    db_name = db_infos.get('NAME')
    if not db_name:
        print('Failed to init seahub db, db name is not setted.')
        return None
    db_user = db_infos.get('USER')
    if not db_user:
        print('Failed to init seahub db, db user is not setted.')
        return None
    db_passwd = db_infos.get('PASSWORD')

    try:
        db_conn = MySQLdb.connect(host=db_host, port=db_port,
                                       user=db_user, passwd=db_passwd,
                                       db=db_name, charset='utf8')
        db_conn.autocommit(True)
        cursor = db_conn.cursor()
    except Exception as e:
        print('Failed to init seahub db: %s.' %  e)

    return db_conn

def migrate_user(cursor, old_user, new_user):
    try:
        cursor.execute('update Activity set op_user=%s where op_user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update Activity user to %s.' % new_user)

    try:
        cursor.execute('update UserActivity set username=%s where username=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update UserActivity user to %s.' % new_user)

    try:
        cursor.execute('update FileHistory set op_user=%s where op_user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update FileHistory user to %s.' % new_user)

    try:
        cursor.execute('update FileAudit set user=%s where user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update FileAudit user to %s.' % new_user)

    try:
        cursor.execute('update FileUpdate set user=%s where user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update FileUpdate user to %s.' % new_user)

    try:
        cursor.execute('update PermAudit set from_user=%s where from_user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update PermAudit from_user to %s.' % new_user)

    try:
        cursor.execute('update PermAudit set `to`=%s where `to`=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update PermAudit to_user to %s.' % new_user)

    try:
        cursor.execute('update UserTrafficStat set email=%s where email=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update UserTrafficStat email to %s.' % new_user)

    try:
        cursor.execute('update UserActivityStat set username=%s where username=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update UserActivityStat username to %s.' % new_user)

    try:
        cursor.execute('update UserTraffic set user=%s where user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update UserTraffic user to %s.' % new_user)

    try:
        cursor.execute('update MonthlyUserTraffic set user=%s where user=%s',

                            (new_user, old_user))
    except Exception as e:
        print('Failed to update MonthlyUserTraffic user to %s.' % new_user)

db_conn = init_seahub_db()
if not db_conn:
    sys.exit(-1)

cursor = None
try:
    cursor = db_conn.cursor()
except Exception as e:
    print('Failed to init seahub db: %s.' %  e)
    sys.exit(-1)

f = open(sys.argv[1])
strings = f.read()
ident_start = '$migrate$'
ident_end = '$to$'
while True:
    start = strings.find(ident_start)
    if start < 0:
        break

    if len(strings) < start + len(ident_start) + 1:
        break
    strings = strings[start+len(ident_start)+1:]

    next_space = strings.find(' ')
    if next_space < 0:
        break

    old_user =  strings[0:next_space]

    end = strings.find(ident_end)
    if end < 0:
        break

    if len(strings) < end + len(ident_end) + 1:
        break
    strings = strings[end+len(ident_end)+1:]

    next_space = strings.find(' ')
    if next_space  < 0:
        break

    new_user =  strings[0:next_space]

    print ("migrate %s to %s." % (old_user, new_user))
    migrate_user(cursor, old_user, new_user)

    if len(strings) <0:
        break

if cursor:
    cursor.close()
if db_conn:
    db_conn.close()
