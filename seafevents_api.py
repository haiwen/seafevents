from seafevents.app.config import appconfig, load_config
from .statistics.db import get_file_ops_stats_by_day, get_user_activity_stats_by_day, \
     get_total_storage_stats_by_day, get_org_user_traffic_by_day, \
     get_user_traffic_by_day, get_org_traffic_by_day, get_system_traffic_by_day,\
     get_all_users_traffic_by_month, get_all_orgs_traffic_by_month, get_user_traffic_by_month

def init(config_file):
    if not appconfig.get('session_cls'):
        load_config(config_file)