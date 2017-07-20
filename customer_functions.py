#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def role_mapping(role):
    if '科长' in role:
        return '科长'
    if '部长' in role:
        return '部长'
    else:
        return '员工'

