import math

from sqlalchemy import text

from seafevents.app.config import ENABLED_ROLE_PERMISSIONS
from seafevents.utils.seahub_db import SeahubDB


class AdditionalAICreditService:
    def get_monthly_credit(self, session, org_id):
        with SeahubDB(session) as db:
            role = db.get_org_role(org_id) or 'default'
            permissions = ENABLED_ROLE_PERMISSIONS.get(role, ENABLED_ROLE_PERMISSIONS.get('default', {}))
            credit_per_user = permissions.get('monthly_ai_credit_per_user', -1)
            if credit_per_user < 0:
                return -1
            return db.get_org_member_quota(org_id) * credit_per_user

    def prepare_debit(self, session, org_id, cost_delta, today):
        monthly_credit = self.get_monthly_credit(session, org_id)
        if monthly_credit < 0:
            return 0, 0

        # Serialize even usage covered by the role, so concurrent workers share the same boundary.
        session.execute(text('''
            INSERT INTO org_additional_ai_credit (org_id, balance, created_at, updated_at)
            VALUES (:org_id, 0, NOW(6), NOW(6))
            ON DUPLICATE KEY UPDATE org_id=org_id
        '''), {'org_id': org_id})
        balance_row = session.execute(
            text('SELECT balance FROM org_additional_ai_credit WHERE org_id=:org_id FOR UPDATE'),
            {'org_id': org_id},
        ).fetchone()
        balance = balance_row[0]

        cost_row = session.execute(text('''
            SELECT COALESCE(SUM(cost), 0)
            FROM ai_usage_statistics
            WHERE org_id=:org_id AND date>=:month_start AND date<=:today
        '''), {
            'org_id': org_id,
            'month_start': today.replace(day=1),
            'today': today,
        }).fetchone()
        cost_before = cost_row[0] or 0
        used_before = math.ceil(cost_before * 100)
        used_after = math.ceil((cost_before + cost_delta) * 100)
        debit = max(used_after - monthly_credit, 0) - max(used_before - monthly_credit, 0)
        return balance, debit

    def apply_debit(self, session, org_id, balance, debit):
        shortfall = max(debit - balance, 0)
        deducted_credit = min(balance, debit)
        if deducted_credit > 0:
            session.execute(text('''
                UPDATE org_additional_ai_credit
                SET balance=:balance, updated_at=NOW(6)
                WHERE org_id=:org_id
            '''), {'balance': balance - deducted_credit, 'org_id': org_id})
        return shortfall
