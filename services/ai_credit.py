import logging
from datetime import date

from sqlalchemy import text

from seafevents.app.config import ENABLED_ROLE_PERMISSIONS, ORG_MEMBER_QUOTA_DEFAULT


logger = logging.getLogger(__name__)


class AdditionalAICreditService:
    def get_monthly_credit(self, session, org_id):
        role_row = session.execute(
            text('SELECT role FROM organizations_orgsettings WHERE org_id=:org_id LIMIT 1'),
            {'org_id': org_id},
        ).fetchone()
        role = role_row[0] if role_row and role_row[0] else 'default'
        permissions = ENABLED_ROLE_PERMISSIONS.get(role)
        if permissions is None:
            permissions = ENABLED_ROLE_PERMISSIONS.get('default', {})

        try:
            credit_per_user = float(permissions.get('monthly_ai_credit_per_user', -1))
        except (TypeError, ValueError):
            logger.warning('Invalid monthly AI credit config for role %s', role)
            return -1

        if credit_per_user < 0:
            return -1

        quota_row = session.execute(
            text('SELECT quota FROM organizations_orgmemberquota WHERE org_id=:org_id LIMIT 1'),
            {'org_id': org_id},
        ).fetchone()
        member_quota = quota_row[0] if quota_row else ORG_MEMBER_QUOTA_DEFAULT
        return member_quota * credit_per_user

    def deduct_overflow_credits(self, session, org_id, cost_delta, today=None):
        monthly_credit = self.get_monthly_credit(session, org_id)
        if monthly_credit < 0:
            return 0

        month_start = (today or date.today()).replace(day=1)
        used_row = session.execute(text('''
            SELECT COALESCE(SUM(cost), 0)
            FROM ai_usage_statistics
            WHERE org_id=:org_id AND date>=:month_start
        '''), {
            'org_id': org_id,
            'month_start': month_start,
        }).fetchone()
        current_used_credit = float(used_row[0] or 0) * 100
        previous_used_credit = max(current_used_credit - cost_delta * 100, 0)
        overflow_credit = (
            max(current_used_credit - monthly_credit, 0)
            - max(previous_used_credit - monthly_credit, 0)
        )
        if overflow_credit <= 0:
            return 0

        credit_row = session.execute(
            text('SELECT credits FROM org_additional_ai_credit WHERE org_id=:org_id FOR UPDATE'),
            {'org_id': org_id},
        ).fetchone()
        if not credit_row:
            return 0

        current_additional_credit = float(credit_row[0] or 0)
        deducted_credit = min(current_additional_credit, overflow_credit)
        if deducted_credit <= 0:
            return 0

        session.execute(text('''
            UPDATE org_additional_ai_credit
            SET credits=:credits, updated_at=NOW()
            WHERE org_id=:org_id
        '''), {
            'credits': current_additional_credit - deducted_credit,
            'org_id': org_id,
        })
        return deducted_credit
