from seaserv import seafserv_threaded_rpc
from seaserv import get_shared_groups_by_repo, get_group_members

def get_related_users_by_repo(repo_id):
    """Give a repo id, returns a list of users of:
    - the repo owner
    - members of groups to which the repo is shared
    - users to which the repo is shared
    """
    owner = seafserv_threaded_rpc.get_repo_owner(repo_id)
    users = [owner]

    groups = get_shared_groups_by_repo(repo_id)
    for group in groups:
        members = get_group_members(group.id)
        for member in members:
            if member.user_name not in users:
                users.append(member.user_name)

    share_repos = seafserv_threaded_rpc.list_share_repos(owner, 'from_email', -1, -1)
    for repo in share_repos:
        if repo.id == repo_id:
            if repo.shared_email not in users:
                users.append(repo.shared_email)

    return users
