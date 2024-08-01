import os
import logging
from gevent.pool import Pool

from seafevents.repo_metadata.ai.utils.openai_api import OpenAIAPI, get_llm_url
from seafevents.repo_metadata.ai.utils.sdoc2md import sdoc2md
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.repo_metadata import EXCLUDED_PATHS
from seafevents.repo_metadata.utils import get_file_by_path
from seafevents.repo_metadata.utils import METADATA_TABLE


logger = logging.getLogger(__name__)



def gen_doc_summary(content):
    llm_url, url_type = get_llm_url()
    if url_type == 'proxy':
        openai_api = OpenAIAPI(llm_url)
    system_content = 'You are a document summarization expert. I need you to generate a concise summary of a document that is no longer than 40 words. The summary should capture the main points and themes of the document clearly and effectively.The output language is the same as the input language. If it seems there is no content provided for summarization, just output word: None'
    system_prompt = {"role": "system", "content": system_content}
    user_prompt = {"role": "user", "content": content}
    messages = [system_prompt, user_prompt]
    summary = openai_api.chat_completions(messages)
    return summary


def create_summary_of_sdoc_in_repo(repo_id):
    metadata_server_api = MetadataServerAPI('seafevents')
    sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}`'
    query_result = metadata_server_api.query_rows(repo_id, sql).get('results', [])
    updated_summary_rows = []

    def process_row(row):
        parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
        file_name = row[METADATA_TABLE.columns.file_name.name]
        if parent_dir == '/':
            path = parent_dir + file_name
        else:
            path = parent_dir + '/' + file_name
        if _is_excluded_path(path):
            return

        row_id = row[METADATA_TABLE.columns.id.name]
        _, ext = os.path.splitext(file_name)
        if ext == '.sdoc':
            sdoc_content = get_file_by_path(repo_id, path)
            md_content = sdoc2md(sdoc_content)
            summary_text = gen_doc_summary(md_content)
            if summary_text in ['None', 'none']:
                return

            updated_row = {
                METADATA_TABLE.columns.id.name: row_id,
                METADATA_TABLE.columns.summary.name: summary_text,
            }
            updated_summary_rows.append(updated_row)

    pool = Pool(50)
    logger.info(f'Start summarizing sdoc in repo {repo_id}')
    for row in query_result:
        pool.spawn(process_row, row)

    pool.join()

    if updated_summary_rows:
        metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_summary_rows)
    logger.info(f'Finish summarizing sdoc in repo {repo_id}')
    return {'success': True}


def _is_excluded_path(path):
    if not path or path == '/':
        return True
    for ex_path in EXCLUDED_PATHS:
        if path.startswith(ex_path):
            return True
