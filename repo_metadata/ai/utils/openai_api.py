import requests
import logging
import json
import os

from seafevents.app.config import get_config
from seafevents.utils import get_opt_from_conf_or_env


logger = logging.getLogger(__name__)


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


def get_llm_url():
    section_name = 'AI'
    llm_url, url_type = None

    config_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR')
    if config_dir:
        config_file = os.path.join(config_dir, 'seafevents.conf')
    else:
        config_file = os.environ.get('EVENTS_CONFIG_FILE')
    
    if not config_file or not os.path.exists(config_file):
        return

    config = get_config(config_file)
    
    if not config.has_section(section_name):
        return

    llm_type = get_opt_from_conf_or_env(config, section_name, 'llm_type', 'LLM_TYPE')
    if llm_type == 'open-ai-proxy':
        llm_url = get_opt_from_conf_or_env(config, section_name, 'openai_proxy_url', 'OPENAI_PROXY_URL')
        url_type = 'proxy'

    if not llm_url:
        raise ValueError("llm_url not found in the configuration file or environment variables.")
    return llm_url, url_type


class OpenAIAPI:
    def __init__(self, openai_url, timeout=180):
        self.openai_url = openai_url.rstrip('/') + '/api/v1/chat-completions/create'
        self.timeout = timeout

    def chat_completions(self, messages, temperature=0):
        json_data = {
            'model': 'gpt-4o-mini',
            'messages': messages,
            'temperature': temperature
        }
        response = requests.post(self.openai_url, json=json_data, timeout=self.timeout)
        data = parse_response(response)
        try:
            result = data['choices'][0]['message']['content']
        except KeyError as e:
            logger.exception(e)
            result = None

        return result
