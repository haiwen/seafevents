import os 
import logging

from seafevents.app.config import get_config
from seafevents.utils import get_opt_from_conf_or_env


logger = logging.getLogger(__name__)


class RepoMetadataAIserver:
    def __init__(self):
        self.llm_url = None
        # Refer to diff llm model
        self.llm_type = None
        # Refer to llm api key
        self.llm_key = None

    def init(self, config):
        self._parse_config(config)

    def _parse_config(self, config):
        section_name = 'AI'
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

        self.llm_type = get_opt_from_conf_or_env(config, section_name, 'llm_type', 'LLM_TYPE')
        if self.llm_type == 'open-ai-proxy':
            self.llm_url = get_opt_from_conf_or_env(config, section_name, 'llm_url', 'LLM_URL')
            if not self.llm_url:
                logger.info("llm_url not found in the configuration file or environment variables.")

metadata_ai_server = RepoMetadataAIserver()
