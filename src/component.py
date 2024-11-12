'''
Template Component main class.

'''

import json
import logging
import os
import sys
from pathlib import Path

from kbc.env_handler import KBCEnvHandler

import result
from ms_graph.client import Client
from ms_graph.exceptions import BaseError, BadRequest
from result import ListDataResultWriter, ListResultWriter

# global constants'
# configuration variables
APP_SECRET = '#appSecret'
APP_KEY = 'appKey'
CONFIG_REFRESH_TOKEN = 'refresh_token'
STATE_REFRESH_TOKEN = "#refresh_token"
KEY_API_TOKEN = '#api_token'
KEY_BASE_HOST = 'base_host_name'
KEY_LISTS = 'lists'
KEY_LIST_SITE_REL_PATH = 'site_url_rel_path'
KEY_LIST_NAME = 'list_name'
KEY_LIST_INCLUDE_ADD_COLS = 'include_additional_cols'
KEY_USE_DISPLAY_NAMES = 'use_display_names'
KEY_LIST_LOAD_SETUP = 'load_setup'
KEY_LIST_LOAD_MODE = 'load_mode_incremental'
KEY_LIST_RESULT_NAME = 'result_table_name'

# #### Keep for debug
KEY_DEBUG = 'debug'
MANDATORY_PARS = [KEY_BASE_HOST, KEY_LISTS]
MANDATORY_IMAGE_PARS = []

OAUTH_APP_SCOPE = 'offline_access Files.Read Sites.Read.All'


class UserException(Exception):
    pass


def _initialize_client(refresh_tokens, app_key, app_secret):
    for refresh_token in refresh_tokens:
        try:
            client = Client(refresh_token=refresh_token, client_id=app_key,
                            client_secret=app_secret, scope=OAUTH_APP_SCOPE)
            return client
        except BadRequest as exc:
            logging.exception(f"Refresh token failed, retrying connection with new refresh token. {exc}")
            pass
    raise UserException('Authentication failed, reauthorize the extractor in extractor configuration!')


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None
        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            for ls in self.cfg_params[KEY_LISTS]:
                self.validate_parameters(ls,
                                         [KEY_LIST_NAME, KEY_LIST_LOAD_SETUP], 'list')
                # self.validate_parameters(ls[KEY_LIST_LOAD_SETUP],
                #                          [KEY_LIST_LOAD_MODE, KEY_LIST_RESULT_NAME], 'list')
                # normalize config - structure used for UI
                ls[KEY_LIST_LOAD_MODE] = ls[KEY_LIST_LOAD_SETUP][KEY_LIST_LOAD_MODE]
                ls[KEY_LIST_RESULT_NAME] = ls[KEY_LIST_LOAD_SETUP][KEY_LIST_RESULT_NAME]

        except ValueError as e:
            logging.exception(e)
            exit(1)

        refresh_tokens = []

        previous_state = self.get_state_file()
        refresh_token = previous_state.get(STATE_REFRESH_TOKEN)
        if refresh_token:
            refresh_tokens.append(refresh_token)

        authorization_data = json.loads(self.get_authorization().get('#data'))
        config_refresh_token = authorization_data.get(CONFIG_REFRESH_TOKEN)
        refresh_tokens.append(config_refresh_token)

        if not config_refresh_token:
            raise UserException('Missing refresh token in authorization data!')

        app_key = self.get_authorization()[APP_KEY]
        app_secret = self.get_authorization()[APP_SECRET]

        self.client = _initialize_client(refresh_tokens, app_key, app_secret)
        self.list_metadata_wr = ListResultWriter(self.tables_out_path)
        self.write_state_file({STATE_REFRESH_TOKEN: self.client.refresh_token})

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        for lst_par in params[KEY_LISTS]:
            try:
                logging.info(
                    f'Downloading list "{lst_par[KEY_LIST_NAME]}" '
                    f'from the site: {params[KEY_BASE_HOST] + lst_par[KEY_LIST_SITE_REL_PATH]}')
                logging.info('Validating site and list references...')
                site = self.client.get_site_by_relative_url(params[KEY_BASE_HOST], lst_par[KEY_LIST_SITE_REL_PATH])
                logging.debug(f'Site: {site}')
                if not site.get('id'):
                    raise RuntimeError(
                        f'No site with given url: '
                        f'{"/".join([params[KEY_BASE_HOST], lst_par[KEY_LIST_SITE_REL_PATH]])} found.')
                site_settings = self.client.get_site_settings(site['id'])
                logging.debug(f'Site settings: {site_settings}')
                sh_list = self.client.get_site_list_by_name(site['id'], lst_par[KEY_LIST_NAME])
                logging.debug(f'List detail: {sh_list}')
                if not sh_list:
                    raise RuntimeError(
                        f'No list named "{lst_par[KEY_LIST_NAME]}" found on site : '
                        f'{"/".join([params[KEY_BASE_HOST], lst_par[KEY_LIST_SITE_REL_PATH]])} .')

                logging.info('Getting list details...')
                list_columns = self.client.get_site_list_columns(site['id'], sh_list['id'],
                                                                 include_system=lst_par.get(KEY_LIST_INCLUDE_ADD_COLS,
                                                                                            False),
                                                                 use_display_colnames=lst_par.get(KEY_USE_DISPLAY_NAMES,
                                                                                                  True))
                logging.debug(f'List columns: {list_columns}')
                logging.info('Collecting list data...')
                data_results = self._collect_and_write_list(site['id'], sh_list, list_columns, lst_par)
                self.create_manifests(results=data_results, incremental=lst_par.get(KEY_LIST_LOAD_MODE, False))
            except BaseError as ex:
                logging.exception(ex)
                exit(1)

        logging.info('Writing metadata results')
        self.list_metadata_wr.close()
        metadata_tables = self.list_metadata_wr.collect_results()

        self.create_manifests(results=metadata_tables, incremental=True)
        logging.info('Extraction finished!')

    def _collect_and_write_list(self, site_id, sh_lst, list_columns, lst_par):
        data_wr = ListDataResultWriter(self.tables_out_path, list_columns, lst_par[KEY_LIST_RESULT_NAME])
        for fl in self.client.get_site_list_fields(site_id, sh_lst['id']):
            for f in fl:
                logging.debug(f"List row: {f}")
                if isinstance(f, list):
                    f = json.dumps(f)
                data_wr.write(f, user_values={result.LIST_ID: sh_lst['id']})
        # write metadata
        self.list_metadata_wr.write(sh_lst, user_values={result.SITE_ID: site_id,
                                                         result.RES_TABLE_NAME: lst_par[KEY_LIST_RESULT_NAME]})

        data_wr.close()
        return data_wr.collect_results()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as e:
        logging.exception(e)
        exit(1)
