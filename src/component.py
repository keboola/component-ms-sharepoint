'''
Template Component main class.

'''

import json
import logging
import sys

from kbc.env_handler import KBCEnvHandler

import result
from ms_graph.client import Client
from ms_graph.exceptions import BaseError
from result import ListDataResultWriter, ListResultWriter

# global constants'
# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_BASE_HOST = 'base_host_name'
KEY_LISTS = 'lists'
KEY_LIST_SITE_REL_PATH = 'site_url_rel_path'
KEY_LIST_NAME = 'list_name'
KEY_LIST_INCLUDE_ADD_COLS = 'include_additional_cols'
KEY_LIST_LOAD_MODE = 'load_mode_incremental'
KEY_LIST_RESULT_NAME = 'result_table_name'

# #### Keep for debug
KEY_DEBUG = 'debug'
MANDATORY_PARS = [KEY_LISTS]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            for l in self.cfg_params[KEY_LISTS]:
                self.validate_parameters(l,
                                         [KEY_LIST_SITE_REL_PATH, KEY_LIST_NAME, KEY_LIST_LOAD_MODE,
                                          KEY_LIST_RESULT_NAME], 'list')
        except ValueError as e:
            logging.exception(e)
            exit(1)

        authorization_data = json.loads(self.get_authorization().get('#data'))
        token = authorization_data.get('access_token')
        if not token:
            raise Exception('Missing access token in authorization data!')
        self.client = Client(token)
        self.list_metadata_wr = ListResultWriter(self.tables_out_path)

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        all_results = []
        for lst_par in params[KEY_LISTS]:
            try:
                site = self.client.get_site_by_relative_url(params[KEY_BASE_HOST], lst_par[KEY_LIST_SITE_REL_PATH])
                if not site.get('id'):
                    raise RuntimeError(
                        f'No site with given url: '
                        f'{"/".join([params[KEY_BASE_HOST], lst_par[KEY_LIST_SITE_REL_PATH]])} found.')

                list = self.client.get_site_list_by_name(site['id'], lst_par[KEY_LIST_NAME])
                if not list:
                    raise RuntimeError(
                        f'No list named "{lst_par[KEY_LIST_NAME]}" found on site : '
                        f'{"/".join([params[KEY_BASE_HOST], lst_par[KEY_LIST_SITE_REL_PATH]])} .')

                list_columns = self.client.get_site_list_columns(site['id'], list['id'],
                                                                 include_system=lst_par.get(KEY_LIST_INCLUDE_ADD_COLS,
                                                                                            False))
                data_results = self._collect_and_write_list(site['id'], list, list_columns, lst_par)
                all_results.extend(data_results)
            except BaseError as ex:
                logging.exception(ex)
                exit(1)

        self.list_metadata_wr.close()
        all_results.extend(self.list_metadata_wr.collect_results())

        self.create_manifests(results=all_results)

    def _collect_and_write_list(self, site_id, sh_lst, list_columns, lst_par):
        data_wr = ListDataResultWriter(self.tables_out_path, list_columns, lst_par[KEY_LIST_RESULT_NAME])
        for fl in self.client.get_site_list_fields(site_id, sh_lst['id']):
            data_wr.write_all(fl, user_values={result.LIST_ID: sh_lst['id']})
        # write metadata
        self.list_metadata_wr.write(sh_lst, user_values={result.SITE_ID: site_id})

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
