import logging

import requests
from kbc.client_base import HttpClientBase
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ms_graph import exceptions


class Client(HttpClientBase):
    OAUTH_LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
    MAX_RETRIES = 10
    BASE_URL = 'https://graph.microsoft.com/v1.0/'
    BETA_BASE_URL = 'https://graph.microsoft.com/beta/'
    SYSTEM_LIST_COLUMNS = ["ComplianceAssetId",
                           "ContentType",
                           # "Modified",
                           # "Created",
                           # "Author",
                           # "Editor",
                           "Attachments",
                           "Edit",
                           "LinkTitleNoMenu",
                           "LinkTitle",
                           "DocIcon",
                           "ItemChildCount",
                           "FolderChildCount",
                           "AppAuthor",
                           "AppEditor"]

    def __init__(self, refresh_token, client_secret, client_id, scope):
        HttpClientBase.__init__(self, base_url=self.BASE_URL, max_retries=self.MAX_RETRIES, backoff_factor=0.3,
                                status_forcelist=(429, 503, 500, 502, 504))
        # refresh always on init
        self.__refresh_token = refresh_token
        self.__clien_secret = client_secret
        self.__client_id = client_id
        self.__scope = scope
        access_token, self.__refresh_token = self.request_tokens()
        # set auth header
        self._auth_header = {"Authorization": 'Bearer ' + access_token,
                             "Content-Type": "application/json"}

    @property
    def refresh_token(self):
        return self.__refresh_token

    def __response_hook(self, res, *args, **kwargs):
        # refresh token if expired
        if res.status_code == 401:
            access_token, refresh_token = self.request_tokens()
            # update auth header
            self._auth_header = {"Authorization": 'Bearer ' + access_token,
                                 "Content-Type": "application/json"}
            # reset header
            res.request.headers['Authorization'] = 'Bearer ' + access_token
            s = requests.Session()
            # retry request
            return self.requests_retry_session(session=s).send(res.request)

    def request_tokens(self):
        data = {"client_id": self.__client_id,
                "client_secret": self.__clien_secret,
                "refresh_token": self.__refresh_token,
                "grant_type": "refresh_token",
                "scope": self.__scope}
        r = requests.post(url=self.OAUTH_LOGIN_URL, data=data)
        parsed = self._parse_response(r, 'login')
        return parsed['access_token'], parsed['refresh_token']

    def requests_retry_session(self, session=None):
        session = session or requests.Session()
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
            allowed_methods=('GET', 'POST', 'PATCH', 'UPDATE')
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        # append response hook
        session.hooks['response'].append(self.__response_hook)
        return session

    def _get_paged_result_pages(self, endpoint, parameters):

        has_more = True
        next_url = self.base_url + endpoint
        while has_more:

            resp = self.get_raw(next_url, params=parameters)
            req_response = self._parse_response(resp, endpoint)

            if req_response.get('@odata.nextLink'):
                has_more = True
                next_url = req_response['@odata.nextLink']
                # the next url has parameters, so empty the parameters dict
                parameters = {}
            else:
                has_more = False

            yield req_response

    def get_site_by_relative_url(self, hostname, site_path):
        """

        :param hostname: e.g. mytenant.sharepoint.com
        :param site_path: e.g. /site/MyTeamSite
        :return:
        """
        url = self.base_url + f'/sites/{hostname}:/{site_path}'
        resp = self._parse_response(self.get_raw(url), 'sites')
        return resp

    def get_site_lists(self, site_id, filter=''):
        endpoint = f'/sites/{site_id}/lists'

        lists = []
        for ls in self._get_paged_result_pages(endpoint, {"$filter": filter}):
            lists.extend(ls['value'])
        return lists

    def get_site_list_by_name(self, site_id, list_name):
        """

        :param site_id: site id
        :param list_name: unique list name (case sensitive)
        :return: list object
        """
        lists = self.get_site_lists(site_id, f"displayName eq '{list_name}'")
        res_list = [ls for ls in lists if ls['displayName'] == list_name]

        return res_list[0] if res_list else None

    def get_site_settings(self, site_id):
        """
        Get site settings
        :param site_id:
        Returns: site settings
        """
        url = self.BETA_BASE_URL + f'sites/{site_id}/settings'
        return self._parse_response(self.get_raw(url), 'sites')

    def get_site_list_columns(self, site_id, list_id, include_system=False, use_display_colnames=True,
                              expand_par='columns(select=name, description, displayName, personOrGroup)'):
        """
        Gets array of columns available in the specified list.

        :param site_id:
        :param list_id:
        :param include_system:
        :param use_display_colnames:
        :param expand_par:
        :return:
        """
        endpoint = f'/sites/{site_id}/lists/{list_id}/columns'
        parameters = {}#{'expand': expand_par}

        columns = []
        for ls in self._get_paged_result_pages(endpoint, parameters):
            columns.extend(ls['columns'])

        logging.debug(f'Columns: {columns}')
        if not include_system:
            columns = [c for c in columns if
                       c['name'] not in self.SYSTEM_LIST_COLUMNS and not c['name'].startswith('_')]

        # convert Person type to lookupIds
        for col in columns:
            if col.get('personOrGroup') and not col.get('personOrGroup').get('allowMultipleSelection'):
                col['name'] = col['name'] + 'LookupId'

        if use_display_colnames:
            logging.info('Using display column names.')
            self._dedupe_header(columns)
        else:
            logging.info('Using unique API column names.')
            # use api names as display - already unique
            self._name_as_display_names(columns)
        return columns

    def get_site_list_fields(self, site_id, list_id):
        endpoint = f'/sites/{site_id}/lists/{list_id}/items'
        params = {'expand': 'fields'}
        for r in self._get_paged_result_pages(endpoint, params):
            logging.debug(f'List raw items: {r}')
            yield [f['fields'] for f in r['value']]

    def _parse_response(self, response, endpoint):
        status_code = response.status_code
        if 'application/json' in response.headers['Content-Type']:
            r = response.json()
        else:
            r = response.text
        if status_code in (200, 201, 202):
            return r
        elif status_code == 204:
            return None
        elif status_code == 400:
            raise exceptions.BadRequest(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 401:
            raise exceptions.Unauthorized(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 403:
            raise exceptions.Forbidden(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 404:
            raise exceptions.NotFound(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 405:
            raise exceptions.MethodNotAllowed(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 406:
            raise exceptions.NotAcceptable(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 409:
            raise exceptions.Conflict(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 410:
            raise exceptions.Gone(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 411:
            raise exceptions.LengthRequired(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 412:
            raise exceptions.PreconditionFailed(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 413:
            raise exceptions.RequestEntityTooLarge(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 415:
            raise exceptions.UnsupportedMediaType(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 416:
            raise exceptions.RequestedRangeNotSatisfiable(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 422:
            raise exceptions.UnprocessableEntity(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 429:
            raise exceptions.TooManyRequests(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 500:
            raise exceptions.InternalServerError(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 501:
            raise exceptions.NotImplemented(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 503:
            raise exceptions.ServiceUnavailable(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 504:
            raise exceptions.GatewayTimeout(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 507:
            raise exceptions.InsufficientStorage(f'Calling endpoint {endpoint} failed', r)
        elif status_code == 509:
            raise exceptions.BandwidthLimitExceeded(f'Calling endpoint {endpoint} failed', r)
        else:
            raise exceptions.UnknownError(f'Calling endpoint {endpoint} failed', r)

    def _dedupe_header(self, columns):
        col_keys = dict()
        dup_headers = set()
        for col in columns:
            if col['displayName'] in col_keys:
                dup_headers.add(col['displayName'])
                col['displayName'] = col['displayName'] + '_' + col['name']
            else:
                col_keys[col['displayName']] = col
        # update first value names as well
        for c in dup_headers:
            col_keys[c]['displayName'] = col_keys[c]['displayName'] + '_' + col_keys[c]['name']

    def _name_as_display_names(self, columns):
        for col in columns:
            col['displayName'] = col['name']
