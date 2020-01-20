from kbc.client_base import HttpClientBase

from ms_graph import exceptions


class Client(HttpClientBase):
    MAX_RETRIES = 10
    BASE_URL = 'https://graph.microsoft.com/v1.0/'
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

    def __init__(self, access_token):
        HttpClientBase.__init__(self, base_url=self.BASE_URL, max_retries=self.MAX_RETRIES, backoff_factor=0.3,
                                status_forcelist=(429, 503, 500, 502, 504),
                                default_http_header={"Authorization": 'Bearer ' + access_token,
                                                     "Content-Type": "application/json"})

    def _get_paged_result_pages(self, endpoint, parameters):

        has_more = True
        next_url = self.base_url + endpoint
        while has_more:

            resp = self.get_raw(next_url, params=parameters)
            req_response = self._parse_response(resp, endpoint)

            if req_response.get('@odata.nextLink'):
                has_more = True
                next_url = req_response['@odata.nextLink']
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

    def get_site_lists(self, site_id):
        endpoint = f'/sites/{site_id}/lists'
        lists = []
        for l in self._get_paged_result_pages(endpoint, {}):
            lists.extend(l['value'])
        return lists

    def get_site_list_by_name(self, site_id, list_name):
        """

        :param site_id: site id
        :param list_name: unique list name (case sensitive)
        :return: list object
        """
        lists = self.get_site_lists(site_id)
        res_list = [l for l in lists if l['name'] == list_name]

        return res_list[0] if res_list else None

    def get_site_list_columns(self, site_id, list_id, include_system=False,
                              expand_par='columns(select=name, description, displayName)'):
        """
        Gets array of columns available in the specified list.

        :param site_id:
        :param list_id:
        :param include_system:
        :param expand_par:
        :return:
        """
        endpoint = f'/sites/{site_id}/lists/{list_id}'
        parameters = {'expand': expand_par}

        columns = []
        for l in self._get_paged_result_pages(endpoint, parameters):
            columns.extend(l['columns'])

        if not include_system:
            columns = [c for c in columns if
                       c['name'] not in self.SYSTEM_LIST_COLUMNS and not c['name'].startswith('_')]

        self._dedupe_header(columns)
        return columns

    def get_site_list_fields(self, site_id, list_id):
        endpoint = f'/sites/{site_id}/lists/{list_id}/items'
        params = {'expand': 'fields'}
        for r in self._get_paged_result_pages(endpoint, params):
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
