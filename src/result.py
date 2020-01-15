from kbc.result import ResultWriter, KBCTableDef

LIST_ID = 'list_id'
SITE_ID = 'site_id'


class ListResultWriter(ResultWriter):
    COLS = ["createdDateTime",
            "description",
            "eTag",
            "id",
            "lastModifiedDateTime",
            "name",
            "webUrl",
            "displayName",
            "createdBy_user"]
    TABLE_DEF = KBCTableDef(name='list_metadata', pk=['id', 'webUrl'], columns=COLS)

    def __init__(self, result_dir_path):
        ResultWriter.__init__(self, result_dir_path, self.TABLE_DEF, fix_headers=True, user_value_cols=[SITE_ID])

    def write(self, data, file_name=None, user_values=None, object_from_arrays=False, write_header=True):
        # flatten obj
        data['createdBy_user'] = data.get('createdBy', {}).get('user', {}).get('displayName')
        super().write(data, file_name, user_values, object_from_arrays, write_header)


class ListDataResultWriter(ResultWriter):

    def __init__(self, result_dir_path, column_mapping, result_name):
        ResultWriter.__init__(self, result_dir_path,
                              KBCTableDef(name=result_name + '_data', pk=['id', 'list_id'], columns=[]),
                              fix_headers=True, flatten_objects=False)
        self.column_mapping = column_mapping
        # override column names with display name
        self.table_def.columns = [c['displayName'] for c in column_mapping]
        # custom user added col
        self.user_value_cols = [LIST_ID]
        self.table_def.columns.append(LIST_ID)

    def write(self, data, file_name=None, user_values=None, object_from_arrays=False, write_header=True):
        # flatten obj
        data = self._change_col_names(data)
        super().write(data, user_values=user_values)

    def _change_col_names(self, data):
        """
        replace name with display names inplace
        :param data:
        :return:
        """

        for key in self.column_mapping:
            if key['name'] == 'ID':
                # because MS bullshit
                key['name'] = 'id'

            if data.get(key['name']):
                data[key['displayName']] = data.pop(key['name'])

        return data
