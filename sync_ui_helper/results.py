#
# Copyright (c) 2012 Citrix Systems, Inc.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

import sys
import json

INVALID_JSON             = 1000
NO_COMMAND_FOUND         = 1001
INVALID_COMMAND          = 1002
NO_LOGIN_FOUND           = 1003
CONNECT_FAILED           = 1004
SYNC_ERROR               = 1005
DATABASE_ERROR           = 1006
NO_PROC_NAME_FOUND       = 1007
NOT_CONNECTED            = 1008
FN_RET_TYPE_INVALID      = 1009
NO_PACKAGE_NAME_FOUND    = 1010
NO_ID_FOUND              = 1011
NO_DICT_FOUND            = 1012
ALREADY_CONNECTED        = 1013

_MESSAGES = {
    INVALID_JSON:           "Input json line is invalid.",
    NO_COMMAND_FOUND:       "No command found in input line.",
    INVALID_COMMAND:        "Invalid command {0}.",
    NO_LOGIN_FOUND:         "Login details not found in connect command.",
    CONNECT_FAILED:         "Failed to connect to the database: {0}.\n"
                            "Check that ORACLE_HOME is set correctly.",
    SYNC_ERROR:             "User defined database error.",
    DATABASE_ERROR:         "Database error.",
    NO_PROC_NAME_FOUND:     "No procedure/function name found.",
    NOT_CONNECTED:          "Not connected to database.",
    FN_RET_TYPE_INVALID:    "Function return type {0} not supported.",
    NO_PACKAGE_NAME_FOUND:  "No package name found.",
    NO_ID_FOUND:            "No id found.",
    NO_DICT_FOUND:          "No key-value pairs found.",
    ALREADY_CONNECTED:      "Already connected to the database.",
}


class ExtendedEncoder(json.JSONEncoder):
    def default(self, obj):
        #handles both date and datetime objects
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(self, obj)


def format_cursor(cursor):
    field_nums = {}
    all_fields = []
    for i, column_desc in enumerate(cursor.description):
        field_name = column_desc[0].lower()
        field_nums[field_name] = i
        all_fields.append(field_name)

    rows = cursor.fetchall()
    data = []

    for row in rows:
        node = {}
        for field_name in all_fields:
            node[field_name] = row[field_nums[field_name]]
        data.append(node)

    return data

def write_result(result_dict):
    #there might be some errors in encoding e.g. for out_params it
    #may fail because json expects the keys to a dictionary to be strings
    try:
        result_str = json.dumps(result_dict, cls=ExtendedEncoder)
    except ValueError:
        result_str = "json_encoding_failed"
    result_str += "\n"
    sys.stdout.write(result_str)
    sys.stdout.flush()

def write_success_result(result_id,
                         return_val = None,
                         ret_cursor = None,
                         out_params = None):
    result_dict = {}
    result_dict['id'] = result_id
    result_dict['status'] = "success"
    if return_val is not None:
        result_dict['return_val'] = return_val
    if ret_cursor is not None:
        ret_list = format_cursor(ret_cursor)
        result_dict['return_val'] = ret_list
    if out_params is not None:
        result_dict['out_params'] = out_params
    write_result(result_dict)

def write_error_result(result_id = None,
                       code = None,
                       message_params = None,
                       oracle_code = None,
                       oracle_message = None):
    result_dict = {}
    if result_id is not None:
        result_dict['id'] = result_id
    result_dict['status'] = "error"
    if code is not None:
        result_dict['code'] = code
        try:
            msg_str = _MESSAGES[code].format(message_params)
        except (AttributeError, IndexError, KeyError):
            pass
        else:
            result_dict['message'] = msg_str
    if oracle_code is not None:
        result_dict['oracle_code'] = oracle_code
    if oracle_message is not None:
        result_dict['oracle_message'] = oracle_message
    write_result(result_dict)
