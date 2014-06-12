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

import os
import sys
import results

import cx_Oracle


connection = None
object_type_cache = {}

def _connect(input_id, login_str):
    global connection
    global object_type_cache

    #flush the object type cache
    object_type_cache = {}

    if connection is not None:
        results.write_error_result(result_id = input_id,
                                   code = results.ALREADY_CONNECTED)
        return

    if not login_str:
        results.write_error_result(result_id = input_id,
                                   code = results.NO_LOGIN_FOUND)
        return

    try:
        connection = cx_Oracle.connect(login_str)
    except cx_Oracle.InterfaceError as error:
        results.write_error_result(result_id = input_id,
                                   code = results.CONNECT_FAILED,
                                   message_params = error)
        return
    #any other exception will be caught in the outer try block in main

    results.write_success_result(result_id = input_id)

def _disconnect(input_id):
    global connection
    global object_type_cache

    #flush the object type cache
    object_type_cache = {}

    if connection is not None:
        connection.close()
    connection = None
    results.write_success_result(result_id = input_id)

def _check_obj_type(input_id, package_name, object_name):
    global connection
    global object_type_cache

    try:
        obj_type = object_type_cache[(package_name, object_name)]
    except KeyError:
        #need to find out object type from database
        pass
    else:
        return obj_type

    if connection is None:
        results.write_error_result(result_id = input_id,
                                   code = results.NOT_CONNECTED)
        return

    #find out if object_name is a procedure or a function
    select_cursor = connection.cursor()
    select_cursor.execute(
                  "select upper(aa.data_type)"
                  "from (select object_id"
                  "      from user_objects"
                  "      where object_type = 'PACKAGE' and"
                  "            object_name = upper(:pack_name)"
                  "      union all"
                  "      select ao.object_id"
                  "      from user_synonyms us"
                  "           join all_objects ao on (us.table_owner = ao.owner and"
                  "                                   us.table_name = ao.object_name)"
                  "      where us.synonym_name = upper(:syn_name) and"
                  "            ao.object_type = 'PACKAGE') o"
                  "      join all_arguments aa on (o.object_id = aa.object_id)"
                  "where aa.object_name = upper(:obj_name)"
                  "and aa.position = 0"
                  "and aa.in_out = 'OUT'"
                  "and aa.data_level = 0"
                  "order by aa.sequence",
                  pack_name = package_name,
                  syn_name = package_name,
                  obj_name = object_name)

    return_val = select_cursor.fetchone()
    if return_val is not None:
        return_type = return_val[0]
    else:
        return_type = None

    select_cursor.close()
    object_type_cache[(package_name, object_name)] = return_type
    return return_type

def _execute_proc(input_id, package_name, proc_name, params):
    global connection

    if connection is None:
        results.write_error_result(result_id = input_id,
                                   code = results.NOT_CONNECTED)
        return

    return_type = _check_obj_type(input_id, package_name, proc_name)
    cursor = connection.cursor()
    full_name = package_name + "." + proc_name

    if return_type is not None:
        #call function
        if return_type == "REF CURSOR":
            cx_ora_type = cx_Oracle.CURSOR
        elif return_type == "VARCHAR2":
            cx_ora_type = cx_Oracle.STRING
        else:
            results.write_error_result(result_id = input_id,
                                       code = results.FN_RET_TYPE_INVALID,
                                       message_params = return_val)
            return

        ret_val = cursor.callfunc(full_name,
                                  cx_ora_type,
                                  keywordParameters=_encode(params))

        if return_type == "VARCHAR2":
            results.write_success_result(result_id = input_id, return_val = ret_val)
        elif return_type == "REF CURSOR":
            results.write_success_result(result_id = input_id, ret_cursor = ret_val)

    else:
        #call procedure
        cursor.callproc(full_name,
                        keywordParameters=_encode(params))
        results.write_success_result(result_id = input_id)


    cursor.close()

def _encode(obj):
    if isinstance(obj, dict):
        return dict((_encode(k), _encode(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return [_encode(x) for x in obj]
    elif isinstance(obj, unicode):
        return obj.encode("utf-8")
    else:
        return obj



def handle_command(input_id, input_obj):
    try:
        command_str = input_obj['command']
    except KeyError:
        results.write_error_result(result_id = input_id,
                                   code = results.NO_COMMAND_FOUND)
        return

    if command_str == "connect":
        try:
            login_str = input_obj['login']
        except KeyError:
            results.write_error_result(result_id = input_id,
                                       code = results.NO_LOGIN_FOUND)
            return
        
        _connect(input_id, login_str)
        return

    if command_str == "disconnect":
        _disconnect(input_id)
        return

    if command_str == "quit":
        sys.exit(0)
        return

    if command_str == "call":
        try:
            package_name = input_obj['package']
        except KeyError:
            results.write_error_result(result_id = input_id,
                                       code = results.NO_PACKAGE_NAME_FOUND)
            return

        try:
            proc_name = input_obj['procedure']
        except KeyError:
            results.write_error_result(result_id = input_id,
                                       code = results.NO_PROC_NAME_FOUND)
            return

        proc_params = {}
        try:
            proc_params = input_obj['params']
        except KeyError:
            pass
        _execute_proc(input_id, package_name, proc_name, proc_params)
        return

    results.write_error_result(result_id = input_id,
                               code = results.INVALID_COMMAND,
                               message_params = command_str)
