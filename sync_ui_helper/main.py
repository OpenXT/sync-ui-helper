#!/usr/bin/python
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
import json

import results
import commands

import sync_db


def run():
    while True:
        input_line = sys.stdin.readline()

        if not input_line:
            sys.exit(0)

        try:
            input_obj = json.loads(input_line)
        except ValueError:
            results.write_error_result(code = results.INVALID_JSON)
            continue

        #get the id of the input line
        try:
            input_id = input_obj['id']
        except KeyError:
            results.write_error_result(code = results.NO_ID_FOUND)
            continue
        except TypeError:
            results.write_error_result(code = results.NO_DICT_FOUND)
            continue


        try:
            with sync_db.error.convert():
                commands.handle_command(input_id, input_obj)
        except sync_db.error.SyncError as error:
            results.write_error_result(result_id = input_id,
                                       code = results.SYNC_ERROR,
                                       oracle_code = error.code,
                                       oracle_message = "{0}".format(error))
        except sync_db.error.Error as error:
            results.write_error_result(result_id = input_id,
                                       code = results.DATABASE_ERROR,
                                       oracle_code = error.code,
                                       oracle_message = "{0}".format(error))


