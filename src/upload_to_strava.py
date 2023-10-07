import os
import sys
import glob
import re
from datetime import datetime
import pytz
from shutil import copy
import time
import strava.swagger_client
from strava.swagger_client.rest import ApiException
from pprint import pprint

path = sys.argv[1]


class TcxFile(object):

    def __init__(self, filename):
        self.filename = filename
        self.datetime = self.parse_filename(filename)
        self.size = os.path.getsize(filename)

    @staticmethod
    def parse_filename(filename):
        m = re.match(f"{path}/(.*)_[\d]+.tcx", filename)
        return datetime.fromisoformat(m.group(1))


def sort_key(file):
    return file.filename

#
# class StravaClient(object):
#
#     def __init__(self, access_token):
#
#         config = strava.swagger_client.Configuration()
#         config.api_key['access_token'] = 'xxx'
#         api_client = strava.swagger_client.ApiClient(config)
#
#         # Configure OAuth2 access token for authorization: strava_oauth
#         # strava.swagger_client.configuration.access_token = access_token
#
#         # create an instance of the API class
#         self.api_instance = strava.swagger_client.UploadsApi(api_client)
#
#     def upload(self, file):
#         file = file.filename  # File | The uploaded file. (optional)
#         name = ''  # String | The desired name of the resulting activity. (optional)
#         description = ''  # String | The desired description of the resulting activity. (optional)
#         trainer = ''  # String | Whether the resulting activity should be marked as having been performed on a trainer. (optional)
#         commute = ''  # String | Whether the resulting activity should be tagged as a commute. (optional)
#         dataType = 'tcx'  # String | The format of the uploaded file. (optional)
#         externalId = ''  # String | The desired external identifier of the resulting activity. (optional)
#
#         try:
#             # Upload Activity
#             api_response = self.api_instance.create_upload(
#                 file=file,
#                 name=name,
#                 description=description,
#                 trainer=trainer,
#                 commute=commute,
#                 data_type=dataType,
#                 external_id=externalId)
#             pprint(api_response)
#         except ApiException as e:
#             print("Exception when calling UploadsApi->createUpload: %s\n" % e)
#
#
# tcx_files = [TcxFile(f) for f in glob.glob(f"{path}/*.tcx")]
# filtered = [file for file in tcx_files if file.datetime < datetime(2019, 7, 9, 0, 0, 0, 0, pytz.UTC) and file.datetime > datetime(2011, 12, 18, 0, 0, 0, 0, pytz.UTC)]
# filtered.sort(key=sort_key)
#
# strava_client = StravaClient('')
#
# for f in filtered[:1]:
#     print(f.filename)
#     strava_client.upload(f)

# size = 0
# bulk = []
# bulk_index = 0
#
# for f in filtered:
#     if size >= 25:
#         dst_dir = os.path.join(path, str(bulk_index))
#         os.mkdir(dst_dir)
#         for g in bulk:
#             copy(g.filename, dst_dir)
#
#         bulk = []
#         size = 0
#         bulk_index += 1
#
#     size += 1
#     bulk.append(f)
