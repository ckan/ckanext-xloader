
import requests
import json
import os
import urllib2
import datetime
import sys
import time


from requests_toolbelt import MultipartEncoder


def verify_response(r, method, kind, key, verbose):
  if r.ok:
    if verbose:
      print 'Success | %s | %s | %s' % (method, kind, key)
  else:
    print 'Failed  | %s | %s | %s' % (method, kind, key)
    print r.text
    exit()

  return r.json()

def num_to_excel_col(n):
    if n < 1:
        raise ValueError("Number must be positive")
    result = ""
    while True:
        if n > 26:
            n, r = divmod(n - 1, 26)
            result = chr(r + ord('A')) + result
        else:
            return chr(n + ord('A') - 1) + result

class OpenGov():
  """
  Core OpenGov functions
  """

  def __init__(self, host, api_key, entity_id):
    self.host = host or 'http://controlpanel.ogbeta.us'
    self.api_key = api_key
    self.entity_id = entity_id

  # GET, POST, PUT, DELETE
  # https://controlpanel.ogbeta.us/api/grid_data/v0/transaction_datasets
  def transaction_datasets(self, method, id = None, data = None, verbose=False):
    if method == "GET":
      url = '%s/api/grid_data/v0/transaction_datasets' % (self.host)
      url += '?entity_id=%s' % self.entity_id
      key = "GET"
    elif method == "POST":
      url = '%s/api/grid_data/v0/transaction_datasets' % (self.host)
      url += '?entity_id=%s&linked_to_default_coa=undefined' % self.entity_id
      key = "id: %s, data: %s" % (id, data)
    elif method == "PUT":
      url = '%s/api/grid_data/v3/transaction_datasets' % (self.host)
      url += '/%s?entity_id=%s&embed=status' % (id, self.entity_id)
      key = "id: %s, data: %s" % (id, data)
    elif method == "DELETE":
      url = '%s/api/grid_data/v0/transaction_datasets' % (self.host)
      url += '/%s?entity_id=%s' % (id, self.entity_id)
      key = id
    else:
      print "Unsupported request type: %s" % (method)
      exit()

    r = requests.request(
      method,
      url,
      data=json.dumps(data),
      headers={
        'content-type': 'application/json',
        'authorization': 'Token token=%s' % self.api_key
      }
    )

    return verify_response(r, method, "Dataset", key, verbose)

  # POST, PUT
  # https://controlpanel.ogbeta.us/api/grid_data/v3/upload_batches
  def upload_batches(self, method, id = None, data = None, verbose = True):
    url = '%s/api/grid_data/v3/upload_batches' % (self.host)
    key = id

    if method == "POST":
      url += '?entity_id=%s&dataset_id=%s' % (self.entity_id, id)
      key = "id: %s, data: %s" % (id, data)
    elif method == "PUT":
      url += '/%s?entity_id=%s&embed=uploads' % (id, self.entity_id)
      key = "id: %s, data: %s" % (id, data)
    else:
      print "Unsupported request type: %s" % (method)
      exit()

    r = requests.request(
      method,
      url,
      data=json.dumps(data),
      headers={
        'content-type': 'application/json',
        'authorization': 'Token token=%s' % self.api_key
      }
    )

    return verify_response(r, method, "Upload Batches", key, verbose)

  # GET, POST, PUT
  # https://controlpanel.ogbeta.us/api/grid_data/v3/upload_batches
  def uploads(self, method, id = None, data = None, verbose = False):
    url = '%s/api/grid_data/v3/uploads' % (self.host)
    key = id

    if method == "GET":
      url += '/%s?entity_id=%s' % (id, self.entity_id)
      key = id
    elif method == "POST":
      url += '?entity_id=%s&upload_batch_id=%s' % (self.entity_id, id)
      key = "id: %s, data: %s" % (id, data)
    elif method == "PUT":
      url += '/%s?entity_id=%s' % (id, self.entity_id)
      key = "id: %s, data: %s" % (id, data)
    else:
      print "Unsupported request type: %s" % (method)
      exit()

    r = requests.request(
      method,
      url,
      data=json.dumps(data),
      headers={
        'content-type': 'application/json',
        'authorization': 'Token token=%s' % self.api_key
      }
    )

    return verify_response(r, method, "Uploads", key, verbose)

  # GET, PUT
  # https://controlpanel.ogbeta.us/api/grid_data/v3/data_sheets
  def data_sheets(self, method, id = None, data = None, verbose = False):
    url = '%s/api/grid_data/v3/data_sheets' % (self.host)
    key = id

    if method == "GET":
      url += '?upload_id=%s' % id
      key = id
    elif method == "PUT":
      url += '/%s?entity_id=%s&embed=status' % (id, self.entity_id)
      key = id
    else:
      print "Unsupported request type: %s" % (method)
      exit()

    r = requests.request(
      method,
      url,
      data=json.dumps(data),
      headers={
        'content-type': 'application/json',
        'authorization': 'Token token=%s' % self.api_key
      }
    )

    return verify_response(r, method, "Data Sheets", key, verbose)

  # PUT
  def s3(self, method, url = None, file_path = None):
    if method != "PUT":
      print "Unsupported request type: %s" % (method)
      exit()

    r = requests.request(
      method,
      url,
      data=open(file_path, 'rb'),
      headers={
        "Origin": self.host,
        "Access-Control-Request-Method": method
      }
    )

    return r



def import_resource_to_opengov(config, file_path, resource_dict, logger=None):
  verbose = True


  OpenGovCore = OpenGov(config.get('backend'), config.get('api_key'), config.get('entity_id'))



  # Get/Create transaction dataset
  # transaction_dataset = next((d for d in self.transaction_datasets if d.get('name') == dist.get('title')), False)

  # if transaction_dataset:
  #   print 'Dataset "%s" exists updating' % transaction_dataset.get('name')
  # else:
  #  transaction_dataset = self.opengov.transaction_datasets("POST", None, { "name": dist.get("title") }, verbose=verbose)

  transaction_dataset = OpenGovCore.transaction_datasets("POST", None, { "name": resource_dict.get("name") }, verbose=verbose)

  # Upload Data
  upload_batch = OpenGovCore.upload_batches("POST", transaction_dataset.get('id'), { "name": resource_dict.get("name") }, verbose=verbose)

  upload_file_path, upload_file_name = os.path.split(resource_dict.get('url'))
  upload = OpenGovCore.uploads("POST", "%s&dataset_id=%s" % (upload_batch.get('id'), transaction_dataset.get('id')), { "upload_file_name": upload_file_name }, verbose=verbose)

  OpenGovCore.s3("PUT", upload.get('file_upload_url'), file_path)

  # Process Data
  upload = OpenGovCore.uploads("PUT", upload.get('id'), { "upload_complete": True }, verbose=verbose)

  if upload.get('status') != 'unparsed':
    logger.info('Upload failed id: "%s"' % upload.get('id'))
    exit()

  counter = 0
  timeout = 90
  logger.info("Waiting to finish processing for upload, id: %s..." % upload.get('id'))
  while counter < timeout:
    if counter == timeout - 1:
      logger.info('Time out after %ss: "%s"' % (timeout, upload.get('id')))
      exit()

    upload = OpenGovCore.uploads("GET", upload.get('id'), verbose=verbose)

    if upload.get('status') == 'parsed':
      break

    counter += 1
    time.sleep(1)

  # Set up schema and dataset
  data_sheets = OpenGovCore.data_sheets("GET", upload.get('id'), verbose=verbose)
  data_sheet = data_sheets.get('data_sheets')[0]

  headers = data_sheet.get('headers')
  column_guesses = data_sheet.get('column_guesses')

  body = {
    "column_mapping": {},
    "data_sheet_id": data_sheet.get('id')
  }

  for i, header in enumerate(headers):
    col = num_to_excel_col(i + 1)

    body["column_mapping"][col] = {
      "type": column_guesses[i],
      # "name": headers[i],
      "dependent_column_type": None
    }

    if header == resource_dict.get('amount_column'):
       body["column_mapping"][col]["type"] = "currency"
       body["column_mapping"][col]["special_types"] = ["transaction_amount"]

    if header == resource_dict.get("date_column"):
       body["column_mapping"][col]["special_types"] = ["financial_year"]

  logger.info(body)

  OpenGovCore.transaction_datasets("PUT", transaction_dataset.get('id'), body, verbose=verbose)

  OpenGovCore.upload_batches("PUT", upload_batch.get('id'), { "run_append_job": True }, verbose=verbose)

  logger.info("Finished Importing %s\n" % resource_dict.get('name'))
