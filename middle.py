#!/usr/bin/python
###########################################################################
#          (C) Vrije Universiteit, Amsterdam (the Netherlands)            #
#                                                                         #
# This file is part of vunlp, the VU University NLP e-lab                 #
#                                                                         #
# vunlp is free software: you can redistribute it and/or modify it under  #
# the terms of the GNU Affero General Public License as published by the  #
# Free Software Foundation, either version 3 of the License, or (at your  #
# option) any later version.                                              #
#                                                                         #
# vunlp is distributed in the hope that it will be useful, but WITHOUT    #
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or   #
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public     #
# License for more details.                                               #
#                                                                         #
# You should have received a copy of the GNU Affero General Public        #
# License along with vunlp.  If not, see <http://www.gnu.org/licenses/>.  #
###########################################################################

"""
Web-service to parse a set of documents using Lisa supercomputer.
Usage from command line:
python middle.py
"""

from __future__ import unicode_literals, print_function, absolute_import
from bottle import Bottle, route, run, template, request, response, abort, debug
from bottle import install, uninstall
import bottle
import tempfile
import os
import shutil
import sys
import tarfile
import logging
import urllib
import mimetypes
import StringIO
import vunlp
#from jsonapiplugin import JSONAPIPlugin as jsonplug
import json
import parameters

import db 
bottle.BaseRequest.MEMFILE_MAX = 4096 * 4096 
app = Bottle()
debug(True)



#
# Functions
#

def get_upload_or_quit(request):
   """Get uploaded file from a form and abort when it is not provided or not known"""
   if not request.files.get('upload'):
     abort(400, 'No upload found')
   return request.files.get('upload')




@app.route('/hello')
def hello():
    """Look whether it works """
    logging.debug("Where would this sentence appear?")
    return "Hello World and all that!"


#
# Get list of parsers
# Todo: complete structure for parsers, make this usable.
#
@app.route('/parsers')
def parserlist():
    """get list of supported parsers"""
    return "<html><head></head><body><p>Alpino</p> <p>Stanford</p></body></html>"

#
# Batches
#
# Get batch ID
#
#@app.post('/batch')
@app.post(vunlp.ROUTE_REQUEST_ID)
def batchid():
    """Register a new batch

    - Get the recipe as payload,
    - create a batch record in the db
    - return the id of the batch to the caller.
    Return statuscode 417 if the recipe cannot be deciphered.
    """
    if request.content_type == vunlp.JSONCONTENTTYPE:
       recipe = vunlp.unpack_recipe(request.json)
    else:
       recipe = request.forms.get('recipe')
    if not recipe:
      logging.debug("No recipe found")
      abort(417, 'no recipe')
    id = str(db.get_new_batchid(recipe))
    logging.debug("Returning batch-id: " + str(id))
    if request.content_type == vunlp.JSONCONTENTTYPE:
       response.set_header(str('content-type'), vunlp.JSONCONTENTTYPE)
       return json.dumps(vunlp.pack_batchid(str(id)))
    else:
      return str(id)

def batchname(id, rawname):
  return str(id) + rawname

#
# Check whether a batch exists
#
#@app.route('/batch/<batchid>/status',  apply = [JSONAPIPlugin()])
#@app.route('/batch/<batchid>/status')
@app.route(vunlp.ROUTE_REQUEST_BATCHSTATUS)
def batch_status(batchid):
#def check_batchid(batchid):
  """Test the state of a batch

     return 404 when the batch does not exist
     otherwise, return the state of the batch.
  """
  if not db.known_batchid(batchid):
    abort(404, 'Batch-id not known')
  return json.dumps(vunlp.pack_status(db.get_batchphase(batchid)))

# Start a batch:
# 1) Change the phase of the batch
# 2) Send the texts to Lisa.
# 3) Send start-signal to Lisa.
#@app.put('/batch/<batchid>/start')
@app.put(vunlp.ROUTE_REQUEST_STARTBATCH)
def startbatch(batchid):
   """Start processing a batch"""
   db.set_batchphase(batchid, vunlp.RUNNINGPAR)
#   start_cron()

#
# Texts
#

def unpack_json_upload(block):
  """ 
  Unpack the materials from a text-upload in JSON encoding.
  The block consists of a list of dicts. Each dict contains the following
    'id': filename or text-id
    'code': Character encoding of the content string (currently: 'base64' or 'plain' (i.e., nothing
            special)
    'content': String with the content (text, tgz, tar or zip).
            
  @param block: list of dicts
  @return: iteration of tuples (textid, text) 
  """
  for it in block:
     textid = it['id']
     coding = it['code']
     if coding == 'base64':
        content = it['content'].decode('base64')
     else:
        content = it['content']
     contentf = StringIO.StringIO(content)
     upload = vunlp.UploadFile(textid, contentf)
     for name, f in upload.files():
       yield name, f
#     db.insert_text(batchid, name, str(f.read()))

def unpack_file_upload(block):
   """ 
   Unpack the materials from a file-upload.
   The file may contain a single text or it may  be a tar, tgz or zip file
   (to be recognized in the filename).
             
   @param block: upload (cgi.FieldStorage object)
   @return: iteration of tuples (textid, text) 
   """
   blockname = block.filename
   blockfil = block.file
   upload = vunlp.UploadFile(blockname, blockfil)
   for name, f in upload.files():
      yield name, f


#@app.post('/batch/<batchid>/text')
@app.post(vunlp.ROUTE_REQUEST_UPLOAD)
def get_file(batchid):
   """Receive a text or collection of texts as
        1. file upload
        2. upload of tar, tgz or zip
        3. json dict of (textid : text)

   """
   logging.debug("Receive a file.")
#   batchid = get_valid_batch_id_from_user_or_quit()
#   logging.debug("Got batch-id:" + str(batchid))
   if not db.known_batchid(batchid):
      abort(404, 'Batch-id not known')
#   upload = get_upload_or_quit(request)
#   try:
   print("Start trying")
   if request.content_type == vunlp.JSONCONTENTTYPE:
        print("JSON-type")
        print("JSON content: {}".format(request.json))
        for (name, f) in unpack_json_upload(request.json):
          print("Get file {}".format(name))
          db.insert_text(batchid, name, str(f.read()))
   else:
       print("un-JSON-type")
       for (name, f) in unpack_file_upload(get_upload_or_quit(request)):
          db.insert_text(batchid, name, str(f.read()))
#   except Exception as e:
#     print("Error getting file upload:{} ".format(sys.exc_info()[0]))
#     logging.error("Error getting file upload: {}".format(sys.exc_info()[0]))
#     logging.error("Error getting file upload: " + str(e.args))
#     abort(500, 'Error getting file upload')

#@app.post('/batch/<batchid>/text')
#def get_text(batchid):
#   """Receive a tuple (textid, text) as json
#      
#   """
#   logging.debug("Receive a text.")
##   batchid = get_valid_batch_id_from_user_or_quit()
##   logging.debug("Got batch-id:" + str(batchid))
#   if not db.known_batchid(batchid):
#     abort(404, 'Batch-id not known')
#     
##   upload = get_upload_or_quit(request)
#   try:
#     upload = uploadfile.UploadFile(get_upload_or_quit(request))
#     for name, f in upload.files():
#       db.insert_text(batchid, name, str(f.read()).encode('utf-8', 'replace'))
#   except Exception as e:
#     logging.error("Error getting file upload: " + str(e.args))
#     abort(500, 'Error getting file upload')

#@app.route('/batch/<batchid>/text/<textid>/status',  apply = [JSONAPIPlugin()])
#@app.route('/batch/<batchid>/text/<textid>/status')
@app.route(vunlp.ROUTE_REQUEST_TEXTSTATUS)
def getfilestatus(batchid, textid):
   """
    Return string with status information about a text in a batch
    Side effect: remove text if it has time-out status.
   """
   try:
     logging.debug("About to obtain phase of: " + batchid + "; " + textid)
     filephase = db.get_filephase(batchid, textid)
     logging.debug("Found: " + filephase)
   except Exception as e:
      logging.error("Exception args:" + str(e.args))
      logging.error("Not found")
      abort(404, 'File not found')
   if filephase in [ vunlp.UNKNOWNPAR, vunlp.TIMEOUTPAR, vunlp.TIMEOUTPAR ]:
      status = filephase 
   elif filephase in [ vunlp.PARSEREADYPAR, vunlp.LOGPARSEREADYPAR ]:
      status =  "ready"
   else:
      status = "waiting"
   if status == vunlp.TIMEOUTPAR:
      db.remove_text(batchid, textid)
   if request.content_type == vunlp.JSONCONTENTTYPE:
      response.set_header(str('content-type'), vunlp.JSONCONTENTTYPE)
      return json.dumps(vunlp.pack_status(status))
   else:
      return status

def returncontent(batchid, textid, contenttype):
   """Return ready parses to user"""
   content = db.get_content(batchid, textid, contenttype)
   if content == None:
      abort(404, 'Content not present')
   if request.content_type == vunlp.JSONCONTENTTYPE:
      response.set_header(str('content-type'), vunlp.JSONCONTENTTYPE)
      res = json.dumps(vunlp.pack_content_single('textid', content))
   else:
       res = content
   if contenttype == 'parse':
      db.remove_text(batchid, textid)
   return res



#@app.route('/batch/<batchid>/text/<textid>/text',  apply = [JSONAPIPlugin()])
@app.route(vunlp.ROUTE_REQUEST_TEXT)
def get_infile_back(batchid, textid):
   return returncontent(batchid, textid, 'text')

@app.route(vunlp.ROUTE_REQUEST_LOG, method = 'HEAD')
def getlogstatus(batchid, textid):
   """Return status 200 when logfile is available and has non-zero length"""
   logging.debug("Look whether " + textid + " has logcontent.")
   if not db.logfile_has_content(batchid, textid):
     abort(404, 'logfile not available or empty')

#@app.route('/batch/<batchid>/text/<textid>/log')
@app.route(vunlp.ROUTE_REQUEST_LOG)
def returnlog(batchid, textid):
   """Return parser logfile to user"""
   return returncontent(batchid, textid, 'log')


#   phase = db.get_filephase(batchid, textid)
#   if phase == db.LOGREADYPAR or phase == db.LOGPARSEREADYPAR:
#     content = db.get_log(batchid, textid)
#     if request.content_type == vunlp.JSONCONTENTTYPE:
#       response.set_header(str('content-type'), vunlp.JSONCONTENTTYPE)
#       return json.dumps(vunlp.pack_content_single('textid', db.get_log(batchid, textid)))
#     else:
#       return db.get_log(batchid, textid)
#   else:
#     abort(404, 'Logfile not available or file unknown')


#@app.route('/batch/<batchid>/text/<textid>/parse')
@app.route(vunlp.ROUTE_REQUEST_PARSE)
def returnparse(batchid, textid):
   """Return ready parses to user"""
   return returncontent(batchid, textid, 'parse')

#   phase = db.get_filephase(batchid, textid)
#   if phase == db.PARSEREADYPAR or phase == db.LOGPARSEREADYPAR:
#     content = db.get_parse(batchid, textid)
#     if request.content_type == vunlp.JSONCONTENTTYPE:
#       response.set_header(str('content-type'), vunlp.JSONCONTENTTYPE)
#       res = json.dumps(vunlp.pack_content_single('textid', db.get_parse(batchid, textid)))
#     else:
#       res = content
#     db.remove_text(batchid, textid)
#     return res
#   else:
#     abort(404, 'File not parsed or file unknown')

   


#@app.route('/batch/<batchid>/parses')
@app.route(vunlp.ROUTE_REQUEST_BATCHPARSES)
def returnparses(batchid):
   """Return a tgz with ready parses to user"""
   if db.count_ready_parses(batchid) == 0:
       abort(404, 'no parses available')
   f = tempfile.TemporaryFile()
   tarf = tarfile.open('', 'w:gz', f)
   for (textid, content) in db.get_parses(batchid):
     g = tempfile.TemporaryFile()
     g.write(content)
     g.seek(0)
     ti = tarf.gettarinfo('', textid, g)
     tarf.addfile(ti, g)
   tarf.close()
   f.seek(0)
   return f.read()

#@app.route('/batch/<batchid>/result')
@app.route(vunlp.ROUTE_REQUEST_BATCHRESULT)
def returnresults(batchid):
    """Return the ready parses and logfiles"""
    readylogs = db.count_ready_items(batchid, 'log')
    logging.debug("Nr. of logfiles: {}".format(readylogs))
    readyparses = db.count_ready_items(batchid, 'parse')
    logging.debug("Nr. of parses: {}".format(readylogs))
    if (readylogs == 0) & (readyparses == 0):
       abort(404, 'no parses or logs available')
    outlist = []
    if readylogs > 0:
       logging.debug("Get logfiles")
       for textid, content in db.get_ready_items(batchid, 'log'):
          logging.debug("Get logfile {}".format(textid))
          outlist = vunlp.add_doc_item(textid, 'log', content, outlist)
    if readyparses > 0:
       logging.debug("Get textfiles")
       for textid, content in db.get_ready_items(batchid, 'parse'):
          logging.debug("Get textfile {}".format(textid))
          outlist = vunlp.add_doc_item(textid, 'parse', content, outlist)
    if request.content_type == vunlp.JSONCONTENTTYPE:
      response.set_header(str('content-type'), vunlp.JSONCONTENTTYPE)
      res = json.dumps(outlist)
    else:
       res = outlist
    return res
       
    

##@app.route('/batch/<batchid>/parses')
#@app.route(vunlp.ROUTE_REQUEST_BATCHPARSES)
#def returnparses(batchid):
#   """Return a tgz with ready parses to user"""
#   if db.count_ready_parses(batchid) > 0:
#     f = tempfile.TemporaryFile()
#     tarf = tarfile.open('', 'w:gz', f)
#     for (textid, content) in db.get_parses(batchid):
#       g = tempfile.TemporaryFile()
#       g.write(content)
#       g.seek(0)
#       ti = tarf.gettarinfo('', textid, g)
#       tarf.addfile(ti, g)
#     tarf.close()
#     f.seek(0)
#     return f.read()


if __name__ == "__main__":
   logging.basicConfig(level = logging.DEBUG)
   run(app, host='localhost', port=8090, debug=True)
