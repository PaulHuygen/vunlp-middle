import parameters
import StringIO
import peewee

DEFAULT_URL          = parameters.DEFAULT_URL
REQUEST_ID           = "{url}/batch"
REQUEST_BATCHITEM  = "{url}/batch/{batchid}/{item}"
REQUEST_BATCHSTATUS  = "{url}/batch/{batchid}/status"
#REQUEST_STARTBATCH   = "{url}/batch/{batchid}/start"
REQUEST_UPLOAD       = "{url}/batch/{batchid}/text"
REQUEST_TEXTITEM     = "{url}/batch/{batchid}/text/{textid}/{item}"
REQUEST_FILESTATUS   = "{url}/batch/{batchid}/text/{textid}/status"
REQUEST_BATCHPARSES =  "{url}/batch/{batchid}/parses"
REQUEST_BATCHRESULT =  "{url}/batch/{batchid}/result"


ROUTE_REQUEST_ID = REQUEST_ID.format(url = '')
ROUTE_REQUEST_BATCHSTATUS  = REQUEST_BATCHITEM.format(url = '', batchid = '<batchid>', item = 'status')
ROUTE_REQUEST_STARTBATCH   = REQUEST_BATCHITEM.format(url = '', batchid = '<batchid>', item = 'start')
ROUTE_REQUEST_UPLOAD       = REQUEST_BATCHITEM.format(url = '', batchid = '<batchid>', item = 'text')
ROUTE_REQUEST_BATCHPARSES  = REQUEST_BATCHITEM.format(url = '', batchid = '<batchid>', item = 'parses')
ROUTE_REQUEST_BATCHRESULT  = REQUEST_BATCHITEM.format(url = '', batchid = '<batchid>', item = 'result')
ROUTE_REQUEST_BATCHLOGS    = REQUEST_BATCHITEM.format(url = '', batchid = '<batchid>', item = 'logs')
ROUTE_REQUEST_TEXTSTATUS   = REQUEST_TEXTITEM.format(url = '', batchid = '<batchid>', textid = '<textid>', item = 'status')
ROUTE_REQUEST_TEXT         = REQUEST_TEXTITEM.format(url = '', batchid = '<batchid>', textid = '<textid>', item = 'text')
ROUTE_REQUEST_PARSE        = REQUEST_TEXTITEM.format(url = '', batchid = '<batchid>', textid = '<textid>', item = 'parse')
ROUTE_REQUEST_LOG          = REQUEST_TEXTITEM.format(url = '', batchid = '<batchid>', textid = '<textid>', item = 'log')

UNKNOWNPAR       = 'unknown'
RUNNINGPAR       = 'running'
INITPAR          = 'init'
SENTPAR          = 'sent'
PARSEREADYPAR    = 'parseready'
READYPAR         = 'ready'
LOGREADYPAR      = 'logready'
LOGPARSEREADYPAR = 'logparseready'
TIMEOUTPAR       = 'timeout'


JSONCONTENTTYPE = 'application/json'
JSONHEADER = {'content-type': JSONCONTENTTYPE}

def pack_recipe(recipe):
    """ Pack recipe in a dict to be transported as json

    @param recipe:
    @return: dict with recipe.
    """
    return dict(recipe = recipe)

def unpack_recipe(d):
    """ Retrieve recipe from dict from json

    @param : dict that contains the recipe
    @return: recipe
    """
    try:
       return d['recipe']
    except Exception as e:
       return None

def pack_batchid(batchid):
    """ Pack batchid in a dict to be transported as json

    @param batchid:
    @return: dict with batchid.
    """
    return dict(batchid = batchid)


def unpack_batchid(d):
    """ Retrieve batchid from dict from json

    @param batchid:
    @return: dict with batchid.
    """
    try:
       return d['batchid']
    except Exception as e:
       return None

def pack_status(status):
    """ Pack status in a dict to be transported as json

    @param status:
    @return: dict with status.
    """
    return dict(status = status)


def unpack_status(d):
    """ Retrieve status from dict from json

    @param status:
    @return: dict with status.
    """
    try:
       return d['status']
    except Exception as e:
       return None


def append_text_json(id, content):
  enc = 'plain'
  try:
    s = json.dumps(content)
    s = content
  except Exception as e:
    s = content.encode('base64')
    enc = 'base64'
  return dict( id = id, code = enc,  content = s) 

def make_jsonnable(content):
    """
    Convert content to base64 if necessary.

    @param: content
    @return: [ encoding , jsonnable_content ]
    """
    enc = 'plain'
    try:
       s = json.dumps(content)
       s = content
    except Exception as e:
       s = content.encode('base64')
       enc = 'base64'
    return [ enc, s ]


def pack_content_single(contentid, rawcontent):
    """ Pack a singlestring containing a text, a package of texts or some other material in a dict that can be jsonned.

    @param contentid: Handle of the raw content
    @param rawcontent: string to be packed
    @return: list with a single dict, ready to be jsonned.
    """
    payload = []
    enc = 'plain'
    try:
      s = json.dumps(rawcontent)
      s = rawcontent
    except Exception as e:
      s = rawcontent.encode('base64')
      enc = 'base64'
    payload.append(dict( id = contentid, code = enc,  content = s) )
    return payload 

def unpack_content(l):
  for it in l:
      id = it['id']
      coding = it['code']
      if coding == 'base64':
        content = it['content'].decode('base64')
      else:
        content = it['content']
      contentf = StringIO.StringIO(content)
      upload = UploadFile(id, contentf)
      for name, f in upload.files():
          yield name, f

def unpack_single_content(l):
    for name, f in unpack_content(l):
       pass
    return name, f

def add_doc_item(textid, item, content, storelist):
    """
    Add a text, log, parse to a list that can be jsonned

    @param textid:
    @param item: "in", "log" or "parse"
    @param content:
    @param storelist: List to which the item has to be appended
    """
    enc, s = make_jsonnable(content)
    storelist.append(dict( textid = textid, item = item, enc = enc, content = s))
    return storelist

def get_doc_items(l):
    """
    Get text, log, parse items from a list that
    has been made with add_doc_item

    @param l: list with items as dicts
    @return id, itemtype, content (string)
    """
    for item in l:
        if item['enc'] == 'base64':
            content = item['content'].decode('base64')
        else:
           content = item['content']
        yield item['textid'], item['item'], content

        

import mimetypes
import tarfile
import zipfile

class UploadFile:
   """ Make the contents of an upload available.
       Recognizes tar, tgz and zip. Otherwize handled as plain text.

       Extracts zipfiles, tarfiles and tar-gz-files transparently.
   """
   PLAINTYPE = 'plain'
   TGZTTYPE = 'tgz'
   TARTYPE = 'tar'
   ZIPTYPE = 'zip'

   upload_filename = None
   contentobj = None      # Tarfile-object, Zipfile-object or iterable with filename and finelcontent
   uploadtype = None

   def __init__(self, filename, f):
      """ Provide the uploadobject.

      @param filename: filename
      @param f: file-object with content
      """
      # Find out the filetype of the upload
      # In case of tar/tgz/zip, open an appropiate object in self.contentobj
      # Otherwise put an iterable with content and filename in self.contentobj
      self.upload_filename = filename
      (uptype, upencoding) = mimetypes.guess_type(self.upload_filename)
      if (uptype == 'application/x-tar') & (upencoding == 'gzip'):
         self.uploadtype =  self.TGZTTYPE
         self.contentobj = tarfile.open(fileobj = f, mode = 'r:gz')
      elif (uptype == 'application/x-tar') & (upencoding == None):
         self.uploadtype = self.TARTYPE
         self.contentobj = tarfile.open(fileobj = f, mode = 'r')
      elif (uptype == 'application/zip'):
         self.uploadtype = self.ZIPTYPE
         self.contentobj = zipfile.ZipFile(f, 'r')
      else:
         self.uploadtype = self.PLAINTYPE
         self.contentobj = [(self.upload_filename, f)]

   def _get_next_tarmem(self):
      for mem in self.contentobj.getmembers():
         yield (mem.name, self.contentobj.extractfile(mem))

   def _get_next_zipmem(self):
      for fn in self.contentobj.namelist():
         yield (fn, self.contentobj.open(fn))

   def _get_next_plainfile(self):
      for elem in self.contentobj:
         yield elem

   def files(self):
      """ generator of the uploaded files.

      @return: tuples of (filename, fileobj)
      """
      if ( self.uploadtype == self.TGZTTYPE ) | ( self.uploadtype == self.TARTYPE ) :
         for res in self._get_next_tarmem():
           yield res
      elif self.uploadtype == self.ZIPTYPE:
         for res in self._get_next_zipmem():
           yield res
      else:
         for res in self._get_next_plainfile():
           yield res

   def filetype():
      return self.uploadtype


