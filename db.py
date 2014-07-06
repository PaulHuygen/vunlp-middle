#!/usr/bin/python
from __future__ import unicode_literals, print_function, absolute_import
#import vunlpparameters as parms
import vunlp
import sqlite3
import zlib
import sys
import textstoremodel as text
import logging
import peewee

#UNKNOWNPAR =  'unknown'
#RUNNINGPAR = 'running'
#INITPAR = 'init'
#SENTPAR = 'sent'
#PARSEREADYPAR = 'parseready'
#LOGREADYPAR = 'logready'
#LOGPARSEREADYPAR = 'logparseready'
#TIMEOUTPAR = 'timeout'

#
# Convenience functions
#
def _get_text_record(batchid, textid):
  """ get the textrecord with the given id' or raise exception

      TODO: Make it handle the correct exception.
  """ 
  try:
    return text.Text.select().join(text.Batch).where((text.Batch.batchid == batchid) & (text.Text.name == textid)).get()
  except Exception as e:
    logging.debug("Cannot retrieve record " + batchid + "; " + textid)
    logging.debug("Exception message: " + str(e.args))
    raise
#  return text.Text.get(text.Text.batch.batchid == batchid & text.Text.name = textid)

def _get_or_create_text_record(batchid, textid):
  """ get the textrecord with the given id's """ 
  try:
    record = _get_text_record(batchid, textid)
  except Exception:
    logging.debug("Exception: " + str(sys.exc_info()[0]))
    batchrec = text.Batch.get(text.Batch.batchid == batchid)
    record = text.Text.create(batch = batchrec, name = textid)
    record.save()
  return record


#
# Functions for batches
#
def get_new_batchid(recipe):
  """ 
    Register a new batch

    @param recipe: Parse recipe
  """ 
  newbatch = text.Batch.create(phase = vunlp.INITPAR, recipe = recipe)
  newbatch.save()
  logging.debug('Batch nr. ' + str(newbatch.batchid) +  ' created.')
  return str(newbatch.batchid)

def known_batchid(batchid):
  """ Return whether a batch with the given id exists.""" 
  try:
    record = text.Batch.get(text.Batch.batchid == str(batchid))
    return True
  except Exception as e:
    return False


def get_recipe(batchid):
  """ Return the recipe of the batch with the given id or raise exception.""" 
  return text.Batch.get(text.Batch.batchid == batchid).recipe

def set_batchphase(batchid, phase):
  """Signal that a batch has come into another phase"""
  record = text.Batch.get(text.Batch.batchid == batchid)
  record.phase = phase
  record.save()

def get_batchphase(batchid):
  """ Return the phase of the batch with the given id or raise exception.""" 
  return text.Batch.get(text.Batch.batchid == batchid).phase

def files_in_batch(batchid):
  """count the number of texts in the batch with given id"""
  return text.Text.select().join(text.Batch).where(text.Batch.batchid == batchid).count()

def remove_empty_batch(batchid):
  """
   Remove the batch when it is running but contains no texts
  """
  record = text.Batch.get(text.Batch.batchid == batchid)
  if (record.phase == vunlp.RUNNINGPAR) & (files_in_batch(batchid) == 0):
    record.delete_instance()

#
# Functions for texts
#

#def put_infile_in_database(batchid, textid, content):
def insert_text(batchid, textid, content):
  """Write a text to be processed in the database
 
     Raise exception if a batch with given ID does not exist.
     If a record with the given batchid and text-handle pre-existed, replace the text in this record.

     @param batchid: Handle of the batch,
     @param name: Handle of the file,
     @param content: Text to be processed in the batch.
  """
  if not known_batchid(batchid):
     raise Exception('Batch ' + str(batchid) + 'not known')
  record = _get_or_create_text_record(batchid, textid)
  record.intext = zlib.compress(content)
  record.phase = vunlp.INITPAR
  record.save()   


def get_filephase(batchid, textid):
  """
    Get the status of a text or raise exception
  """
  return _get_text_record(batchid, textid).phase


def logfile_has_content(batchid, fileid):
  """Return True when the logfile has content"""
  return not (_get_text_record(batchid, fileid).logtext == None)

def count_files_to_upload():
  """count nuber of files that should be uploaded"""
  return text.Text.select().join(text.Batch).where(
            (text.Text.phase == vunlp.INITPAR) & (text.Batch.phase == vunlp.RUNNINGPAR)
         ).count()


def nr_unready_files():
  """count number of files that are not parseready"""
  nr_unreadyfiles = text.Text.select().where(
            (text.Text.phase != vunlp.PARSEREADYPAR) & (text.Text.phase != vunlp.LOGPARSEREADYPAR) & (text.Text.phase != vunlp.TIMEOUTPAR)
         ).count()
  logging.debug("Number of files to submit to host: " + str(nr_unreadyfiles))
  return text.Text.select().where(
            (text.Text.phase != vunlp.PARSEREADYPAR) & (text.Text.phase != vunlp.LOGPARSEREADYPAR) & (text.Text.phase != vunlp.TIMEOUTPAR)
         ).count()
 
def get_files_to_be_uploaded():
  """get files that should be uploaded

     @return: tuples of (text-id, batch-id, recipe, text)
  """
  for record in text.Text.select().join(text.Batch).where(
            (text.Text.phase == vunlp.INITPAR) & (text.Batch.phase == vunlp.RUNNINGPAR)
         ):
    yield (record.name, record.batch.batchid, record.batch.recipe, zlib.decompress(record.intext))
                 

def registrate_uploaded_files(textpointers):
  """
  Mark texts as uploaded

  @param textpointers: list of tuples (batchid, textid)
  """
  for (batchid, textid) in textpointers:
     record = _get_text_record(batchid, textid)
     record.phase = vunlp.SENTPAR
     record.save()

def store_parse_or_log(batchid, textid, content, filtyp):
  """
  Put a downloaded parse or log in the database.

  @param batchid: batch-id
  @param textid: filename
  @param content: file-type with content
  @param filtyp: 'parse' or 'log' (otherwise: exception)
  """
  contentstr = zlib.compress(content.read())
  try:
    record = _get_text_record(batchid, textid)
  except Exception as e:
    logging.error("Try to store a " + filtyp + " for a non-existing text: " + batchid + "/" + textid)
    return
  oldfilephase = get_filephase(batchid, textid)
  if filtyp == 'parse':
    if oldfilephase == vunlp.LOGREADYPAR:
      newfilephase = vunlp.LOGPARSEREADYPAR
    else:
      newfilephase = vunlp.PARSEREADYPAR
    record.outtext = contentstr
  elif filtyp == 'log':
    if oldfilephase == vunlp.PARSEREADYPAR:
      newfilephase = vunlp.LOGPARSEREADYPAR
    else:
      newfilephase = vunlp.LOGREADYPAR
    record.logtext = contentstr
  else:
    raise Exception('wrong filetype keyword: ' + filtyp)
  record.phase = newfilephase
  record.save()

def _itemkindparameter(itemkind):
    """
    Convert 'parse' into 'vunlp.PARSEREADYPAR'  and
    convert 'log' into 'vunlp.LOGREADYPAR'.
    """
    if itemkind == 'parse':
        return vunlp.PARSEREADYPAR
    else:
        return vunlp.LOGREADYPAR


  
def count_ready_items(batchid, itemkind):
  """
  Count number of parses or logs that are ready to be downloaded by the owner
  @param batchid:
  @param itemkind: 'parse' or 'log'
  """
  return text.Text.select().join(text.Batch).where(
           (text.Batch.batchid == batchid) & (
              (text.Text.phase == vunlp.LOGPARSEREADYPAR) 
                   | 
              (text.Text.phase == _itemkindparameter(itemkind))
           )
         ).count()

def get_text(batchid, textid):
  """Get the text to be parsed from the database or raise exception"""
  return zlib.decompress(_get_text_record(batchid, textid).intext)

def get_parse(batchid, textid):
  """Get a parse from the database"""
  return zlib.decompress(_get_text_record(batchid, textid).outtext)

def get_log(batchid, textid):
  """Get a log from the database"""
  return zlib.decompress(_get_text_record(batchid, textid).logtext)

def get_content(batchid, textid, contenttype):
  """Get text, parse or log if available. Otherwise, return None"""
  try:
    record = _get_text_record(batchid, textid)
  except Exception as e:
    return None
  if contenttype == 'text':
    return zlib.decompress(record.intext)
  if (contenttype == 'log') & (record.phase in [ vunlp.LOGREADYPAR, vunlp.LOGPARSEREADYPAR ]):
    return zlib.decompress(record.logtext)
  if (record.phase in [ vunlp.PARSEREADYPAR, vunlp.LOGPARSEREADYPAR ]):
    return zlib.decompress(record.outtext)
  return None


def remove_text(batchid, textid):
  """
   Remove a text record

   Remove the batch if it has no texts left
  """
  _get_text_record(batchid, textid).delete_instance()
  remove_empty_batch(batchid)
 

def get_ready_items(batchid, itemkind):
   """
   Get the ready parses from the batch and remove them from the database
   @param batchid:
   @param itemkind: 'parse' or 'log'
   """
   removelist = []
   for record in text.Text.select().join(text.Batch).where(
      (text.Batch.batchid == batchid) & (
         (text.Text.phase == _itemkindparameter(itemkind)) 
                 | 
         (text.Text.phase == vunlp.LOGPARSEREADYPAR)
      )
      ):
        textid = record.name
        logging.debug("get {} item {}".format(itemkind, textid))
        if itemkind == 'log':
            content = zlib.decompress(record.logtext)
        else:
            content = zlib.decompress(record.outtext)
            removelist.append(textid)
        yield [ textid, content ]
   for textid in removelist:
       logging.debug("To remove {}".format(textid))
       remove_text(batchid, textid)


                    
def mark_timedout(batchid, textid):
  """Set phase of file to timeout"""
  record = _get_text_record(batchid, textid)
  record.phase = vunlp.TIMEOUTPAR
  record.save()
 

#
# Test, try
#
if __name__ == '__main__':
  text.Batch.create_table()
  text.Text.create_table()





