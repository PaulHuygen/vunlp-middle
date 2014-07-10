#!/usr/bin/python
###########################################################################
#          (C) Vrije Universiteit, Amsterdam (the Netherlands)            #
#                                                                         #
# This file is part of VuNLP - The VU university NLP tools                #
#                                                                         #
# VuNLP is free software: you can redistribute it and/or modify it under  #
# the terms of the GNU Affero General Public License as published by the  #
# Free Software Foundation, either version 3 of the License, or (at your  #
# option) any later version.                                              #
#                                                                         #
# VuNLP is distributed in the hope that it will be useful, but WITHOUT    #
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or   #
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public     #
# License for more details.                                               #
#                                                                         #
# You should have received a copy of the GNU Affero General Public        #
# License along with AmCAT.  If not, see <http://www.gnu.org/licenses/>.  #
###########################################################################
"""Cron job transports files to and from supercomputer Lisa."""
from __future__ import unicode_literals, print_function, absolute_import
#import vunlpdb as db
import db
import sqlite3
import zlib
import parameters
import os
import shutil
import tarfile
import paramiko, base64
import tempfile
#import supercomputer
import superhost
import lockfile
import sys
import logging
import glob

if not os.path.exists(parameters.LOCKDIR):
   os.makedirs(parameters.LOCKDIR)
lock = lockfile.FileLock(parameters.LOCKFILE)
if not os.path.exists(parameters.CRONLOGDIR):
   os.makedirs(parameters.CRONLOGDIR)
logfil = parameters.CRONLOGFILE
logging.basicConfig(filename = logfil, level = logging.DEBUG)

shost = \
  superhost.SuperHost( hostname = parameters.SUPER_HOSTNAME \
                     , username = parameters.SUPER_USER \
                     , uploadscript = parameters.SUPER_UPLOADSCRIPT \
                     , downloadtemplate = parameters.SUPER_DOWNLOADTEMPLATE \
                     , nr_files_in_tray_template = parameters.SUPER_FILECOUNTINTRAYTEMPLATE \
                     , filelist_in_tray_template = parameters.SUPER_FILELISTTEMPLATE \
                     )

RSYNCUPCOMMAND = "rsync -Oavz --remove-source-files {localtray}/* {user}@{host}:{remotetray}/ "
RSYNCDOWNCOMMAND = "rsync -Oavz --remove-source-files {user}@{host}:{remotetray}/ {outtray}"
#RSYNCDOWNCOMMAND = "rsync -Oavz  {user}@{host}:{remotetray}/ {outtray}"

def internal_filename(textid, batchid):
  """Return a unique filename based on external filename and batch-id"""
  return str(batchid) + '_' + textid

def crack_internal_filename(intfilnam):
  """Retrieve batch ID and  external filename from internal filename"""
  batchid, sep, textid = intfilnam.partition('_')
  return (batchid, textid)
   

# def create_uploadfile(batchid, textid, recipe, content):
#   """ Create a temporary file-object to be uploaded to Lisa
# 
#   The first line in the file-object is '#!', followed by the parse command.
#   The remainder is the content of a textfile submitted by a user.
#   Returns the handler to the file-object, ready to be read.
#   """
#   f = tempfile.TemporaryFile()
#   f.write("#!" + recipe + '\n')
#   f.write(content)
#   f.seek(0)
#   return f

def create_uploadfile_for_rsync(filpath, recipe, content):
  """ Create a temporary file-object to be uploaded to Lisa

  The first line in the file-object is '#!', followed by the parse command.
  The remainder is the content of a textfile submitted by a user.
  Returns the handler to the file-object, ready to be read.
  """
  f = open(filpath, 'w')
  f.write("#!" + recipe + '\n')
  f.write(content)
  f.close()

#def supercomputer():
#  client = paramiko.SSHClient()
#  client.load_system_host_keys()
#  client.connect(parms.superchost, username=parms.supercuser)
#  return client

#def count_parses_to_download(superco):
#  """Asks number of ready parses on Lisa
#
#     @return: number of files in parse-tray
#  """
#  try:
##     supin, supout, superr = superco.execute_command(parms.supercntreadys)
#     supin, supout, superr = superco.execute_command(parameters.SUPER_FILECOUNTINTRAYTEMPLATE.format(tray = 'parse'))
#     for line in supout:
#       count = line.strip('\n')
#     supin.channel.shutdown_write()
#     return count  
#  except SSHException:
#     return False
#  except:
#     return False


#def send_uploads_to_supercomputer(superco):
#  """Send texts to supercomputer """
#  try:
#    supin, supout, superr = superco.execute_command(parms.superupscript)
#    tarf = tarfile.open('', 'w:gz', supin)
#    sendlist = []
#    for y in db.get_files_to_be_uploaded():
#       fileid = y[0]
#       batchid = y[1]
#       ofilnam = y[2]
#       recipe = y[3]
#       comprestext = y[4] 
#       sendlist.append(fileid)
#       f = create_uploadfile(batchid, internal_filename(ofilnam, batchid), recipe, comprestext)
#       ti = tarf.gettarinfo('', internal_filename(ofilnam, batchid), f)
#       tarf.addfile(ti, f)
#    tarf.close()
#    supin.channel.shutdown_write()
#    db.registrate_uploaded_files(sendlist)
#  except:
#     logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
#     logging.error("Cannot send upload")

# def send_uploads_to_supercomputer():
#   """Send texts to supercomputer """
#   logging.debug("Send uploads")
#   try:
#     shost.open_out()
#     sendlist = []
#     for (textid, batchid, recipe, comprestext) in db.get_files_to_be_uploaded():
#        sendlist.append((batchid, textid))
#        filnam = internal_filename(textid, batchid)
#        f = create_uploadfile(batchid, filnam, recipe, comprestext)
#        shost.next_out(filnam, f)
#     shost.close_out()
#     db.registrate_uploaded_files(sendlist)
#   except:
#      logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
#      logging.error("Cannot send upload")

def send_uploads_to_supercomputer():
  """Send texts to supercomputer, using rsync """
  logging.debug("Send uploads")
  remotetray = parameters.SUPER_TRAYSPATH + '/intray' 
  try:
    sendlist = []
    temptray = tempfile.mkdtemp()
    logging.debug("Temporary storage for uploads in :" + temptray)
    for (textid, batchid, recipe, comprestext) in db.get_files_to_be_uploaded():
       sendlist.append((batchid, textid))
       filnam = internal_filename(textid, batchid)
       filpath = os.path.join(temptray, filnam)
       f = create_uploadfile_for_rsync(filpath, recipe, comprestext)
    os.system(RSYNCUPCOMMAND.format( user = parameters.SUPER_USER     \
                                   , host = parameters.SUPER_HOSTNAME \
                                   , remotetray = remotetray \
                                   , localtray = temptray \
                                   )  \
    )
    logging.debug("Marking files as uploaded.")
    db.registrate_uploaded_files(sendlist)
    shutil.rmtree(temptray)
  except:
     logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
     logging.error("Cannot send upload")
     shutil.rmtree(temptray)


#def download_parses(superco):
#  try:
#    supin, supout, superr = superco.execute_command(parms.superdwnload)
#    tarq = open('/tmp/tarf.tgz', 'w')
#    tarq.write(supout.read())
#    tarq.close()
#    tarf = tarfile.open('/tmp/tarf.tgz', 'r:gz')
#    for parse in tarf.getmembers():
#      batchid, textid = crack_internal_filename(parse.name)
##      db.store_parse(batchid, name, tarf.extractfile(parse))
#      db.store_parse_or_log(batchid, textid, tarf.extractfile(parse), 'parse')
#    tarf.close()
#    supin.channel.shutdown_write()
#    os.remove('/tmp/tarf.tgz')
#  except:
#     logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
#     logging.error("Cannot download parses")

# def download_result(shost, tray):
#   """ Download parses or logs and put in database
# 
#   TODO: Find out why this doesn't work (weird errors, seemingly incorrect unpacking)
#   @param tray: 'parse' or 'log'
#   @return: <ReturnValue>
#   """
#   try:
#     shost.open_in(tray)
#     for (intname, text) in shost.next_in():
#       batchid, textid = crack_internal_filename(intname)
#       db.store_parse_or_log(batchid, textid, text, tray)
#     shost.close_in()
#   except Exception as e:
#      logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
#      logging.error("Cannot download parses")


#    supin, supout, superr = superco.execute_command(parms.superdwnload)
#    tarq = open('/tmp/tarf.tgz', 'w')
#    tarq.write(supout.read())
#    tarq.close()
#    tarf = tarfile.open('/tmp/tarf.tgz', 'r:gz')
#    for parse in tarf.getmembers():
#      batchid, textid = crack_internal_filename(parse.name)
##      db.store_parse(batchid, name, tarf.extractfile(parse))
#      db.store_parse_or_log(batchid, textid, tarf.extractfile(parse), 'parse')
#    tarf.close()
#    supin.channel.shutdown_write()
#    os.remove('/tmp/tarf.tgz')
#  except:
#     logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
#     logging.error("Cannot download parses")

#def download_result(tray):
#  """ Download parses or logs and put in database
#
#  TODO: Find out why this doesn't work (weird errors, seemingly incorrect unpacking)
#  @param tray: 'parse' or 'log'
#  @return: <ReturnValue>
#  """
#  try:
#    shost.open_in(tray)
#    while True:
#      ofilnam, text = shost.next_in()
#      if ofilnam == None:
#        logging.debug("No more files.")
#        break
#      logging.debug("Process " + ofilnam)
#      batchid, textid = crack_internal_filename(ofilnam)
#      db.store_parse_or_log(batchid, textid, text, tray)
#    shost.close_in()
#  except Exception as e:
#    logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
#    logging.error("Argument:" + str(e.args))
#    logging.error("Cannot download result")

def ensure_dir(f):
    """ 
      Make a directory if it does not exist 

      stolen from stackoverflow
    """
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def download_with_rsync(filtype):
   """
     Retrieve ready parses or logfiles

       - Collects ready parses/logs in "localtray"
       - Removes downloaded parses/logs from supercomputer
       - Stores parses/logs in the database
       - removes parses/logs from the outtray

     @param filtype: 'log' or 'parse'
   """   
   logging.debug("About to download " + filtype)
   if filtype == 'parse':
       remotetray = parameters.SUPER_TRAYSPATH + '/' + parameters.SUPER_OUTTRAYNAME
   elif filtype == 'log':
       remotetray = parameters.SUPER_TRAYSPATH + '/' + parameters.SUPER_LOGTRAYNAME 
   else:
       remotetray = parameters.SUPER_TRAYSPATH + '/' + parameters.SUPER_TIMEOUTTRAYNAME 
   temptray = tempfile.mkdtemp()
   logging.debug("Temporary storage for " + filtype + " in :" + temptray)
   os.system(RSYNCDOWNCOMMAND.format( user = parameters.SUPER_USER     \
                                    , host = parameters.SUPER_HOSTNAME \
                                    , remotetray = remotetray \
                                    , outtray = temptray \
                                    ) \
   )
   try:
     for intfilpath in glob.iglob(temptray + "/*"):
        intfilnam = os.path.basename(intfilpath)
        logging.debug("Store " + filtype + " " + intfilnam + " in DB.")
        batchid, textid = crack_internal_filename(intfilnam)
        f = open(intfilpath, 'r')
#        db.store_parse(batchid, filnam, f)
        db.store_parse_log_or_timeout(batchid, textid, f, filtype)
        f.close()
        os.remove(intfilpath)
   except NameError as e:
     logging.error("NameError:" +  str(e))
   except:
     logging.error("Unexpected error (II):" +  str(sys.exc_info()[0]))
#   os.removedirs(temptray)

def download_parses_with_rsync():
   """
     Retrieve ready parses

       - Collects ready parses in "outtray"
       - Removes downloaded parses from supercomputer
       - Stores parses in the database
       - removes parses from the outtray
   """   
   logging.debug("Dwnscript: " + parms.downloadscript)
   ensure_dir(parms.outtray)
   os.system(parms.downloadscript)
   try:
#     logging.debug("Globglob: " + str(glob.glob("/usr/local/share/vunlp/outtray/*")))
     logging.debug("Globglob: " + str(glob.glob(parms.outtray + "/*")))
   except:
     logging.error("Unexpected error (III):" +  str(sys.exc_info()[0]))
   logging.debug("Look for parses in " + parms.outtray + "/*.")
   try:
     for intfilpath in glob.iglob(parms.outtray + "/*"):
        logging.debug("Store " + intfilpath)
        intfilnam = os.path.basename(intfilpath)
        logging.debug("Store parse " + intfilnam + " in DB.")
        batchid, textid = crack_internal_filename(intfilnam)
        f = open(intfilpath, 'r')
        db.store_parse_or_log(batchid, textid, f, 'parse')
        f.close()
        os.remove(intfilpath)
   except AttributeError as e:
     logging.error("Attribute error:" + str(e))
   except:
     logging.error("Unexpected error (IV):" +  str(sys.exc_info()[0]))

#def mark_timeoutfiles():
#  """get list of timed-out from superhost and mark them"""  
#  try:
#    supin, supout, superr = superco.execute_command(parms.supertimoutscript)
#    for intfilnam in supout:
#      batchid, filename = crack_internal_filename(intfilnam.strip('\n'))
#      db.mark_timedout(batchid, filename)
#  except  SSHException:
#     logging.error("SSH Exception occurred.")
#  except: 
#     logging.error("Cannot look for timeoutfiles.")

def mark_timeoutfiles():
  """get list of timed-out from superhost and mark them"""  
  try:
    for ofilnam in shost.files_in('timeout'):
      batchid, textid = crack_internal_filename(ofilnam)
      logging.debug("Timout file: " + ofilnam + ", " + textid + ", " + batchid)
      db.mark_timedout(batchid, textid)
#    supin, supout, superr = superco.execute_command(parms.supertimoutscript)
#    for intfilnam in supout:
#      batchid, filename = crack_internal_filename(intfilnam.strip('\n'))
#      db.mark_timedout(batchid, filename)
  except  SSHException:
     logging.error("SSH Exception occurred.")
  except: 
     logging.error("Cannot look for timeoutfiles.")


def start_alpinomanager():
  shost.execute_command(parameters.SUPER_STARTMANAGERSCRIPT)  


try:
  logging.debug("Start vunlp cron.")
  lock.acquire(-1)
  logging.debug("Lock acquired.")
  if db.nr_unready_files() > 0:
    logging.debug("There are unready files.")
#    superco = supercomputer.Supercomputer()
    cnt = db.count_files_to_upload()
    logging.debug(str(cnt) +  " texts must be uploaded.")
    if cnt > 0:
      send_uploads_to_supercomputer()
#    if shost.nr_of_files_in('parse') > 0:
    logging.debug("Download logs and parses") 
    download_with_rsync('log')
    download_with_rsync('parse')
    download_with_rsync('timeout')
#      download_result('log')
#      download_result('parse')
#    logging.debug("About to find out about timeouts.")
#    mark_timeoutfiles()
#    superco.close()
    start_alpinomanager()
  else:
    logging.debug("Nothing to do.")
  lock.release()
except lockfile.AlreadyLocked:
    logging.debug("Cronjob was already running.")
except Exception as e:
  logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
  logging.error("Argument:" + str(e.args))
  lock.release()

