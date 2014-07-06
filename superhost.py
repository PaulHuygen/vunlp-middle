#!/usr/bin/env python
# supercomputer -- contact with supercomputer that performs vunlp tasks
# 20130619 Paul Huygen
from __future__ import unicode_literals, print_function, absolute_import
import paramiko, base64
#import vunlpparameters
import unittest
import logging
import sys
import tarfile
import tempfile
import parameters
import StringIO
import tempfile

LISA_HOSTNAME = 'lisa.sara.nl'
LISA_USER = 'phuijgen'
LISAROOT = '/home/phuijgen/nlp'
LISABINDIR = LISAROOT + '/service/bin'
LISA_UPLOADSCRIPT = LISABINDIR + 'download_archive'

HITCH_HOSTNAME = 'hitch'
HITCH_USER = 'paul'
HITCH_BINDIR = '/home/paul/bin'
HITCH_UPLOADSCRIPT = HITCH_BINDIR + '/vunlptestupload'
HITCH_DOWNLOADTEMPLATE = HITCH_BINDIR + '/testdownload {tray}'
HITCH_FILESINTRAYTEMPLATE = HITCH_BINDIR + '/vunlpcntreadies {tray}'
HITCH_TIMEOUTSLISTSCRIPT = HITCH_BINDIR + '/vunlptimeouts'

def copy_fileobjects(infil, outfil, buffer_size = 1024*1024):
   """ copy the contents of file-like objects neatly with a buffer"""
   buffer_size = 1024*1024
   while 1:
     copy_buffer = infil.read(buffer_size)
     if copy_buffer:
         outfil.write(copy_buffer)
     else:
         break
   


class SuperHost(object): #superclass, inherits from default object

  client = None 
  hostname = None
  username = None
  uploadscript = None  
  downloadtemplate = None
  nr_files_in_tray_template = None
  filelist_in_tray_template = None

  def __init__( self    \
              , hostname = parameters.SUPER_HOSTNAME \
              , username = parameters.SUPER_USER
              , uploadscript = parameters.SUPER_UPLOADSCRIPT \
              , downloadtemplate = parameters.SUPER_DOWNLOADTEMPLATE \
              , nr_files_in_tray_template = parameters.SUPER_FILECOUNTINTRAYTEMPLATE \
              , filelist_in_tray_template = parameters.SUPER_FILELISTTEMPLATE \
              ):

      self.hostname = hostname
      self.username = username
      self.uploadscript = uploadscript
      self.downloadtemplate = downloadtemplate
      self.nr_files_in_tray_template = nr_files_in_tray_template
      self.filelist_in_tray_template = filelist_in_tray_template


  def _open_connection(self):
    """Open an SSH connection if it has not already been done"""
    if self.client == None:
      self.client =  paramiko.SSHClient()
      self.client.load_system_host_keys()
      self.client.connect(self.hostname, username=self.username)
    return

  def execute_command(self, command):
    """
     Execute a command on the remote host

    @return: Tuple of file-objects: stdin, stdout, stderror 
    """
    self._open_connection()
    return self.client.exec_command(command)

  def nr_of_files_in(self,tray):
    """
    retrieve the number of files in a tray

    @param tray: 'in', 'parse', 'log' or 'timeout'
    @return: Number of files in tray or None
    """
    try:
      supin, supout, superr = self.execute_command(self.nr_files_in_tray_template.format(tray = tray))
      nrfiles = supout.read()
      return int(nrfiles)
#      supin.channel.shutdown_write()
    except Exception as e:
       logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
       logging.error("Argument:" + str(e.args))
       return None

  def files_in(self, tray):
    """
    retrieve a list of the names of the files in a tray

    @param tray: 'in', 'parse', 'log' or 'timeout'
    @return: iterable of filenames
    """
    try:
      supin, supout, superr = self.execute_command(self.filelist_in_tray_template.format(tray = tray))
      for filnam in supout.readlines():
         yield filnam.strip()
#      supin.channel.shutdown_write()
    except Exception as e:
       logging.error("Unexpected error in files_in:" +  str(sys.exc_info()[0]))
       logging.error("Argument:" + str(e.args))

  def tar_outfile(self):
    """
    Open a tarfile to pass texts to the superhost

    """
    try:
      supin, supout, superr = self.execute_command(self.uploadscript)
      tarf = tarfile.open('', 'w:gz', supin)
#      supin.channel.shutdown_write()
    except Exception as e:
       logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
       logging.error("Argument:" + str(e.args))
       return None
    return tarf
    
  def tar_infile(self, tray):
    """
      Open a tarfile to retrieve texts, parses or logs from the superhost


      Todo: find out why python does not recognize the buffer from SSH as a gz file.   
      @argum: tray : 'in', 'parse' or 'log'
    """
    try:
      supin, supout, superr = self.execute_command(self.downloadtemplate.format(tray = tray))
      temptar = tempfile.TemporaryFile()
#      temptar.write(supout.read())
      copy_fileobjects(supout, temptar)
      temptar.seek(0)
      tarf = tarfile.open('', 'r:gz', temptar)
      supin.channel.shutdown_write()
    except Exception as e:
       logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
       logging.error("Argument:" + str(e.args))
       logging.error("Cannot open input tarfile")
       return None
    return tarf


#  def __init__(self):
#    self.client =  paramiko.SSHClient()
#    self.client.load_system_host_keys()
#    self.client.connect(vunlpparameters.superchost, username=vunlpparameters.supercuser)
#    return

  def open_out(self):
    """open connection with host to upload texts"""
    self.outtar = self.tar_outfile()

  def next_out(self, textid, text):
    """upload a text"""
    ti = self.outtar.gettarinfo('', textid, text)
    self.outtar.addfile(ti, text)
    
  def close_out(self):
    """close the connection with host to upload texts"""
    self.outtar.close()

  def open_in(self, tray):
    """
    open connection with host to download texts, parses or logs

    @param tray: 'in', 'param' or 'log'
    """
    self.intar = self.tar_infile(tray)

  def next_in(self):
    """ Get next text from host

    @return: (None, None) or (textid, file-object)
    """
    for mem in self.intar.getmembers():
       name = mem.name
       f = self.intar.extractfile(mem)
       yield (name, f.read())

#    tinf = self.intar.next()
#    if tinf == None:
#       return (None, None)
#    else:
#      logging.debug("About to read parse: ")
#      text = self.intar.extractfile(tinf).read().encode('utf-8', 'replace')
#      logging.debug("Parse read. ")
#      logging.debug("retrieved parse: " + text)
##     return (tinf.name, self.intar.extractfile(tinf))
#      yield (tinf.name, text)

  def close_in(self):
    """ close connection with host to download texts, parses or logs """
    self.intar.close()

  def close(self):
    """Close the  SSH connection if it exists"""
    if self.client != None:
      self.client.close()      


#class Lisa(SuperHost):
#
#  def __init__(self):
#    self.hostname = LISA_HOSTNAME
#    self.username = LISA_USER
#    self.uploadscript = LISA_UPLOADSCRIPT

class TestSu(SuperHost):

    empty_trays_script = None

    def __init__( self                                               \
                , hostname = parameters.TSUPER_HOSTNAME \
                , username = parameters.TSUPER_USER
                , uploadscript = parameters.TSUPER_UPLOADSCRIPT \
                , downloadtemplate = parameters.TSUPER_DOWNLOADTEMPLATE \
                , nr_files_in_tray_template = parameters.TSUPER_FILECOUNTINTRAYTEMPLATE \
                , filelist_in_tray_template = parameters.TSUPER_FILELISTTEMPLATE \
                , empty_trays_script = parameters.TSUPER_EMPTY_TRAYS_SCRIPT \
                ):
#      super(TestSu, self).__init__(..) doesn't seem to work 
      self.hostname = hostname
      self.username = username
      self.uploadscript = uploadscript
      self.downloadtemplate = downloadtemplate
      self.nr_files_in_tray_template = nr_files_in_tray_template
      self.filelist_in_tray_template = filelist_in_tray_template
      self.empty_trays_script = empty_trays_script
    def empty_trays(self):
      """
      Empty the trays before tsting

      @return: Nothing
      """
      try:
         supin, supout, superr = self.execute_command(self.empty_trays_script)
#         supin.channel.shutdown_write()
      except Exception as e:
          logging.error("Unexpected error:" +  str(sys.exc_info()[0]))
          logging.error("Argument:" + str(e.args))




class Hitch(SuperHost):

    def __init__(self):
      self.hostname = HITCH_HOSTNAME
      self.username = HITCH_USER
      self.uploadscript = HITCH_UPLOADSCRIPT
      self.downloadtemplate = HITCH_DOWNLOADTEMPLATE
      self.nr_files_in_tray_template = HITCH_FILESINTRAYTEMPLATE

class TestHostConnect(unittest.TestCase):

  def setup(self):
    # Create a temp directory
    pass

  def teardown(self):
    pass

  def test_connection(self):
    """Connect an execute a command"""
    shost = TestSu()
    inf, outf, errf = shost.execute_command('echo aap')
    res = outf.read().strip()
    self.assertEqual(res, 'aap')

  def test_upload_files(self):
     """Upload a couple of files"""
     shost = TestSu()
     shost.empty_trays()
     shost.open_out()
     f = tempfile.TemporaryFile()
     f.write('aap') 
     f.seek(0)
     shost.next_out('aap.txt', f)
     f.close()
     f = tempfile.TemporaryFile()
     f.write('noot') 
     f.seek(0)
     shost.next_out('noot.txt', f)
     f = tempfile.TemporaryFile()
     f.write('Mies is slow and goes to timeout') 
     f.seek(0)
     shost.next_out('mies.txt', f)
     f.close()
     shost.close_out()
     res = shost.nr_of_files_in('parse')
     self.assertEqual(res, 2)
     res = shost.nr_of_files_in('log')
     self.assertEqual(res, 2)
     res = shost.nr_of_files_in('timeout')
     self.assertEqual(res, 1)
     shost.close()

  def test_download_files(self):
     """Download the files"""
     shost = TestSu()
     shost.open_in('parse')
     filecontents = dict()
     for (name, text) in shost.next_in():
        filecontents[name] = text
     self.assertEqual( filecontents['aap.txt'], 'aap')
 

if __name__ == "__main__":
    unittest.main()


#  logging.basicConfig(level=logging.DEBUG)
#  hitch = Hitch()
#  for filnam in hitch.get_timeoutslist():
#     print("Timed out: " + filnam)


#  if hitch.can_harvest('parse'):
#     print("Yes, we can")
#  else:
#     print("No, we cannot")
#  hitch.open_out();
#  for filnam in ["aap.txt", "noot.txt"]:
#    logging.debug('Send ' + filnam)
#    f = open(filnam)
#    hitch.next_out(filnam, f)
#    f.close()
#  hitch.close_out()
#
#  hitch.open_in('parse')
#  while True:
#    filnam, text = hitch.next_in()
#    if filnam == None:
#       break
#    f = open(filnam, 'w')
#    copy_fileobjects(text, f)
#    f.close
#  hitch.close_in()
  
