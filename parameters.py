#!/usr/bin/env python
# Parameters for vunlp package
# Written by installscript on di jul 30 15:02:31 CEST 2013
import os
VUNLPHOME = os.path.expanduser('~') + '/vunlp'
# DATABASE:
DBFILE = VUNLPHOME + "/vunlptextstore.db"
# Webservice host in local test mode:
DEFAULT_URL = "http://localhost:8090"
LOCKFILE =  VUNLPHOME + "/lock/cronlock"
CRONLOGFILE = VUNLPHOME + "/log/cronlog"
# # Super-host: Access info in local test mode
# SUPER_HOSTNAME = "localhost"
# SUPER_USER     = "paul"
# SUPER_ROOT     = VUNLPHOME
# Super-host: Access info for Lisa
SUPER_HOSTNAME = "lisa.surfsara.nl"
SUPER_USER     = "phuijgen"
SUPER_ROOT     = "/home/phuijgen/nlp/service"
# # Super-host: Scripts in local test mode
# SUPER_UPLOADSCRIPT            = VUNLPHOME + "/bin/from_middle"
# SUPER_DOWNLOADTEMPLATE        = VUNLPHOME + "/bin/to_middle {tray}"
# SUPER_FILECOUNTINTRAYTEMPLATE = VUNLPHOME + "/bin/nrfiles {tray}"
# SUPER_FILELISTTEMPLATE        = VUNLPHOME + "/bin/filelist {tray}"
# Super-host: Scripts in local test mode
SUPER_UPLOADSCRIPT            = SUPER_ROOT + "/bin/download_archive"
SUPER_DOWNLOADTEMPLATE        = SUPER_ROOT + "/bin/upload {tray}"
SUPER_FILECOUNTINTRAYTEMPLATE = SUPER_ROOT + "/bin/nrfilesintray {tray}"
SUPER_FILECOUNTREADYPARSESTEMPLATE = SUPER_ROOT + "/bin/coundreadyparses"
SUPER_FILELISTTEMPLATE        = SUPER_ROOT + "/bin/filelist {tray}"
SUPER_STARTMANAGERSCRIPT      = SUPER_ROOT + "/bin/alpinomanager"
# # Super-host: Path to trays
# SUPER_TRAYSPATH    = VUNLPHOME + "/trays"
# Super-host: Path to trays
SUPER_TRAYSPATH    = SUPER_ROOT
SUPER_INTRAYNAME = "intray"
SUPER_OUTTRAYNAME = "outtray"
SUPER_LOGTRAYNAME = "logtray"
SUPER_TIMEOUTTRAYNAME = "timeouttray"
# Test superhost: Access info
TSUPER_HOSTNAME = "localhost"
TSUPER_USER     = "paul"
TSUPER_ROOT     = VUNLPHOME
# Test superhost: Scripts
TSUPER_UPLOADSCRIPT            = VUNLPHOME + "/bin/from_middle"
TSUPER_DOWNLOADTEMPLATE        = VUNLPHOME + "/bin/to_middle {tray}"
TSUPER_FILECOUNTINTRAYTEMPLATE = VUNLPHOME + "/bin/nrfiles {tray}"
TSUPER_FILELISTTEMPLATE        = VUNLPHOME + "/bin/filelist {tray}"
TSUPER_EMPTY_TRAYS_SCRIPT      = VUNLPHOME + "/bin/emptytrays"
# Test super host: Paths
TSUPER_TRAYSPATH    = VUNLPHOME + "/trays"
#TSUPER_INTRAY      = ""
#TSUPER_PARSESTRAY  = ""
#TSUPER_LOGTRAY     = ""
#TSUPER_TIMEOUTTRAY = ""
