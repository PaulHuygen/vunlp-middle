#!/usr/bin/env python
# vunlptextstore.db -- Object relational databasemodel for vunlp webservice
#                      Using Peewee 
#20130713 Paul Huygen
from peewee import *
import parameters
#db = SqliteDatabase('../db/vunlptextstore.db')
#db = SqliteDatabase('/usr/local/share/vunlp/db/vunlptextstore.db')
db = SqliteDatabase(parameters.DBFILE)

class Batch(Model):
    batchid = PrimaryKeyField()
    phase = CharField(null=True)
    recipe = CharField(null=True)

    class Meta:
        database = db

class Text(Model):
    batch = ForeignKeyField(Batch)
    intext = BlobField(null=True)
    logtext = BlobField(null=True)
    name = TextField(null=False)
    outtext = BlobField(null=True)
    phase = TextField(null=True)

    class Meta:
        database = db

if __name__ == '__main__':
  import sys
  (prog, arg) = sys.argv
  if arg == 'create_database':
    print('Creating database')
    Batch.create_table()
    Text.create_table()
  else:
    print('usage: ' + prog + ' create_database')
    

