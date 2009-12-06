# file: pgres.py
import logging
import psycopg2
from dbmother.modb import IMotherDb
from dbmother.mocoms import YELLOW, GREEN

class DbIface(IMotherDb):

  withOid= 1

  def __init__(self, user, pwd, dbname, host= None, port= 5432):
    if not host:
      s= "dbname=%s user=%s password=%s" % (dbname, user, pwd)
    else:
      s= "dbname=%s user=%s host=%s password=%s port=%d" % (dbname, user, host, pwd, port) 
    self.connection= psycopg2.connect(s)
    self.cursor= self.connection.cursor()

  def rollback(self):
    self.connection.rollback()
  def commit(self):
    self.connection.commit()
  def close(self):
    self.connection.close()

  def _execute(self, q, d):
    logging.debug(GREEN(self.session_name) + ': '+ YELLOW(self.cursor.mogrify(q,d)))
    d= d or None
    self.cursor.execute(q, d)

  def _executemany(self, q, l):
    self.cursor.executemany(q, l)

  def _extract(self):
    c= self.cursor
    cres= c.fetchall()
    desc= c.description
    res= []
    for rec in cres:
      drec= {}
      for n, field in enumerate(rec):
        drec[desc[n][0]]= field
      res.append(drec)
    return res

  def _gquery(self, q, d):
    self._execute(q, d)
    return self._extract()

  def _qquery(self, q, d):
    self._execute(q, d)

  def _mqquery(self, q, l):
    self._executemany(q, l)

  def _mgquery(self, q, l):
    self._executemany(q, l)
    return self._extract()

  def insert(self, sql, d, tbl):
    if self.withOid:
      self.oc_query(sql, d)
      lastoid= self.cursor.lastrowid
      _id= self.ov_query('select id from %s where oid = %d ' % (tbl, lastoid))
      d= {'id': _id}
      return d
    else:
      a= 1/0


