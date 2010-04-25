# Author: Federico Tomassini aka efphe
# Thanks to: http://wubook.net/ http://en.wubook.net/
# License: BSD license. See `LICENSE` file at the root
#          of python module
_commajoin= ','.join
from dbmother.pooling import *
import logging

class MommaSql:
  argFrmt= None
  def _equalKeys(self, d, skipid= 1):
    sl= []
    af= self.argFrmt
    for k, v in d.iteritems():
      if k == 'id' and skipid: continue
      sl.append('%s= %s' % (str(k), af % k))
    return sl

  def _updict(self, d):
    sl= self._equalKeys(d)
    return _commajoin(sl)

  def _selict(self, d, f):
    sl= self._equalKeys(d, skipid= 0)
    what= f and _commajoin(f) or '*'
    return what, ' and '.join(sl)

  def _insict(self, d):
    sls= []
    sld= []
    af= self.argFrmt
    for k, v in d.iteritems():
      if k == 'id': continue
      sls.append(k)
      sld.append(af % k)
    return _commajoin(sls), _commajoin(sld)

  def _delict(self, d):
    if 'id' in d:
      return _commajoin(self._equalKeys({'id': d['id']}, skipid= 0))
    return _commajoin(self._equalKeys(d))


class MoMap:
  def __init__(self, fmap):
    fil= open(fmap, 'rb')
    import cPickle
    map_dicts= cPickle.load(fil)
    self._map_file= map_file
    self._map_fields= map_dicts['K']
    self._map_pkeys= map_dicts['P']
    self._map_children= map_dicts['C']
    self._map_rels= map_dicts['R']
    fil.close()

class MommaRoot:
  def __init__(self):
    self.momap= None
    self.pooling= None

  def init_mother_pooling(self, ptype, plimit, dbtype, *args, **kwargs):
    MotherPooling._dbPoolLimit= plimit
    MotherPooling._dbPoolType= ptype
    if dbtype == DB_PGRES:
      from pgres import DbIface
      MommaSql.argFrmt= '%%(%s)s'
    elif dbtype == DB_SQLITE:
      from sqlite import DbIface
      MommaSql.argFrmt= ':s'
    else:
      a= 1/0
    MotherPooling._dbClass= DbIface
    MotherPooling._dbArgs= args
    MotherPooling._dbKwargs= kwargs
    Momma.pooling= MotherPooling(0,0,0)

  def init_momap(self, fmap):
    try:
      self.momap= MoMap(fmap)
    except: pass

Momma= MommaRoot()
def MotherSession(name= None):
  pooling= Momma.pooling
  return pooling.getDb(name)

class WMotherSession(object):
  def __init__(self, name= None, ret= -1, skipException= 1, session= None):
    self.name= name
    self.session= session
    self.ret= ret
    self.skipException= skipException
  def __enter__(self):
    if self.session: 
      return self.session, self.ret
    ses= MotherSession(self.name)
    self.session= ses
    return ses, self.ret
  def __exit__(self, type, value, traceback):
    self.session.endSession(type)
    if type or value:
      logging.error('Session Error: %s, %s', str(type), str(value))
    return self.skipException

class MotherInitializer:
  def init_db(self, ptype, plimit, dbtype, *a, **kw):
    Momma.init_mother_pooling(ptype, plimit, dbtype, *a, **kw)

def init_mother(ptype, plimit, dbtype, *a, **kw):
  i= MotherInitializer()
  i.init_db(ptype, plimit, dbtype, *a, **kw)

def pg_init_mother(ptype, plimit, user, pwd, dbname, host= None, port= 5432):
  init_mother(ptype, plimit, DB_PGRES, user, pwd, dbname, host= host, port= port)

class DbMother(MommaSql):
  def __init__(self, session, tbl, store= {}):
    self.tableName= tbl
    self.store= store
    self.session= session
    self.moved= []

  def setFields(self, d):
    self.store.update(d)
  def setField(self, k, v):
    self.store[k]= v
  def getFields(self, unsafe= 1):
    if unsafe:
      return self.store
    return self.store.copy()
  def getField(self, k, default= None):
    return self.store.get(k, default)

  def update(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    vls= self._updict(store)
    sql= 'UPDATE %s set %s where id = %d' % (self.tableName, vls, store['id'])
    ses.oc_query(sql, store)
    return self

  def insert(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    _vl, _vlv= self._insict(store)
    sql= 'INSERT INTO %s (%s) VALUES (%s)' % (self.tableName, _vl, _vlv)
    d= ses.insert(sql, store, self.tableName)
    store.update(d)
    return self

  def load(self, d= {}, fields= [], safe= False):
    ses= self.session
    store= self.store
    store.update(d)
    what, ftr= self._selict(store, fields)
    sql= 'select %s from %s where %s' % (what, self.tableName, ftr)
    if safe:
      dd= ses.mr_query(sql, store)
      assert len(dd) < 2
      if dd: dd= dd[0]
      else: dd= {}
    else:
      dd= ses.or_query(sql, store)
    store.update(dd)
    return self

  def delete(self, d= {}):
    ses= self.session
    store= self.store
    store.update(d)
    ftr= self._delict(store)
    sql= 'delete from %s where %s' % (self.tableName, ftr)
    ses.oc_query(sql, store)
    return self

