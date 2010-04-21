# Author: Federico Tomassini aka efphe
# Thanks to: http://wubook.net/ http://en.wubook.net/
# License: BSD license. See `LICENSE` file at the root
#          of python module
from Queue import Queue
from threading import Lock
from random import randint
import logging

POOLTYPE_ELASTIC= 1
POOLTYPE_GROWING= 2
POOLTYPE_LIMITED= 3

DB_PGRES= 1
DB_SQLITE= 2

_mtimeout= 4

""" 
  Elastic: when no connection is ready, build one. Once done, destroy it
  Growing: when no connection is ready, build one and store it.
  Limited: when no connection is ready, build one if allowed
"""

class MotherPooling(Queue):

  # If you set these values, __init__ values
  # are ignored
  _dbClass= None
  _dbArgs= None
  _dbKwargs= None
  _dbPoolType= None
  _dbPoolLimit= None

  def __init__(self, poolType, poolLimit, dbClass, *args, **kwargs):
    """ poolLimit: if elastic, this is the min connection number, otherwise this is the 
        max number of connection """
    self.poolType= self._dbPoolType or poolType
    self.dbClass= self._dbClass or dbClass
    self.args= self._dbArgs or args
    self.kwargs= self._dbKwargs or kwargs
    self.dbifaces= 0
    self.poolLimit= self._dbPoolLimit or poolLimit
    # We Need a Lock because, altough Queue is thread safe,
    # we want a full control on queue size
    self.mpoolingLock= Lock()
    self.dbemployed= []
    Queue.__init__(self)

  def _buildDb(self):
    self.dbifaces+= 1
    return self.dbClass(*self.args, **self.kwargs)

  def getDb(self, name= None):
    db= self._getDb()
    db.set_name(name or 'MotherSession-%d' % randint(1, 12345), self)
    return db

  def _getDb(self):
    self.mpoolingLock.acquire()

    def _retdb(d):
      if not d:
        d= self._buildDb()
      if d not in self.dbemployed:
        self.dbemployed.append(d)
      self.mpoolingLock.release()
      return d

    try:
      db= self.get_nowait()
      return _retdb(db)
    except: pass

    if self.poolType != POOLTYPE_LIMITED:
      return _retdb(0)

    if self.dbifaces < self.poolLimit:
      return _retdb(0)

    self.mpoolingLock.release()
    db= self.get(1, _mtimeout)
    self.mpoolingLock.acquire()
    return _retdb(db)

  def _removeFromEmployed(self, db):
    self.mpoolingLock.acquire()
    if db in self.dbemployed:
      self.dbemployed.remove(db)
    self.mpoolingLock.release()

  def putDb(self, db):
    self.mpoolingLock.acquire()
    try:
      logging.debug('emptying dbemployed...')
      self.dbemployed.remove(db)
    except: pass
    if self.poolType != POOLTYPE_ELASTIC or self.dbifaces < self.poolLimit:
      logging.debug('session back home...')
      self.put(db)
    else:
      logging.debug('discarding session...')
      try:
        db.close()
      except: pass
      del db
      self.dbifaces-= 1
    self.mpoolingLock.release()

class FooDb:
  def __init__(self, *a, **kw):
    pass

class MotherPoolingElastic(MotherPooling):
  def __init__(self, dbClass, *args, **kwargs):
    MotherPooling.__init__(self, POOLTYPE_ELASTIC, None, dbClass, *args, **kwargs)
class MotherPoolingGrowing(MotherPooling):
  def __init__(self, dbClass, *args, **kwargs):
    MotherPooling.__init__(self, POOLTYPE_GROWING, None, dbClass, *args, **kwargs)
class MotherPoolingLimited(MotherPooling):
  def __init__(self, limit, dbClass, *args, **kwargs):
    MotherPooling.__init__(self, POOLTYPE_LIMITED, limit, dbClass, *args, **kwargs)

