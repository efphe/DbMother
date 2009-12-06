from dbmother.mocoms import GREEN, RED
import logging
class IMotherDb:

  def set_name(self, name, pooling):
    if not hasattr(self, 'pooling'):
      self.pooling= pooling
    self.session_name= name

  def endSession(self, rback= False):
    if not rback:
      logging.debug(GREEN('Committing Session: %s' % self.session_name))
      self.commit()
    else:
      logging.debug(RED('Rollbacking Session: %s' % self.session_name))
      self.rollback()
    self.pooling.putDb(self)

  def oc_query(self, s, filter= None):
    self._qquery(s, filter)

  def mr_query(self, s, filter= None):
    return self._gquery(s, filter)

  def or_query(self, s, filter= None):
    res= self.mr_query(s, filter)
    assert len(res) == 1
    return res[0]

  def ov_query(self, s, filter= None):
    res= self.or_query(s, filter)
    res= res.values()
    assert len(res) == 1
    return res[0]

  def mq_query(self, s, l):
    self._mqquery(s, l)

  def mg_query(self, s, l):
    self._mgquery(s, l)

