from dbmother.momma import *
import logging
logging.basicConfig(level= logging.DEBUG)
#from twisted.spread import pb
#from twisted.internet import reactor
#serverfactory = pb.PBServerFactory(MommaRootPb())
#reactor.listenTCP(8789, serverfactory)
#reactor.run()
#app.run(save=0)
m= MotherInitializer()
from dbmother.pooling import POOLTYPE_ELASTIC, DB_PGRES
m.init_db(POOLTYPE_ELASTIC, 4, DB_PGRES, 'user', 'pwd', 'db')
