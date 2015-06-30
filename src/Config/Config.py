__author__ = 'eran'


class Config(object):
    conf = dict()
    #conf['clfFile'] ='/home/ise/Model/dismodel.pkl'
    #conf['clfFile'] ='/Users/avisegal/models/dtew/dismodel.pkl'
    conf['clfFile'] ='/home/eran/Documents/Lassi/src/Algorithem/Model/dismodel.pkl'

    #conf['strmLog'] = '/home/ise/Logs/streamer.log'
    conf['strmLog'] = '/home/eran/Documents/Logs/streamer.log'

    #conf['predLog'] = '/home/ise/Logs/predictor.log'
    conf['predLog'] = '/home/eran/Documents/Logs/predictor.log'

    #conf['dis_predLog'] = '/home/ise/Logs/dis_predictor.log'
    conf['dis_predLog'] = '/home/eran/Documents/Logs/dis_predictor.log'

    conf['debug'] = False

    conf['user'] = 'root'

    conf['password'] = '9670'

    conf['host'] = 'localhost'

    conf['db'] = 'streamer'



