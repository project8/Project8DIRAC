# $HeadURL$
__RCSID__ = "7b8878b (2009-11-05 19:40:01 +0000) Adria Casajus <adria@ecm.ub.es>"

from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dip'  : { 'transport'  : PlainTransport.PlainTransport,
                             'sanity'     : PlainTransport.checkSanity,
                             'delegation' : PlainTransport.delegate
                           },
                  'dips' : { 'transport'  : SSLTransport.SSLTransport,
                             'sanity'     : SSLTransport.checkSanity,
                             'delegation' : SSLTransport.delegate
                           }
                 }

gDefaultProtocol = 'dips'