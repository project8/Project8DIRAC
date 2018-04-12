# $HeadURL$
__RCSID__ = "b918666 (2009-11-20 17:39:43 +0000) Adria Casajus <adria@ecm.ub.es>"

import socket

##############################################################
#
#   Class to wrap Fake socket as if it was a real one
#
##############################################################

class FakeSocket:

  def __getattr__(self, name):
    return getattr( self.sock, name )

  def __init__(self, sock, copies = 0):
    self.iCopies = copies
    self.sock = sock

  def close(self):
    if self.iCopies == 0:
      self.sock.shutdown()
      self.sock.close()
    else:
      self.iCopies -= 1

  def makefile(self, mode, bufsize=None):
    self.iCopies += 1
    return socket._fileobject( self.sock, mode, bufsize)
