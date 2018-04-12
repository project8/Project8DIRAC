"""Base class to give baseline for all the FCCondition plugins.
     It is important to note that in the FCConditionParser, the plugin is called for each
     and every lfn. This greatly simplifies the development of the plugin.
"""
__RCSID__ = "2363ca8 (2015-09-15 17:32:28 +0200) Christophe Haen <christophe.haen@cern.ch>"

class FCConditionBasePlugin( object ):
  """Base class to give baseline for all the FCCondition plugins.
     It is important to note that in the FCConditionParser, the plugin is called for each
     and every lfn. This greatly simplifies the development of the plugin.
  """

  def __init__( self, conditions ):
    """ Gives the parameter of the evaluation to be done.
        The expression is defined as
          <PluginName> = <Plugin params>
        conditions is the string <Plugin params>
        They have to be interpreted
    """

    self.conditions = conditions

  def eval( self, **kwargs ):
    """ eval evaluates param of the plugins against all the info given
        and returns True or False.
    """
    raise NotImplementedError( "to be implemented in the derived class" )
