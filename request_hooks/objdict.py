'''
Created on Aug 24, 2016

@author: christopher.j.castle
'''

class objdict(dict):
    def __getattr__(self, name):
        if name in self:
            returnValue = self[name]
            if type(self[name]) is dict:
                return objdict(returnValue)
            return returnValue
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)
        
    def __getitem__(self, *args, **kwargs):
        returnValue = dict.__getitem__(self, *args, **kwargs)
        if type(returnValue) is dict:
            return objdict(returnValue)
        return returnValue
