'''
Created on Nov 16, 2020

@author: sunil.thakur
'''
from abc import abstractmethod

from core.app import Kernel

class DataSource(object):
    
    @classmethod
    def create(self, config):        
        if config['type'] == 's3':
            return S3DataSource(config['options'])
        elif config['type'] == 'local':
            return LocalDataSource(config['options'])
        else:
            raise Exception('Invalid data source type!')
        
        
class S3DataSource(DataSource):
    
    def __init__(self, config):
        print("Creating S3 data source")
        self.options = config
        
    def load(self):
        context = Kernel.get_instance().get_context()
        self.options['filepath'] = 's3://'+self.options['bucket']+'/'+self.options['filepath']
        del self.options['bucket']
        context.load(self.options)
        
    def save(self, dataset):
        context = Kernel.get_instance().get_context()
        context.save(dataset, self.options)
        
        
class LocalDataSource(DataSource):
    
    def __init__(self, config):
        print("Creating local data source")
        self.options = config
        
    def load(self):
        context = Kernel.get_instance().get_context()
        return context.load(self.options)
        
    def save(self, dataset):
        context = Kernel.get_instance().get_context()
        context.save(dataset, self.options)
        