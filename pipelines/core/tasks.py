'''
Created on Nov 15, 2020

@author: sunil.thakur
'''
from abc import abstractmethod
from pipelines.core.transformers import Transformer

class Task(object):
    
    @classmethod
    def create(cls, context, config):
        if config['type'] == 'transform':
            return TransformationTask(context, config)
        else:
            raise 'Invalid task type'
    
    def __init__(self, context, config):
        print('Initializing ', config['name'])
        
        self.context = context
        self.config = config
        self.name = config['name']
        self.worker = None
        
        self.result = None
        self.inputs = {}
        self.iterations = False
        
        if 'inputs' in config:
            for input_source in config['inputs']:
                self.inputs[input_source] = context.load_data(input_source)
                
        if 'depends-on' in config:
            for input_process in config['depends-on']:
                self.inputs[input_process] = context.load_data(input_process)
                
        if 'iterations' in config:
            self.iterations = config['iterations']
        
        if 'worker' in config:
            self.worker = context.load_worker(config['worker']['class'], config['worker']['type'], config['worker']['params'])
        
        if 'outputs' in config:
            self.outputs = config['outputs']
        else:
            self.outputs = None

        
    def load(self):
        ''' '''
        
    def validate(self):
        if len(self.inputs) == 0:
            print('No valid input source available for this task.')
            return False
        
        if not self.worker:
            print('No valid worker component available for this task.')
            return False
        
        return True
    
    @abstractmethod
    def run(self):
        ''' '''    
                

class TransformationTask(Task):
    
    def __init__(self, context, config):
        super(TransformationTask, self).__init__(context, config)
        
    def run(self):
        print('Running task ', self.name)
        if super(TransformationTask, self).validate():
        
            if not self.worker:
                functions = self.config["functions"]
                self.worker = Transformer(functions)
                
            datasets =  self.worker.transform(self.inputs)

            self.context.save_output(self.name, datasets, self.outputs)
         
        else:
            print('Error running this task')
        
