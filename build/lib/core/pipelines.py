'''
Created on Nov 15, 2020

@author: sunil.thakur
'''
from core.common.utils import load_class_by_name, has_callable_function
from core.tasks import Task
from core.io.datasources import DataSource

class Pipeline(object):
    
    def __init__(self, config):
       
        # 1. Initialize Pipeline Context
        self.context = PipelineContext(config)
        
        # 2. Get all data sources configured 
        self.datasources = {}
        for datasource in config['datasources']:
            self.datasources[datasource['name']] = datasource
        
        # 3. Get all tasks configured     
        self.tasks = {}
        for task in config['tasks']:
            subtasks = {}
            if 'tasks' in task:
                for subtask in task['tasks']:
                    subtasks[subtask['name']] = subtask
                task['tasks'] = subtasks    
            self.tasks[task['name']] = task
        
        # 4. Pass all the data sources to pipeline context
        self.context.datasources = self.datasources
        
        self._current_output = {}
        
    
    def run(self):
        
        counter = 0
        for step in self.context.build_pipeline_steps(self.tasks):
            print("Pipeline Step: ", counter+1)
            for task_name in step.split(','):
                print('Running ', task_name)                
                task = Task.create(self.context, self.tasks[task_name])
                task.run()
            
            counter += 1
            
        print("Pipeline completed its run successfully")


class PipelineContext:
    
    def __init__(self, config):
        self.config = config
        self.datasources = None
        self.inputs = {}
        self.outputs = {}
        
        worker_methods_map = {'transform': ['transform']}

    def build_pipeline_steps(self, tasks):
        
        data = {}
        
        for task_name in tasks:
            data[task_name] = set(tasks[task_name]['depends_on'])

        while True:
            ordered = set(item for item, dep in data.items() if not dep)
            if not ordered:
                break
            dataitem = ','.join(sorted(ordered))
            yield dataitem
            data = {item: (dep - ordered) for item,dep in data.items()
                if item not in ordered}   
            
    
    def load_data(self, source):

        print('Loading from input data sources: ', source)
        
        if source in self.inputs:
            return self.inputs[source]
                
        else:
            datasource_config = self.datasources[source]
            self.inputs[source] = DataSource.create(datasource_config).load()
                
            return self.inputs[source]
                     
    def load_worker(self, worker_class, worker_type, config=None):
        package, class_name = worker_class.rsplit(".",1)
        worker_instance = load_class_by_name(class_name, package, config)
            
        if '-' in worker_type:
            worker_type = worker_type.split('-')[1]
            
        if has_callable_function(worker_instance, worker_type):
            return worker_instance
        else:
            raise 'Incorrect worker configuration!' 
            
    def save_output(self, name, data, sources=None):
        print('Saving to output data source: ', name)
        
        self.inputs[name] = data
        
        if not sources:
            return
        
        for source in sources:
            datasource_config = self.datasources[source]            
            DataSource.create(datasource_config).save(data[source])
    
