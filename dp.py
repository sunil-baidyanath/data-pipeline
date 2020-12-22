'''
Created on Dec 10, 2020

@author: sunil.thakur
'''
#!/usr/bin/python

import argparse
from pipelines.core.runtime import AWSRuntime

class GroupedAction(argparse.Action):    
        
    def __call__(self, parser, namespace, values, option_string=None):
        group,dest = self.dest.split('.',2)
        groupspace = getattr(namespace, group, argparse.Namespace())

        if dest == 'build':
            setattr(groupspace, dest, True)
            setattr(namespace, group, groupspace)
        else:
            setattr(groupspace, dest, values)
            setattr(namespace, group, groupspace)

def parse_args():    
    parser = argparse.ArgumentParser(description="CLI utility for Data Pipeline")
        
    pipeline_action_parsers = parser.add_subparsers(dest="command", required=True, help="Pipeline commands")  
    
    deploy_pipeline_parser = pipeline_action_parsers.add_parser("deploy", help="Deploy a specified pipeline");
    run_pipeline_parser = pipeline_action_parsers.add_parser("start", help="Run a specified pipeline");
    status_pipeline_parser = pipeline_action_parsers.add_parser("status", help="Get status of the specified pipeline run");
    
    deploy_pipeline_parser.add_argument("--name", required = True, dest="params.name", action = GroupedAction, default=argparse.SUPPRESS, help="Name of the pipeline")
    deploy_pipeline_parser.add_argument('--build', dest = "params.build", action=GroupedAction, default = argparse.SUPPRESS, nargs='?', help="Build project")                        
    run_pipeline_parser.add_argument("--name", required = True, dest="params.name", action = GroupedAction, default=argparse.SUPPRESS, help="Name of the pipeline")
    status_pipeline_parser.add_argument("--id", required = True, dest="params.id", action = GroupedAction, default=argparse.SUPPRESS, help="Id of the pipeline run")
    
    args = parser.parse_args()
    
    if not hasattr(args.params, 'build'):
        setattr(args.params, 'build', False)
       
    return args
        

def process_pipeline_action(action, params):    
    runtime = AWSRuntime({})    
    
    if 'start' == action:
        output = runtime.start(params['name'])
        print(output)
        
    elif 'status' == action:
        output = runtime.status(params['id'])
        print(output)
        
    elif 'deploy' == action:
        runtime.deploy(params['name'], params['build'])
        
    else:
        raise Exception('Specified action not supported!')
    
            
def main():
    args = vars(parse_args())    
    print(args) 
    args['params'] = vars(args['params']) 
    process_pipeline_action(args['command'], args['params'])  
     
    
if __name__ == '__main__':
    main()