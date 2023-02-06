import json
import re
import traceback
import textwrap
from enum import Enum
from types import MappingProxyType
from typing import Mapping


# Handles loading of tasks
def dynamically_read(raw_tasks, task_kwargs=dict()):
    regex_pattern = re.compile(r'{{(.*?)}}')
    
    # Hook to replace in-string {{variables}} with kwarg values
    def regex_hook(match):            
        if not match.group(1) in task_kwargs:
            raise ValueError(
                f'"{match.group(1)}" was dynamically entered '
                f'but no value was specified for this key.')
        else:
            return task_kwargs[match.group(1)]
    
    # Hook to catch in-string {{variables}}
    def json_hook(_dict):
        for key, val in _dict.items():
            _dict[key] = json.loads(
                regex_pattern.sub(regex_hook, json.dumps(val)))
            
        return _dict

    # Returns the filled tasks
    with raw_tasks:
        return json.loads(
            raw_tasks.read(),
            object_hook=json_hook
        )
    
    
def catch_errors(func):
    def wrapper(*args, **kwargs):
        try:
            response = func(*args, **kwargs)
            return (response, None)
        except Exception as e:
            error = dict(
                error = e,
                trace = traceback.format_exc()
            )
            return (None, error)
    return wrapper


def get_task_obj(handle, task):
    if task.get('request'):
        task_obj = SingleRequestTask(handle, **task)
    elif task.get('requests'):
        task_obj = MultiRequestTask(handle, **task)
    else:
        raise ValueError("Either 'request' or 'requests' must be supplied.")

    return task_obj


class StatusError(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message

    
class PipeStatus(Enum):
    IDLE = 'pending'
    READY = 'ready'
    INCOMPLETE = 'incomplete'
    COMPLETE = 'complete'
    FAIL = 'fail'
    
    def __repr__(self):
        return self.value

    
class TaskStatus(Enum):
    PENDING = 'pending'
    INCOMPLETE = 'incomplete'
    COMPLETE = 'complete'
    FAIL = 'fail'
    
    def __repr__(self):
        return self.value

    
class HandleStatus(Enum):
    UNINITIALIZED = 'uninitialized'
    INITIALIZED = 'initialized'
    FAIL = 'fail' 
    
    def __repr__(self):
        return self.value
    
    
class Handle:
    def __init__(self, handle):
        self.status = HandleStatus.UNINITIALIZED
        self.handle = handle
        
    def __initialize(self, credentials):
        try:
            self.handle = self.handle(credentials)
            self.status = HandleStatus.INITIALIZED
        except Exception as e:
            self.status = HandleStatus.FAIL
            print(f"Handle creation failed with error: {e}")
            
        return self.status
    
    
    @catch_errors
    def run(self, request):
        response = None
        
        if self.status == HandleStatus.INITIALIZED:
            response = self.handle.get_data(request)
        else:
            raise StatusError(
                f'Can not run while Handle is {self.status}')
            
        return response
    
    
class Wrangler:
    def __init__(self, wrangler):
        self.status = 'initialized'
        self.wrangler = wrangler
        # TODO: Complete with similar logic to Handle
        
        
class Task:
    def __init__(self, handle, name, wrangler=None, loader=None, description=None, 
                 **kwargs): 
        self._status: TaskStatus = TaskStatus.PENDING
        self.name: str = name
        self.description: str = description
        
        self.handle: object = handle
        self.wrangler: object = wrangler
        self.loader: object = None
        
        self.handle_result = None
        self.wrangle_result = None
        self.load_result = None
        self.error: dict = None
        
        self.info: Mapping = MappingProxyType(dict(
            name=name,
            description=description,
            handle=handle,
            wrangler=wrangler,
            loader=loader,
            **kwargs
        ))
        
        self.steps = {'request': TaskStatus.PENDING}
        if wrangler:
            self.steps.update({'wrangle': TaskStatus.PENDING})
        if loader:
            self.steps.update({'load': TaskStatus.PENDING})
        
    @property
    def status(self) -> TaskStatus:
        return self._status
    
    def __repr__(self):
        return f"Task(name='{self.name} status={self._status.value},')"

    def update_status(self):
        if len(set(self.steps.values())) == 1:
            self._status = set(self.steps.values())[0]
        else:
            self._status = TaskStatus.INCOMPLETE
    
    def run(self):
        results = dict()
        for step in self.steps:
            if step == 'request':
                result = self.run_request(skip_status_update=True)
            elif step == 'wrangle':
                result = self.run_wrangle(skip_status_update=True)
            elif step == 'load':
                result = self.run_load(skip_status_update=True)
                
            results.update({step: result})
                
        self.update_status()              
        return results  

    
class SingleRequestTask(Task):
    def __init__(self, handle: object, request: dict, name: str, 
                 **kwargs):        
        super().__init__(handle, name, **kwargs)
        self.request: dict = request

    def run_step(step_name, runner, skip_status_update=False):
        self.steps[step_name] = TaskStatus.INCOMPLETE

        result, self.error = runner.run(self)
        result = TaskStatus.FAIL if self.error else TaskStatus.COMPLETE

        self.steps[step_name] = result

        if not skip_status_update:
            self.update_status()

        return result

    def run_request(self, skip_status_update=False):
        return run_step('request', self.handle, skip_status_update)

    def run_wrangle(self, skip_status_update=False):
        return run_step('wrangle', self.wrangler, skip_status_update)

    def run_load(self, skip_status_update=False):
        return run_step('load', self.loader, skip_status_update)

    
class MultiRequestTask(Task):
    def __init__(self, handle: object, requests: dict, 
                 **kwargrs):        
        super().__init__(handle, name, **kwargrs)
        self.requests: dict = requests
        
    def run_request(self):
        pass
        # TODO: Implement proper multitask logic from v1
        
        

def task_by_name_or_index(func):
    def wrapper(self, task):
        if isinstance(task, str):
            task = next((t for t in self.task_objs if t.name == task), None)
            if task is None:
                raise ValueError(f'Task with name "{task}" not found.')
                
        elif isinstance(task, int):
            try:
                task = self.task_objs[task]
            except IndexError:
                raise ValueError(f'Task with index "{task}" not found.')
                
        elif isinstance(task, Task):
            pass
        
        else:
            raise ValueError(f'<task> must be a name, index, or Task object')
            
        func(self, task)
    return wrapper


class Pipe:
    def __init__(self, handle, tasks=None, tasks_file_path=None, tasks_kwargs=None, 
                 wrangler=None, loader=None, credential_manager=None, secret_id=None, 
                 name=None):   
        self._status: PipeStatus = PipeStatus.IDLE    
        self.name = name
        self.secret_id = secret_id
        self.handle = Handle(handle)
        self.wrangler = wrangler
        self.loader = loader
        
        tasks = tasks if tasks is not None else self.read_tasks(tasks_file_path, tasks_kwargs)
        task_objs = [get_task_obj(handle, task_data) for task_data in tasks]
        self.task_map = MappingProxyType({task.name: task for task in task_objs})
        self.pending_tasks = list(self.task_map.values())
        self.complete_tasks = []
        self.failed_tasks = []
        self.status_count = {
            TaskStatus.PENDING: len(self.pending_tasks),
            TaskStatus.INCOMPLETE: 0,
            TaskStatus.COMPLETE: 0
        }

    @property
    def status(self) -> PipeStatus:
        return self._status
    

    def __repr__(self):
        task_strs = [task.__repr__() for task in self.task_map.values()]
        task_str = '\n'.join(task_strs)
        indented_task_str = textwrap.indent(task_str, '  ')
        return f"Pipe(name='{self.name}', status={self._status.value},\n{indented_task_str}\n)"


    
    @staticmethod
    def read_tasks(tasks_file_path=None, task_kwargs=None):
        task_names = set()
        with open(tasks_file_path) as raw_tasks:
            if task_kwargs:
                tasks = dynamically_read(raw_tasks, task_kwargs)
            else:
                tasks = json.load(raw_tasks)
        
        for task in tasks:
            if task['name'] in task_names:
                raise ValueError(f"Task with name '{task['name']}' already exists.")
            task_names.add(task['name'])

        return tasks
    
    
    def update_status(self):
        if self.status_count[TaskStatus.PENDING] == len(self.task_map):
            self.status = self.handle.status
        elif self.status_count[TaskStatus.COMPLETE] == len(self.task_map):
            self.status = PipeStatus.COMPLETE
        else:
            self.status = PipeStatus.INCOMPLETE

        return self.status

    
    def initialize_task(self, task):
        if task in self.complete_tasks:
            self.complete_tasks.remove(task)
            self.status_count[TaskStatus.COMPLETE] -= 1
        elif task in self.failed_tasks:
            self.failed_tasks.remove(task)
            self.status_count[TaskStatus.FAILED] -= 1

        if task not in self.pending_tasks:
            self.pending_tasks.append(task)
            self.status_count[TaskStatus.PENDING] += 1

        status = self.update_status()
        return status
    
    
    def maintain_status_count(func):
        def wrapper(self, task, *args, **kwargs):
            self.initialize_task(task)

            result = func(self, task, *args, **kwargs)

            if result == TaskStatus.COMPLETE:
                self.complete_tasks.append(task)
                self.status_count[TaskStatus.COMPLETE] += 1
            else:
                self.failed_tasks.append(task)
                self.status_count[TaskStatus.FAILED] += 1

            return result

        return wrapper
    
    def initialize_handle(self, credential_obj):
        if self.handle.status is not HandleStatus.INITIALIZED:
            result = self.handle.__initialize(credential_obj)
            
            if result == HandleStatus.INITIALIZED:
                self._status = PipeStatus.READY
            else:
                self._status = PipeStatus.FAIL
            
        return self.handle.status
    
    @maintain_status_count
    def run_task(self, task, wrangle=True, load=True):
        result = task.run()
        return result

    def run_tasks(self, tasks):
        for task in tasks:
            self.run_task(task)

    def run_pipe(self):
        self.run_tasks(self.task_map.values())
        
        
def check_pipe_not_failed(func):
    def wrapper(self, pipe_name, *args, **kwargs):
        status = self.pipes[pipe_name].status
        
        if status is PipeStatus.IDLE:
            status = self.initialize_pipe(pipe_name) 
        if status is PipeStatus.FAIL:
            raise AttributeError(f'{pipe_name} is failed and cannot be ran.')
        return func(self, pipe_name, *args, **kwargs)
    
    return wrapper
        

class Pipeline(Pipe):
    def __init__(self, pipes: [dict,], pipeline_id=None):
        #self.loader = loader
        self.id = pipeline_id
        self.credential_manager = credential_manager
        self.pipes = MappingProxyType({pipe.name:pipe for pipe in pipes})

    def __repr__(self):
        pipe_strs = [pipe.__repr__() for pipe in self.pipes.values()]
        pipe_str = '\n'.join(pipe_strs)
        indented_pipe_str = textwrap.indent(pipe_str, '  ')
        return f"Pipeline(id='{self.id}',\n{indented_pipe_str}\n)"
    
    def initialize_pipe(self, pipe_name):
        pipe = self.pipes[pipe_name]    
        credential_obj = credential_manager(pipe.secret_id)
        handle_status = pipe.initialize_handle(credential_obj)
            
        return handle_status
        
        
    def initialize_pipes(self, pipe_names):
        results = dict()
        for pipe_name in pipe_names:
            results.update({pipe_name:self.initialize_pipe(pipe_name)})
            
        return results
            
    def initialize_pipeline(self):
        results = self.initialize_pipes(self.pipes.keys()) 
        return results
    
    @check_pipe_not_failed
    def run_task(self, pipe_name, task_name):
        self.pipes[pipe_name].run_task(task_name)     
        
    @check_pipe_not_failed
    def run_pipe(self, pipe_name):
        result = self.pipes[pipe_name].run_pipe()