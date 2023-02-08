from enum import Enum
import traceback
from functools import wraps
from types import MappingProxyType
from typing import Mapping
import textwrap
import json
import re


class StatusError(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message

    
class OpStatus(Enum):
    UNINITIALIZED = 'uninitialized'
    INITIALIZED = 'initialized'
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

    
class PipeStatus(Enum):
    IDLE = 'idle'
    READY = 'ready'
    INCOMPLETE = 'incomplete'
    COMPLETE = 'complete'
    FAIL = 'fail'
    
    def __repr__(self):
        return self.value
    
    
class OperatorUtils:
    def __init__(self, catch_runtime_errors=True):
        self.catch_runtime_errors = catch_runtime_errors
    
    @staticmethod
    def _catch_runtime_errors(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.catch_runtime_errors:
                return func(self, *args, **kwargs), None
            else:
                try:
                    response = func(self, *args, **kwargs)
                    return (response, None)
                except Exception as e:
                    error = dict(
                        type=e.__class__.__name__,
                        message=str(e),
                        trace=traceback.format_exc()
                    )
                return (None, error)     
        return wrapper

    
class Handle(OperatorUtils):
    def __init__(self, api_client, catch_runtime_errors=True, *args, **kwargs):
        super().__init__(catch_runtime_errors=catch_runtime_errors)
        self._status = OpStatus.UNINITIALIZED
        self.api_client = api_client
        self.initialize_error = None
    
    def initialize(self, credentials):
        try:
            self.api_client = self.api_client(credentials)
            self._status = OpStatus.INITIALIZED
        except Exception as e:
            self._status = OpStatus.FAIL
            self.initialize_error = dict(
                type=e.__class__.__name__,
                message=str(e),
                trace=traceback.format_exc()
            )
            raise e from None
            
        return self._status
    
    @property
    def status(self) -> OpStatus:
        return self._status
    
    @OperatorUtils._catch_runtime_errors
    def run(self, request, handle_operations={}):
        if self._status == OpStatus.INITIALIZED:
            response, error = self.api_client.get_data(request, **handle_operations)
        else:
            raise StatusError(
                f'Can not run while Handle is {self._status}')
            
        return response, error
    

class Wrangler(OperatorUtils):
    def __init__(self, wrangler, *args, **kwargs):
        super().__init__(catch_runtime_errors=True)
        self._status = OpStatus.UNINITIALIZED
        self.wrangler = wrangler
        self.initialize_error = None
    
    def initialize(self):
        try:
            self.wrangler = self.wrangler()
            self._status = OpStatus.INITIALIZED
        except Exception as e:
            self._status = OpStatus.FAIL
            self.initialize_error = dict(
                type=e.__class__.__name__,
                message=str(e),
                trace=traceback.format_exc()
            )
            raise e from None
            
        return self._status
    
    @property
    def status(self) -> OpStatus:
        return self._status
    
    @OperatorUtils._catch_runtime_errors
    def run(self, data, wrangle_operations={}):
        if self._status == OpStatus.INITIALIZED:
            response = self.wrangler.wrangle(data, **wrangle_operations)
        else:
            raise StatusError(
                f'Can not run while Wrangler is {self._status}')
            
        return response
    
    
class Loader(OperatorUtils):
    def __init__(self, loader, *args, **kwargs):
        super().__init__(catch_runtime_errors=True)
        self._status = OpStatus.UNINITIALIZED
        self.loader = loader
        self.initialize_error = None
    
    def initialize(self):
        try:
            self.wrangler = self.loader()
            self._status = OpStatus.INITIALIZED
        except Exception as e:
            self._status = OpStatus.FAIL
            self.initialize_error = dict(
                type=e.__class__.__name__,
                message=str(e),
                trace=traceback.format_exc()
            )
            raise e from None
            
        return self._status
    
    @property
    def status(self) -> OpStatus:
        return self._status
    
    @OperatorUtils._catch_runtime_errors
    def run(self, data, load_operations={}):
        if self._status == OpStatus.INITIALIZED:
            response = self.loader.load(data, **load_operations)
        else:
            raise StatusError(
                f'Can not run while Wrangler is {self._status}')
            
        return response
    
    
class TaskOpsUtils:
    def __init__(self, handle, wrangler=None, loader=None):
        self.handle = handle
        self.wrangler = wrangler
        self.loader = loader

        self.handle_result = None
        self.wrangle_result = None
        self.load_result = None
        self.error = None
        
        self.operations = {'handle': TaskStatus.PENDING}
        if wrangler:
            self.operations.update({'wrangle': TaskStatus.PENDING})
        if loader:
            self.operations.update({'load': TaskStatus.PENDING})
    
    def _run_operation(self, op, runner, skip_status_update=False):
        self.operations[op] = TaskStatus.INCOMPLETE

        response, self.error = runner.run(self)
        status = TaskStatus.FAIL if self.error is not None else TaskStatus.COMPLETE
        
        print(response, status, self.error)

        self.operations[op] = status
        self.data[op] = response

        if not skip_status_update:
            self.update_status()

        return status

    
class Task(TaskOpsUtils):
    def __init__(self, name, req_dict, handle, wrangler=None, loader=None, description=None, **kwargs):
        super().__init__(handle, wrangler, loader)

        self._status = TaskStatus.PENDING
        self.name = name
        self.description = description
        self.data = dict(
            handle=None,
            wrangle=None,
            load=None
        )
        self.info = MappingProxyType({
            'name': name,
            'description': description,
            'type': self.__class__.__name__,
            'req': req_dict,
            'handle': handle,
            'wrangler': wrangler,
            'loader': loader,
            **kwargs
        })

    def __repr__(self):
        return f"Task(name='{self.name}', status='{self.status}')"

    @property
    def status(self) -> TaskStatus:
        return self._status.value
    
    def run(self):
        statuses = {}
        for op in self.operations:
            status = None
            if op == 'handle':
                status = self.run_request(skip_status_update=True)
            elif op == 'wrangle':
                status = self.run_wrangle(skip_status_update=True)
            elif op == 'load':
                status = self.run_load(skip_status_update=True)

            statuses[op] = status

        self.update_status()
        return statuses

    def update_status(self):
        if len(set(self.operations.values())) == 1:
            self._status = set(self.operations.values()).pop()
        else:
            self._status = TaskStatus.INCOMPLETE

            
class SingleRequestTask(Task):
    def __init__(self, name, request, handle, wrangler=None, loader=None, **kwargs):
        super().__init__(name, request, handle, wrangler, loader, **kwargs)
        self.request = request

    def run_request(self, skip_status_update=False):
        status = self._run_operation('handle', self.handle, skip_status_update)
        return status

    def run_wrangle(self, skip_status_update=False):
        status = self._run_operation('wrangle', self.wrangler, skip_status_update)
        return status

    def run_load(self, skip_status_update=False):
        status = slef._run_operation('load', self.loader, skip_status_update)
        return status

    
class MultiRequestTask(Task):
    def __init__(self, name, request, handle, wrangler=None, loader=None, **kwargs):      
        super().__init__(name, requests, handle, wrangler, loader, **kwargrs)
        self.requests: dict = requests
        
    def run_request(self):
        pass
        # TODO: Implement proper multitask logic from v1
        
        
class TaskManagementUtils:   
    def __init__(self, tasks):
        self.operator_statuses = dict(
            handle = None,
            wrangler = None,
            loader = None)
        task_objs = [
            self.task_obj_from_dict(
                task_data, self.handle, self.wrangler, self.loader
            ) for task_data in tasks]
        
        self.task_map = {task.name: task for task in task_objs}

    # Handles dynamic reading of in-string variables
    @staticmethod
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
        
    @staticmethod
    def read_tasks(tasks_file_path=None, task_kwargs=None, dynamic_reader=None):
        print(tasks_file_path)
        with open(tasks_file_path) as raw_tasks:
            if task_kwargs:
                tasks = dynamic_reader(raw_tasks, task_kwargs)
            else:
                tasks = json.load(raw_tasks)
        task_names = set(task['name'] for task in tasks)
        if len(task_names) != len(tasks):
            duplicates = task_names - set(task['name'] for task in tasks)
            raise ValueError(f"Duplicate tasks with names: {duplicates}")

        return tasks
    
    @staticmethod
    def task_obj_from_dict(task, handle, wrangler=None, loader=None):
        name = task.pop('name')
        if task.get('request'):
            request = task.pop('request')
            task_obj = SingleRequestTask(name, request, handle, wrangler, loader, **task)
        elif task.get('requests'):
            request = task.pop('requests')
            task_obj = MultiRequestTask(name, request, handle, wrangler, loader, **task)
        else:
            raise ValueError("Either 'request' or 'requests' must be supplied.")

        return task_obj

    def add_task(self, task_name, task_obj=None, task_dict=None):
        if task_dict:
            task_obj = task_obj_from_dict(task_dict, self.handle, self.wrangler, self.loader)
            
        self.task_map.update(task_name=task_obj)
            
    def get_task_status(self, task_name):
        task = self.tasks.get(task_name)
        if task is None:
            raise ValueError(f"Task {task_name} not found")
            
        return task.status
    
    def get_task_statuses(self):
        statuses = dict()
        for task_name, task in self.task_map.items():
            statuses.update(task_name=task.status)
            
        return statuses
    
    def get_task_status_counts(self):
        statuses = dict(
            pending = 0,
            incomplete = 0,
            complete = 0,
            failed = 0,
        )
        
        for task_name, task in self.task_map.items():
            statuses[task.status] += 1
            
        return statuses
        
    def run_task(self, task_name):
        task = self.task_map.get(task_name)
        if task is None:
            raise ValueError(f"Task {task_name} not found")
        
        status = task.run()
        return status
    
    def get_task_data(self, task_name, operator=None):
        task = self.task_map.get(task_name)
        if task is None:
            raise ValueError(f"Task {task_name} not found")
        
        if operator:
            data = task.data[operator]
        else:
            data = task.data
            
        return data

    def get_all_tasks_data(self, operator=None):
        data = dict()
        
        if operator:
            for task_name, task in self.task_map.items():
                data.update(task_name=task.data[operator])
        else:
            for task_name, task in self.task_map.items():
                data.update(task_name=task.data)
            
        return data

    
class Pipe(TaskManagementUtils):
    def __init__(self, handle, tasks=None, tasks_file_path=None, tasks_kwargs=None, 
                 wrangler=None, loader=None, credential_manager=None, secret_id=None, 
                 name=None):
        self._status: PipeStatus = PipeStatus.IDLE
        self.name = name
        self.secret_id = secret_id
        self.handle = Handle(handle)
        self.wrangler = Wrangler(wrangler) if wrangler else None
        self.loader = Loader(loader) if loader else None
        tasks = tasks if tasks is not None else self.read_tasks(
            tasks_file_path, tasks_kwargs, dynamic_reader=self.dynamically_read)
        super().__init__(tasks)
        
        self._pipe_failed = False  
        
    @property
    def status(self) -> PipeStatus:
        return self._status   

    def __repr__(self):
        task_strs = [task.__repr__() for task in self.task_map.values()]
        task_str = '\n'.join(task_strs)
        indented_task_str = textwrap.indent(task_str, '  ')
        return (
            f"Pipe(name='{self.name}', status='{self._status.value}',\n"
                    f"{indented_task_str}\n)")
    
    def update_status(self):
        if self.status_count[TaskStatus.PENDING] == len(self.task_map):
            self.status = self.handle.status
        elif self.status_count[TaskStatus.COMPLETE] == len(self.task_map):
            self.status = PipeStatus.COMPLETE
        else:
            self.status = PipeStatus.INCOMPLETE

        return self.status
    
    def initialize_handle(self, credentials_obj):
        status = self.handle.initialize(credentials_obj)
        self.operator_statuses.update(handle=status)
        if status == OpStatus.FAIL:
            self.status = PipeStatus.FAIL
            
        return status
    
    def initialize_wrangler(self):
        if self.wrangler:
            status = self.wrangler.initialize()
            self.operator_statuses.update(wrangler=status)

            return status
    
    def initialize_loader(self):
        if self.loader:
            status = self.loader.initialize()
            self.operator_statuses.update(wrangler=status)

            return status
        
    def initialize(self, credentials_obj):
        self.initialize_handle(credentials_obj)
        self.initialize_wrangler()
        self.initialize_loader()
        
        return self.operator_statuses
        
    def run(self):
        statuses = dict()
        for task_name, task in self.task_map.items():
            new_status = task.run()
            statuses.update({task_name:new_status})
        
        return statuses
        
        
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
        return (
            f"Pipeline(id='{self.id}',\n"
                    f"{indented_pipe_str}\n)")
    
    @staticmethod
    def _check_pipe_not_failed(pipe):
        if pipe._pipe_failed:
            raise Exception("Pipeline failed earlier, cannot run further operations")
    
    def initialize_pipe(self, pipe_name):
        pipe = self.pipes[pipe_name]    
        credential_obj = self.credential_manager(pipe.secret_id)
        status = pipe.initialize(credential_obj)
            
        return status  
        
    def initialize_pipes(self, pipe_names):
        results = dict()
        for pipe_name in pipe_names:
            results.update({pipe_name:self.initialize_pipe(pipe_name)})
            
        return results
            
    def initialize(self):
        results = self.initialize_pipes(self.pipes.keys()) 
        return results
    
    def run_task(self, pipe_name, task_name):
        self._check_pipe_not_failed(pipe)
        self.pipes[pipe_name].run_task(task_name)     
        
    def run_pipe(self, pipe_name):
        pipe = self.pipes[pipe_name]
        self._check_pipe_not_failed(pipe)
        result = pipe.run()
        
        return result
    
    def get_pipe_data(self, pipe_name, operator=None):
        pipe = self.pipes[pipe_name]
        data = pipe.get_all_tasks_data(operator=operator)
