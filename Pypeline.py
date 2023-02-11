from enum import Enum
import traceback
from functools import wraps
from types import MappingProxyType
from typing import Mapping
from pandas import json_normalize
import textwrap
import json
import re


"""
Title: Pypeline
Current Version: 0.0.05
Created By: Chad Wood
Last Modified On: 20230210
Last Modification: Cleaned up TaskOpsUtils._run_opperation() a bit, though it could probably improve further by a name change.
Added a generator object as an attribute to Pipeline (Pipeline.tasks) which will provide a generator of nested dicts {pipe_name: {task_name: task}}
    I plan to use this feature in a future update for multithreading.
Added __getattribute__ and __getitem__ to Pipe and Pipeline to allow dot-notation navigation between objects.

Note: Next should be restricting setattribute, and allowing setitem to adding pipes/tasks to a pipeline/pipe, respectively.
There should also be some functionality to scope a wrangler/loader to a task, pipe, or entire pipeline--while still letting set wrangler/loaders at more granular levels override the parent counterpart.

Note: I also created a file pipelineutils which contains a credential manager (for PyKeePass) and a Pipeline-Scopable wrangler that maps input [dict,] commands to Pandas operations. This would idealy be used as a convenient way to store Pandas operations as JSON, along with the JSON representation of the Task it applies to. The mapping functionality still requires some debugging, but in theory it will allow regular methods, loc, operators (+, -, =, ...), and the ability to store named _variables for later use. The mapping verifies that passed arguments are attributes of a DataFrame, and uses `getattr()` for security reasons.
"""

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
    """
    Class containing utility functions for pipeline Operators.

    :param catch_runtime_errors:
    A flag indicating whether to catch and handle runtime errors.
    Default is True.

    :attribute:
    - catch_runtime_errors: A flag indicating whether to catch and handle runtime errors.

    :method:
    - _catch_runtime_errors(func):
    A staticmethod that acts as a decorator to catch and handle runtime errors
    in pipeline Operators. The decorated function will return a tuple of (response, error),
    where response is the function's output and error is None if no error occurs,
    otherwise it is a dictionary containing the error information.
    """
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
    """
    Class to handle the processing of data in the pipeline.
    :param api_client: A callable for handling data.
    :param args: Other positional arguments.
    :param kwargs: Other keyword arguments.

    :attribute:
    - initialize_error: None if no error occurred during initialization, otherwise contains the error information.
    - _status: The status of the Handle. Default is `OpStatus.UNINITIALIZED`.
    - api_client: The callable for handling data.

    :method:
    - initialize(credentials): Initializes the user supplied api_client.
    - status: A property that returns the current status of the Handle.
    - run(request, handle_operations={}): Handles the data using the provided `api_client` callable, with the provided `handle_operations`.
    """

    def __init__(self, api_handle, **kwargs):
        """
        Initialize the Handle class.

        :param api_client: A callable for requesting data.
        :param kwargs: Flags passed to OperatorUtils
        """
        super().__init__(**kwargs)
        self._status = OpStatus.UNINITIALIZED
        self.client = api_handle
        self.initialize_error = None
    
    def initialize(self, credentials):
        """
        Initialize the api_client object.
        
        :return: OpStatus (INITIALIZED or FAIL)
        :raises: Exception if error occurs during initialization
        """
        try:
            self.client = self.client(credentials)
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
    def run(self, request, handle_operations=[]):
        if self._status == OpStatus.INITIALIZED:
            response = self.client.get_data(request, *handle_operations)
        else:
            raise StatusError(
                f'Can not run while Handle is {self._status}')
            
        return response
    
    
class Wrangler(OperatorUtils):
    """
    Class to handle wrangling of data in the pipeline.

    :param wrangler: A callable for wrangling data.
    :param args: Other positional arguments.
    :param kwargs: Other keyword arguments.

    :attribute:
    - initialize_error: None if no error occurred during initialization, otherwise contains the error information.
    - _status: The status of the Wrangler. Default is `OpStatus.UNINITIALIZED`.
    - wrangler: The callable for wrangling data.

    :method:
    - initialize(): Initializes the user supplied wrangler.
    - status: A property that returns the current status of the Wrangler.
    - run(data, wrangle_operations=[]): Wrangles the data using the provided `wrangler` callable, with the provided `wrangle_operations`.
    """
    def __init__(self, wrangler, **kwargs):
        """
        Initialize the Wrangler class.

        :param wrangler: A callable for wrangling data.
        :param kwargs: Flags passed to OperatorUtils
        """
        super().__init__(**kwargs)
        self._status = OpStatus.UNINITIALIZED
        self.wrangler = wrangler
        self.initialize_error = None
    
    def initialize(self):
        """
        Initialize the wrangler object.
        
        :return: OpStatus (INITIALIZED or FAIL)
        :raises: Exception if error occurs during initialization
        """
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
    def run(self, data, wrangle_operations=[]):
        if self._status == OpStatus.INITIALIZED:
            response = self.wrangler.wrangle(data, *wrangle_operations)
        else:
            raise StatusError(
                f'Can not run while Wrangler is {self._status}')
            
        return response
    
    
class Loader(OperatorUtils):
    """
    Class to handle loading of data in the pipeline.

    :param loader: A callable for loading data.
    :param args: Other positional arguments.
    :param kwargs: Other keyword arguments.
    
    :attribute:
    - initialize_error: None if no error occurred during initialization, otherwise contains the error information.
    - _status: The status of the Loader. Default is `OpStatus.UNINITIALIZED`.
    - loader: The callable for loading data.

    :method:
    - initialize(): Initializes the user supplied loader.
    - status: A property that returns the current status of the Loader.
    - run(data, load_operations={}): Loads the data using the provided `loader` callable, with the provided `load_operations`.
    """
    def __init__(self, loader, **kwargs):
        """
        Initialize the Loader class.

        :param loader: A callable for loading data.
        :param kwargs: Flags passed to OperatorUtils
        """
        super().__init__(**kwargs)
        self._status = OpStatus.UNINITIALIZED
        self.loader = loader
        self.initialize_error = None
    
    def initialize(self):
        """
        Initialize the loader object.
        
        :return: OpStatus (INITIALIZED or FAIL)
        :raises: Exception if error occurs during initialization
        """
        
        try:
            self.loader = self.loader()
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
    def run(self, data, load_operations=[]):
        if self._status == OpStatus.INITIALIZED:
            response = self.loader.load(data, *load_operations)
        else:
            raise StatusError(
                f'Can not run while Loader is {self._status}')
            
        return response
    
    
class TaskOpsUtils:
    """
    A base class for managing the operations of a Task.
    :param handle: A callable for handling the request.
    :param wrangler: A callable for wrangling the data from handle.
    :param loader: A callable for loading the data from wrangler.

    :attribute:
    - handle_result: The result of the handle operation.
    - wrangle_result: The result of the wrangle operation.
    - load_result: The result of the load operation.
    - error: None if no error occurred during the operation, otherwise contains the error information.
    - operations: A dictionary that contains the status of each operation in the task.
    - data: A dictionary that contains the results of each operation in the task.

    :method:
    - get_response(operator): Return the response for the specified operation.
    - _run_operation(op, runner, passable, skip_status_update=False): Run an operation.
    - update_status(): Update the overall status of the task.
    """
    def __init__(self, handle, wrangler, loader, handle_ops, wrangle_ops, load_ops):
        self.handle = handle
        self.wrangler = wrangler
        self.loader = loader

        self.handle_result = None
        self.wrangle_result = None
        self.load_result = None
        self.error = None
        
        self.op_statuses = {'handle': TaskStatus.PENDING}
        if wrangler:
            self.op_statuses.update({'wrangle': TaskStatus.PENDING})
        if loader:
            self.op_statuses.update({'load': TaskStatus.PENDING})
            
        self.special_operations = dict(
            handle_ops = handle_ops,
            wrangle_ops = wrangle_ops,
            load_ops = load_ops
        )
    
    def _run_operation(self, op, runner, passable, spec_ops=[], skip_status_update=False):
        self.op_statuses[op] = TaskStatus.INCOMPLETE
        response, self.error = runner.run(passable, spec_ops)
        status = self.op_statuses[op] = TaskStatus.FAIL if self.error is not None else TaskStatus.COMPLETE           

        if not skip_status_update:
            self.update_status()

        return status, response

    
class Task(TaskOpsUtils):
    """
    A base class for managing a task.

    :param name: The name of the task.
    :param req_dict: The request dictionary for the task.
    :param handle: A callable for handling the request.
    :param wrangler: A callable for wrangling the data from handle.
    :param loader: A callable for loading the data from wrangler.
    :param description: The description of the task.
    :param kwargs: Other keyword arguments.

    :attribute:
    - _status: The status of the task. Default is TaskStatus.PENDING.
    - name: The name of the task.
    - description: The description of the task.
    - data: The response data from task operators.
    - info: A mapping proxy type containing information about the task.

    :method:
    - run(): Run the task.
    - update_status(): Updates the status of the task.
    """
    def __init__(self, name, req_dict, handle, wrangler=None, loader=None, description=None, **kwargs):
        """
        Initialize the Task class.

        :param name: The name of the task.
        :param req_dict: The request dictionary for the task.
        :param handle: A callable for handling the request.
        :param wrangler: A callable for wrangling the data from handle.
        :param loader: A callable for loading the data from wrangler.
        :param description: The description of the task.
        :param kwargs: Other keyword arguments.
        """
        super().__init__(
            handle=handle, 
            wrangler=wrangler, 
            loader=loader, 
            handle_ops=kwargs.get('handle_ops', []), 
            wrangle_ops=kwargs.get('wrangle_ops', []), 
            load_ops=kwargs.get('load_ops', [])
        )

        self._status = TaskStatus.PENDING
        self.name = name
        self.description = description
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
        return f"Task('{self.name}', status='{self.status}'),"

    @property
    def status(self) -> TaskStatus:
        return self._status.value
    
    def run(self):
        statuses = {}
        for op in self.op_statuses:
            status = None
            if op == 'handle':
                spec_ops = self.special_operations.get('handle_ops')
                status = self.run_request(spec_ops=spec_ops, skip_status_update=True)
            elif op == 'wrangle' and self.op_statuses['handle'] == TaskStatus.COMPLETE:
                spec_ops = self.special_operations.get('wrangle_ops')
                status = self.run_wrangle(spec_ops=spec_ops, skip_status_update=True)
            elif op == 'load' and self.op_statuses['wrangle'] == TaskStatus.COMPLETE:
                spec_ops = self.special_operations.get('load_ops')
                status = self.run_load(spec_ops=spec_ops, skip_status_update=True)

            statuses[op] = status

        self.update_status()
        return statuses

    def update_status(self):
        if len(set(self.op_statuses.values())) == 1:
            self._status = set(self.op_statuses.values()).pop()
        else:
            self._status = TaskStatus.INCOMPLETE

class SingleRequestTask(Task):
    """
    A class for managing a single request task. Inherits from the base Task class.

    :param name: The name of the task.
    :param request: The request for the task.
    :param handle: A callable for handling the request.
    :param wrangler: A callable for wrangling the data from handle.
    :param loader: A callable for loading the data from wrangler.
    :param kwargs: Other keyword arguments.

    :attribute:
    - request: The request for the task.
    
    :method:
    - run_request(): Runs the request for the task.
    - run_wrangle(): Runs the wrangler for the task.
    - run_load(): Runs the loader for the task.
    """
    def __init__(self, name, request, handle, wrangler=None, loader=None, **kwargs):
        super().__init__(name, request, handle, wrangler, loader, **kwargs)
        self.request = request
        self.data = dict(
            handle=None,
            wrangle=None,
            load=None
        )

    def run_request(self, spec_ops, skip_status_update=False):
        status, response = self._run_operation('handle', self.handle, self.request, spec_ops, skip_status_update)
        self.data['handle'] = response
        return status

    def run_wrangle(self, spec_ops, skip_status_update=False):
        status, response = self._run_operation('wrangle', self.wrangler, self, spec_ops, skip_status_update)
        self.data['wrangle'] = response
        return status

    def run_load(self, spec_ops, skip_status_update=False):
        status, response = self._run_operation('load', self.loader, self, spec_ops, skip_status_update)
        self.data['load'] = response
        return status

    
class MultiRequestTask(Task):
    """
    A class for managing multiple requests. Inherits from the base Task class.

    :param name: The name of the task.
    :param request: The requests for the task.
    :param handle: A callable for handling the request.
    :param wrangler: A callable for wrangling the data from handle.
    :param loader: A callable for loading the data from wrangler.
    :param kwargs: Other keyword arguments.

    :attribute:
    - requests: The requests for the task.

    :method:
    - run_request(): Runs the requests for the task.
    """
    def __init__(self, name, requests, handle, wrangler=None, loader=None, **kwargs):      
        super().__init__(name, requests, handle, wrangler, loader, **kwargs)
        self.requests: dict = requests
        self.pattern = re.compile(r'<<(.*?)>>')
        self.cached_unique_values = {}
        self.data = dict(
            handle=[],
            wrangle=None,
            load=None
        )
        
    def run_request(self, spec_ops, skip_status_update=False):
        statuses = dict()
        pattern = self.pattern
        
        # Use the stored unique values if possible
        def hook(match, separator):
            in_string_var = match.group(1)
            if in_string_var in self.cached_unique_values:
                unique_values = self.cached_unique_values[in_string_var]
            else:
                unique_values = last_response[in_string_var].unique().tolist()
                self.cached_unique_values[in_string_var] = unique_values
            
            return separator.join(map(str, unique_values))
        
        last_response = None
        for _iter, request in self.requests.items():
            if last_response is not None:
                separator = request.get("%separator%", ", ")
                request = json.loads(pattern.sub(lambda x: hook(x, separator), json.dumps(request)))
            
            status, response = self._run_operation('handle', self.handle, request, spec_ops, skip_status_update)
            self.data['handle'].append(response)
            
            statuses.update({_iter:status})
            if status is not TaskStatus.COMPLETE:
                break
            else:
                last_response = json_normalize(response)
        
        return statuses
                                  
    def run_wrangle(self, spec_ops, skip_status_update=False):
        status, response = self._run_operation('wrangle', self.wrangler, self, spec_ops, skip_status_update)
        self.data['wrangle'] = response
        return status

    def run_load(self, spec_ops, skip_status_update=False):
        status, response = self._run_operation('load', self.loader, self, spec_ops, skip_status_update)
        self.data['load'] = response
        return status
        
        
class TaskManagementUtils:
    """
    A class for managing multiple tasks and handling reading of tasks.
    
    :param tasks: A list of tasks as dictionaries, with task information.

    :attribute:
    - operator_statuses: A dictionary to store the statuses of the handle, wrangler and loader callables.
    - task_map: A dictionary mapping task names to task objects.

    :method:
    - dynamically_read(raw_tasks, task_kwargs=dict()): Dynamically reads the task information, replacing in-string {{variables}} with kwarg values.
    - read_tasks(tasks_file_path=None, task_kwargs=None, dynamic_reader=None): Reads task information from file or returns existing tasks.
    - task_obj_from_dict(task, handle, wrangler=None, loader=None): Creates a task object from a task dictionary.
    - add_task(self, task_name, task_obj=None, task_dict=None): Adds a task to the task_map.
    - get_task_status(self, task_name): Gets the status of a task.
    - get_task_statuses(self): Gets the statuses of all tasks.
    - get_task_status_counts(self): Gets the count of tasks in each status.
    - run_task(self, task_name): Runs a task by the given name, raises ValueError if task not found.
    - get_task_data(self, task_name, operator=None): Gets the data of a task by the given name, returns all data if operator is None.
    - get_all_tasks_data(self, operator=None): Gets the data of all tasks, returns all data for each task if operator is None.
    - get_task(self, task_name): Gets a task by the given name, raises ValueError if task not found.
    """
    def __init__(self):
        self.operator_statuses = dict(
            handle = None,
            wrangler = None,
            loader = None
        )

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
            requests = task.pop('requests')
            task_obj = MultiRequestTask(name, requests, handle, wrangler, loader, **task)
        else:
            raise ValueError("Either 'request' or 'requests' must be supplied.")

        return task_obj

    def add_task(self, task_name, task_obj=None, task_dict=None):
        if task_dict:
            task_obj = task_obj_from_dict(task_dict, self.handle, self.wrangler, self.loader)
            
        self.task_map.update({task_name:task_obj})
            
    def get_task_status(self, task_name):
        task = self.tasks.get(task_name)
        if task is None:
            raise ValueError(f"Task {task_name} not found")
            
        return task.status
    
    def get_task_statuses(self):
        statuses = dict()
        for task_name, task in self.task_map.items():
            statuses.update({task_name:task.status})
            
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
                data.update({task_name:task.data[operator]})
        else:
            for task_name, task in self.task_map.items():
                data.update({task_name:task.data})
            
        return data   
    
    def get_task(self, task_name):
        task = self.task_map.get(task_name)
        if task is None:
            raise ValueError(f"Task {task_name} not found")
            
        return task

    
class Pipe(TaskManagementUtils):
    """
    A class for managing the execution of tasks within a pipeline.

    :param handle: An object that represents the handle operation.
    :param tasks: A list of tasks as dictionaries, with task information.
    :param tasks_file_path: A file path to a file containing the tasks.
    :param tasks_kwargs: A dictionary of variables to be replaced within the tasks.
    :param wrangler: An object that represents the wrangler operation.
    :param loader: An object that represents the loader operation.
    :param credential_manager: An object that manages the credentials.
    :param secret_id: An id to access a secret.
    :param name: The name of the pipeline.

    :attribute:
    - _status: A pipe status indicating if the pipeline is IDLE, INCOMPLETE, COMPLETE, or FAIL.
    - name: The name of the pipeline.
    - secret_id: An id to access a secret.
    - handle: An object that represents the handle operation.
    - wrangler: An object that represents the wrangler operation.
    - loader: An object that represents the loader operation.

    :property:
    - status: Returns the current status of the pipeline.

    :method:
    - repr(): Returns a string representation of the pipeline.
    - update_status(): Updates the status of the pipeline.
    - initialize_handle(credentials_obj): Initializes the handle with a given credentials object.
    - initialize_wrangler(): Initializes the wrangler, if it exists.
    - initialize_loader(): Initializes the loader, if it exists.
    - initialize(credentials_obj): Initializes the pipeline, including the handle, wrangler, and loader.
    - run(): Runs every task in the pipe and then updates _status

    Inherits all methods from the parent class, TaskManagementUtils.
    """
    def __init__(self, api_handle, tasks=None, tasks_file_path=None, tasks_kwargs=None, 
                 wrangler=None, loader=None, secret_id=None, name=None):
        super().__init__()
        self._status: PipeStatus = PipeStatus.IDLE
        self.name = name
        self.secret_id = secret_id
        self.handle = Handle(api_handle)
        self.wrangler = Wrangler(wrangler) if wrangler else None
        self.loader = Loader(loader) if loader else None
        tasks = tasks if tasks is not None else self.read_tasks(
            tasks_file_path, tasks_kwargs, dynamic_reader=self.dynamically_read)
        
        task_objs = [
            self.task_obj_from_dict(
                task_data, self.handle, self.wrangler, self.loader
            ) for task_data in tasks]
        
        self.task_map = {task.name: task for task in task_objs}
        self._pipe_failed = False    

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            if name in self.task_map:
                return self.task_map[name]
            else:
                raise AttributeError(f"Pipe object has no attribute '{name}'")

    def __getitem__(self, name):
        try:
            return self.task_map[name]
        except KeyError:
            raise KeyError(f"'{name}' not in Pipe")
            
    def __repr__(self):
        task_strs = [task.__repr__() for task in self.task_map.values()]
        task_str = '\n'.join(task_strs)
        indented_task_str = textwrap.indent(task_str, '  ')
        return (
            f"Pipe('{self.name}', status='{self._status.value}',\n"
                    f"{indented_task_str.rstrip(',')})")
    
    @property
    def status(self) -> PipeStatus:
        return self._status 
    
    def update_status(self):
        status_counts = self.get_task_status_counts()
        
        if status_counts[TaskStatus.PENDING.value] == len(self.task_map):
            pass
        elif status_counts[TaskStatus.COMPLETE.value] == len(self.task_map):
            self._status = PipeStatus.COMPLETE
        else:
            self._status = PipeStatus.INCOMPLETE

        return self.status
    
    def initialize_handle(self, credentials_obj):
        status = self.handle.initialize(credentials_obj)
        self.operator_statuses.update(handle=status)
        if status == OpStatus.FAIL:
            self._status = PipeStatus.FAIL
            
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
        handle_status = self.initialize_handle(credentials_obj)
        self.initialize_wrangler()
        self.initialize_loader()
        if self._status == PipeStatus.IDLE and handle_status == OpStatus.INITIALIZED:
            self._status = PipeStatus.READY
        
        return self.operator_statuses
        
    def run(self):
        for task_name, task in self.task_map.items():
            new_status = task.run()
        
        pipe_status = self.update_status()
        return pipe_status
    
    
class Pipeline(Pipe):
    """
    A class for managing and executing pipelines made up of individual pipes.

    :param pipes: A list of dictionaries representing pipes in the pipeline.
    :param pipeline_id: (Optional) An identifier for the pipeline.

    :method:
    - `initialize_pipe(pipe_name)`: Initializes a single pipe in the pipeline.
    - `initialize_pipes(pipe_names)`: Initializes multiple pipes in the pipeline.
    - `initialize()`: Initializes all pipes in the pipeline.
    - `run_task(pipe_name, task_name)`: Executes a single task in a pipe.
    - `run_pipe(pipe_name)`: Executes all tasks in a single pipe.
    - `get_pipe(pipe_name)`: Returns a single pipe in the pipeline.
    - `get_pipe_data(pipe_name, operator=None)`: Returns data generated by a pipe.

    :attribute:
    - `id`: An identifier for the pipeline.
    - `credential_manager`: An object for managing credentials.
    - `pipes`: A dictionary mapping pipe names to pipes.
    """
    def __init__(self, pipeline_id: str, pipes: [dict,], credential_manager: 'func'):
        #self.loader = loader
        self.id = pipeline_id
        self.credential_manager = credential_manager
        self.pipes = {pipe.name:pipe for pipe in pipes}
    
    def __repr__(self):
        pipe_strs = [pipe.__repr__() for pipe in self.pipes.values()]
        pipe_str = '\n'.join(pipe_strs)
        indented_pipe_str = textwrap.indent(pipe_str, '  ')
        return (
            f"Pipeline('{self.id}',\n"
                    f"{indented_pipe_str.rstrip(',')})")
    
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            if name in self.pipes:
                return self.pipes[name]
            else:
                raise AttributeError(f"Pipeline object has no attribute '{name}'")

    def __getitem__(self, name):
        try:
            return self.pipes[name]
        except KeyError:
            raise KeyError(f"'{name}' not in Pipeline")
    
    @staticmethod
    def _check_pipe_not_failed(pipe):
        if pipe._pipe_failed:
            raise Exception("Pipeline failed earlier, cannot run further operations")
            
    def tasks(self):
        for pipe_name, pipe in self.pipes.items():
            for task_name, task in pipe.task_map.items():
                yield {pipe_name: {task_name: task}}
    
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
    
    def run(self):
        results = dict()
        for pipe_name, pipe in self.pipes.items():
            self._check_pipe_not_failed(pipe)
            result = pipe.run()
            results.update({pipe_name:result})
        
        return results
    
    def get_pipe(self, pipe_name):
        return self.pipes[pipe_name]
    
    def get_pipe_data(self, pipe_name, operator=None):
        pipe = self.pipes[pipe_name]
        data = pipe.get_all_tasks_data(operator=operator)
        
        return data
