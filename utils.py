import pandas as pd

class PandasWrangler:
    def __init__(self):
        self.vars = {}
        self.operator_mapping = {
            '==': 'eq',
            '!=': 'ne',
            '<': 'lt',
            '>': 'gt',
            '<=': 'le',
            '>=': 'ge'
        }

    def wrangle(self, data, *methods):
        self.__init__()   
        methods = [*methods]
        
        # Wranglers will soon be updated to only accept the handle_response and wrangle_ops
        df = pd.DataFrame(data.data['handle'])

        for method in methods:
            method_name = list(method.keys())[0]
            method_params = method[method_name]
            try:
                df = self._hook(df, method_name, method_params)
            except Exception as e:
                raise Exception('{'+method_name+': '+str(method_params)+'}'+f'\n  Produced Error:\n{e}') from e

        return df

    def _hook(self, df, method_name, method_params):
        if method_name == '_operator':
            return self._handle_operator(df, method_params)
        elif method_name == "_variables":
            return self._handle_variables(df, method_params)
        elif method_name == 'loc':
            return self._handle_loc(df, method_params)
        elif method_name.startswith("__"):
            return self._handle_stored_variable(df, method_name, method_params)
        else:
            return self._handle_regular_method(df, method_name, method_params)

    def _handle_operator(self, df, method_params):
        column, operator, value = method_params
        method_name = self.operator_mapping.get(operator)
        method_params = [value]
        df = df.loc[self._handle_regular_method(df[column], method_name, method_params)]
        return df

    def _handle_variables(self, df, method_params):
        for var_name, var_params in method_params.items():
            self.vars[var_name] = var_params
        return df

    def _handle_loc(self, df, method_params):
        if type(method_params) is dict:
            index = method_params.get('index', [])
            columns = method_params.get('columns', [])
            if index and columns:
                df = df.loc[index, columns]
            elif index:
                df = df.loc[index]
            elif columns:
                df = df.loc[:, columns]
        elif type(method_params) is tuple:
            df = df.loc[method_params]
        return df

    def _handle_stored_variable(self, df, method_name, method_params):
        var_name = method_name[2:]
        if var_name not in self.vars:
            raise KeyError(f"No stored variables with name '{var_name}'")
        method_params = self.vars[var_name]

        method = getattr(df, method_params["method_name"])
        if method is None:
            raise AttributeError(f"DataFrame has no attribute '{method_params['method_name']}'")
        if not callable(method):
            raise TypeError(f"'{method_params['method_name']}' is not callable")

        method_params_type = type(method_params["method_params"])
        if method_params_type is dict:
            args = method_params["method_params"].pop('args', [])
            kwargs = method_params["method_params"].pop('kwargs', {})
            df = method(*args, **kwargs, **method_params["method_params"])
        elif method_params_type is list:
            df = method(*method_params["method_params"])
        else:
            raise TypeError(f"Method parameters must be a list or a dict, not '{method_params_type}'")

        return df

    def _handle_regular_method(self, df, method_name, method_params):
        method = getattr(df, method_name)
        if method is None:
            raise AttributeError(f"DataFrame has no attribute '{method_name}'")
        if not callable(method):
            raise TypeError(f"'{method_name}' is not callable")

        method_params_type = type(method_params)
        if method_params_type is dict:
            args = method_params.pop('args', [])
            kwargs = method_params.pop('kwargs', {})
            df = method(*args, **kwargs, **method_params)
        elif method_params_type is list:
            df = method(*method_params)
        else:
            raise TypeError(f"Method parameters must be a list or a dict, not '{method_params_type}'")     

        return df