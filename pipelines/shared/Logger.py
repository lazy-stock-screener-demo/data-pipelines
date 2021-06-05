import inspect


def dumpFunc(func):
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str = ", ".join(
            "{} = \n{!r}\n".format(*item) for item in func_args.items()
        )
        print(f"Function: {func.__qualname__}\n=> Arguments: ({func_args_str})")
        result = func(*args, **kwargs)
        print(f"=> Returns: \n{result}\n")
        return result

    return wrapper


def dumpClass(decorator):
    def decorate(cls):
        for attr in cls.__dict__:  # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls

    return decorate