from abc import ABCMeta, abstractmethod


class AbstractStrategy(metaclass=ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'execute') and callable(subclass.execute) or NotImplemented)

    @abstractmethod
    def execute(self):
        raise NotImplementedError
