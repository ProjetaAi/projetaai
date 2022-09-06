"""Script tools for building CLI commands."""
from __future__ import annotations
from abc import abstractmethod
from dataclasses import dataclass, fields
from typing import Callable, ClassVar, Dict, Union
from typing_extensions import Literal

from kedro_projetaai.utils.string import to_snake_case


@dataclass
class Step(Callable):
    """Base class for sequential steps.

    Example:
        >>> class MyStep(Step):
        ...     def run(self):  # must implement this method
        ...         a = 1
        ...         self.log('info', 'doing math')
        ...         a = a + 10
        ...         return {'a': a}
        >>> step = MyStep()
        >>> step
        MyStep()
        >>> step()
        🕒 running my step
        🛈 doing math
        ✓ finished my step
        <BLANKLINE>
        {'a': 11}

        >>> class MyStep(Step):
        ...     def run(self):
        ...         raise Exception('something went wrong')
        >>> step = MyStep()
        >>> step()  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        Exception: something went wrong
    """

    __MARKERS: ClassVar[Dict[str, str]] = {
        'done': '\N{check mark}',
        'working': '\N{clock face three oclock}',
        'error': '\N{cross mark}',
        'info': '\N{circled information source}',
        'blank': ''
    }

    @classmethod
    def from_dict(cls, kwargs: dict) -> Step:
        """Creates an object of the class given a dict of params.

        Args:
            kwargs (dict)

        Returns:
            ConverterStep
        """
        fieldset = {f.name for f in fields(cls) if f.init is True}
        return cls(**{f: kwargs[f] if f in kwargs else None for f in fieldset})

    def log(
        self, marker: Literal['done', 'working', 'error', 'blank', 'info'],
        message: str
    ):
        """Logs a message with a marker."""
        message = f'{self.__MARKERS[marker]} {message}'
        print(message)

    def log_ignored(self):
        """Logs a step ignored message.

        Example:
            >>> class MyStep(Step):
            ...     def run(self):
            ...         self.log_ignored()
            >>> MyStep()()
            🕒 running my step
            🛈 my step ignored
            ✓ finished my step
            <BLANKLINE>
            {}
        """
        self.log('info', f'{self.formatted_class_name} ignored')

    @property
    def formatted_class_name(self) -> str:
        """Returns the class name in snake_case.

        Returns:
            str: The class name in snake_case.
        """
        return ' '.join(
            to_snake_case(self.__class__.__name__).split('_')
        )

    @abstractmethod
    def run(self) -> Union[None, dict]:
        """Executes the step logic."""
        pass  # pragma: no cover

    def __call__(self) -> dict:
        """Executes the step logic.

        Raises:
            e: The exception raised by the step.

        Returns:
            dict: The result of the step.
        """
        self.log('working', f'running {self.formatted_class_name}')
        try:
            out = self.run() or {}
            self.log('done', f'finished {self.formatted_class_name}')
        except Exception as e:
            self.log('error', f'{self.formatted_class_name} errored')
            raise e
        self.log('blank', '')
        return out


def pipe(*args: Step, initial_dict: dict = None) -> dict:
    """Runs steps sequentially accumulating its outputs.

    Args:
        *args (Step): Steps to run

    Example:
        >>> @dataclass
        ... class MyStep(Step):
        ...     def run(self):
        ...         return {'a': 1} # return a dict
        >>> @dataclass
        ... class MyStep2(Step):
        ...     a: int
        ...     def run(self):
        ...         print(self.a)
        >>> pipe(MyStep, MyStep2)
        🕒 running my step
        ✓ finished my step
        <BLANKLINE>
        🕒 running my step2
        1
        ✓ finished my step2
        <BLANKLINE>
        {'a': 1}
    """
    kwargs = initial_dict or {}
    for step in args:
        kwargs.update(step.from_dict(kwargs)())
    return kwargs
