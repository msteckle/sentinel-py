"""Base classes shared by pipeline readers, processors, and writers."""

from __future__ import annotations

from typing import ClassVar, Generic, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    """Name-keyed registry of pipeline components referenced from the YAML.

    Subclasses set :attr:`kind` (used in error messages) and implement
    :meth:`key_of` to say which attribute of a component is its YAML name. They
    typically add a domain-specific alias for :attr:`names` (for example
    ``type_names``) so existing call sites stay readable.
    """

    kind: ClassVar[str] = "component"

    def __init__(self) -> None:
        self._items: dict[str, T] = {}

    @staticmethod
    def key_of(item: T) -> str:
        """Return the YAML name under which ``item`` is registered."""
        raise NotImplementedError

    @staticmethod
    def normalize(name: str) -> str:
        """Normalize a registry key; override to make lookups case-insensitive."""
        return name.strip()

    def register(self, item: T) -> None:
        """Register a component, rejecting empty and duplicate names.

        Parameters
        ----------
        item : T
            Component to register under ``key_of(item)``.

        Raises
        ------
        ValueError
            If the name is empty or already registered.
        """
        name = self.normalize(self.key_of(item))
        if not name:
            raise ValueError(f"Registered {self.kind} names must be non-empty")
        if name in self._items:
            raise ValueError(f"The {self.kind} is already registered: {name}")
        self._items[name] = item

    def get(self, name: str) -> T:
        """Return the component registered as ``name``.

        Raises
        ------
        KeyError
            If no component has that name.
        """
        try:
            return self._items[self.normalize(name)]
        except KeyError as error:
            raise KeyError(f"Unknown {self.kind}: {name}") from error

    @property
    def names(self) -> tuple[str, ...]:
        """Return registered names in deterministic order."""
        return tuple(sorted(self._items))
