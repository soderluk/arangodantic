from typing import List, Optional, Type

from arango import CursorCloseError
from arango.cursor import Cursor

from arangodantic.exceptions import CursorError, CursorNotFoundError


class ArangodanticCursor:
    """
    Wrapper for the arango.cursor.Cursor that will give back instances of the
    defined class rather than dictionaries.
    """

    __slots__ = [
        "cls",
        "cursor",
    ]

    def __init__(self, cls, cursor: Cursor):
        from .models import Model

        self.cls: Type[Model] = cls
        self.cursor: Cursor = cursor

    def __iter__(self):
        return self

    def __next__(self):  # pragma: no cover
        return self.next()

    def __enter__(self):
        return self

    def __len__(self):
        return len(self.cursor)

    def __exit__(self, *_):
        self.close(ignore_missing=True)

    def __repr__(self):
        cursor_id_str = ""
        if self.cursor.id:
            cursor_id_str = f" (Cursor: {self.cursor.id})"
        return f"<ArangodanticCursor ({self.cls.__name__}){cursor_id_str}>"

    def close(self, ignore_missing: bool = False) -> Optional[bool]:
        """
        Close the cursor to free server side resources.

        :param ignore_missing: Do not raise an exception if the cursor is missing on the
        server side.
        :return: True if cursor was closed successfully, False if cursor was missing on
        the server side and **ignore_missing** was True, None if there were no cursors
        to close server-side.
        :raise CursorNotFoundError: If the cursor was missing and **ignore_missing** was
        False.
        """
        try:
            result: Optional[bool] = self.cursor.close(ignore_missing=ignore_missing)
        except CursorCloseError as ex:
            if ex.error_code == 404:
                raise CursorNotFoundError(ex.error_message)
            raise

        return result

    def next(self):
        return self.cls(**(self.cursor.next()))

    def to_list(self) -> List:
        """
        Convert the cursor to a list.
        """
        with self as cursor:
            return [i for i in cursor]

    @property
    def full_count(self) -> int:
        """
        Get the full count.

        :return: The full count.
        :raise CursorError: If the cursor statistics do not contain the full count.
        """
        stats = self.cursor.statistics()
        try:
            full_count: int = stats["fullCount"]
        except KeyError as e:
            raise CursorError(
                "Cursor statistics has no full count, did you use full_count=True?"
            ) from e

        return full_count
