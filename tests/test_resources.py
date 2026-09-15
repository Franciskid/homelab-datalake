"""LakeStorage: the object-write path, and a guard against the exact bug that
reached production once already.

A merge landed two `write_bytes` methods in one class body -- one from this
source, carrying `content_type`; one from the access-log workstream, landing
inbox fixtures with no content type. Python keeps whichever definition comes
later in the class body and silently shadows the other; nothing at runtime can
tell the difference between "this method was defined once" and "it was defined
twice and the second one won". Every frame write in production then raised
`TypeError: write_bytes() got an unexpected keyword argument 'content_type'`,
and no test caught it because every test that exists calls the method exactly
one way -- which is also true of a method defined exactly once. Catching a
duplicate definition needs to look at the source, not call the method.
"""

import ast
import inspect

from homelab_datalake.resources import LakeStorage


def _method_defs(cls: type, name: str) -> list[ast.AST]:
    """Every `def <name>` directly in `cls`'s own class body, found by parsing its
    source rather than by asking Python what it resolved to -- `getattr` and
    friends only ever see the survivor of a duplicate definition."""
    source = inspect.getsource(cls)
    tree = ast.parse(source)
    class_node = tree.body[0]
    assert isinstance(class_node, ast.ClassDef), f"expected a class definition, got {class_node}"
    return [
        node for node in class_node.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name
    ]


def test_write_bytes_is_defined_exactly_once():
    defs = _method_defs(LakeStorage, "write_bytes")
    assert len(defs) == 1, (
        f"LakeStorage.write_bytes is defined {len(defs)} times in the class body; "
        "only the last one is reachable at runtime and the rest are silently dead "
        "code -- this is exactly how the production TypeError happened"
    )


def test_no_method_on_lakestorage_is_defined_twice():
    """The general form of the guard above: the same shadowing could happen to any
    method, not only this one, and a second def is just as invisible either way."""
    source = inspect.getsource(LakeStorage)
    tree = ast.parse(source)
    class_node = tree.body[0]
    names = [
        node.name for node in class_node.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    ]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    assert not duplicates, f"defined more than once on LakeStorage: {duplicates}"


def test_write_bytes_accepts_a_content_type():
    signature = inspect.signature(LakeStorage.write_bytes)
    assert "content_type" in signature.parameters
    assert signature.parameters["content_type"].default == "application/octet-stream"


def test_write_bytes_content_type_is_optional():
    """A caller with no opinion on it -- landing an inbox fixture, say -- must
    still be able to call write_bytes(key, data) with no third argument."""
    signature = inspect.signature(LakeStorage.write_bytes)
    assert signature.parameters["content_type"].default is not inspect.Parameter.empty


# --------------------------------------------------------------------------------
# behaviour, on the local-disk backend the test suite runs against
# --------------------------------------------------------------------------------


def test_write_bytes_writes_the_object_and_returns_its_length(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    written = lake.write_bytes("frames/dt=2026-09-14/a/0.jpg", b"\xff\xd8\xff", content_type="image/jpeg")
    assert written == 3
    assert (tmp_path / "frames/dt=2026-09-14/a/0.jpg").read_bytes() == b"\xff\xd8\xff"


def test_write_bytes_works_with_no_content_type_argument(tmp_path):
    """The inbox-fixture call shape: no third argument at all."""
    lake = LakeStorage(local_root=str(tmp_path))
    lake.write_bytes("inbox/fixture.csv", b"a,b,c\n1,2,3\n")
    assert (tmp_path / "inbox/fixture.csv").read_bytes() == b"a,b,c\n1,2,3\n"


def test_write_bytes_leaves_no_temp_file_behind(tmp_path):
    """Same atomicity story as write_table: write to a .tmp path, then rename."""
    lake = LakeStorage(local_root=str(tmp_path))
    lake.write_bytes("x/y.bin", b"data")
    leftovers = list((tmp_path / "x").glob("*.tmp"))
    assert leftovers == []


def test_write_bytes_creates_parent_directories(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    lake.write_bytes("deeply/nested/path/object.bin", b"x")
    assert (tmp_path / "deeply/nested/path/object.bin").exists()


def test_write_bytes_overwrites_an_existing_object(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    lake.write_bytes("k", b"first")
    lake.write_bytes("k", b"second-value")
    assert (tmp_path / "k").read_bytes() == b"second-value"
