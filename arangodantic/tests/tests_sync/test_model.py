from typing import List
from uuid import uuid4

import pytest
from arango import CursorCountError

from arangodantic import (
    ASCENDING,
    DESCENDING,
    DataSourceNotFound,
    ModelNotFoundError,
    MultipleModelsFoundError,
    UniqueConstraintError,
)
from arangodantic.utils import SortTypes

from .conftest import ExtendedIdentity, Identity, Link, SubModel


def test_save_and_load_model(identity_collection):
    identity = Identity(name="John Doe")
    identity.save()

    assert identity.key_ is not None
    assert identity.rev_ is not None

    loaded_identity = Identity.load(identity.key_)
    assert identity.key_ == loaded_identity.key_


def test_unique_constraint(identity_collection):
    # Create unique index on the "name" field.
    Identity.get_collection().add_hash_index(fields=["name"], unique=True)

    pre_generated_key = str(uuid4())

    identity = Identity(name="John Doe", _key=pre_generated_key)
    identity.save()

    assert identity.rev_ is not None

    loaded_identity = Identity.load(pre_generated_key)
    assert loaded_identity.key_ == pre_generated_key

    # Colliding primary key
    identity_2 = Identity(name="Jane Doe", _key=pre_generated_key)
    with pytest.raises(UniqueConstraintError):
        identity_2.save()

    identity_2.key_ = None
    identity_2.save()

    # Colliding "name"
    identity_3 = Identity(name="Jane Doe")
    with pytest.raises(UniqueConstraintError):
        identity_3.save()

    identity_3.name = "Jane Jr. Doe"
    identity_3.save()

    with pytest.raises(UniqueConstraintError):
        identity_3.name = "Jane Doe"
        identity_3.save()


def test_delete_model(identity_collection):
    identity = Identity(name="Jane Doe")
    identity.save()
    assert identity.delete() is True

    with pytest.raises(ModelNotFoundError):
        identity.delete()

    assert identity.delete(ignore_missing=True) is False


def test_reload(identity_collection):
    identity = Identity(name="Jane Doe")
    with pytest.raises(ModelNotFoundError):
        identity.reload()

    identity.save()

    loaded_identity = Identity.load(identity.key_)
    assert identity.key_ == loaded_identity.key_

    identity.name = "Jane Austen"
    identity.save()

    assert loaded_identity.name == "Jane Doe"
    loaded_identity.reload()
    assert loaded_identity.name == "Jane Austen"


def test_locking(identity_collection):
    identity = Identity(name="James Doe")
    identity.save()

    second_lock = Identity.get_lock(identity.key_)

    # Lock and reload the identity
    with identity.lock_and_reload():
        # Verify the lock is held
        assert second_lock.acquire(block=False) is False

    # Verify the lock is not held any longer
    try:
        assert second_lock.acquire(block=False) is True
    finally:
        second_lock.release()

    # Lock and load as new identity
    with Identity.lock_and_load(identity.key_) as _:
        # Verify the lock is held
        assert second_lock.acquire(block=False) is False

    # Verify the lock is not held any longer
    try:
        assert second_lock.acquire(block=False) is True
    finally:
        second_lock.release()


def test_find(identity_collection):
    i_x = Identity(name="Do not find me")
    i_x.save()

    i_1 = Identity(name="John Doe")
    i_1.save()

    i_y = Identity(name="Do not find me either")
    i_y.save()

    i_2 = Identity(name="James Doe")
    i_2.save()

    i_3 = Identity(name="James Doe")
    i_3.save()

    results = (Identity.find({"name": "John Doe"})).to_list()

    assert len(results) == 1
    assert i_1.key_ == results[0].key_
    assert i_1.name == results[0].name

    with (Identity.find({"name": "James Doe"})) as cursor:
        results = [i for i in cursor]

    assert len(results) == 2
    for r in results:
        assert r.name == "James Doe"

    with pytest.raises(CursorCountError):
        len(cursor)

    with (Identity.find({"name": "James Doe"}, count=True)) as cursor:
        assert len(cursor) == 2


def test_find_with_comparisons(identity_collection):
    i_a = Identity(name="a")
    i_a2 = Identity(name="a")
    i_b = Identity(name="b")
    i_c = Identity(name="c")

    i_a.save()
    i_a2.save()
    i_b.save()
    i_c.save()

    cursor = Identity.find({"name": "a"}, count=True)
    with cursor:
        assert len(cursor) == 2
        for i in cursor:
            assert i.name == "a"

    cursor = Identity.find({"name": {"<": "a"}}, count=True)
    with cursor:
        assert len(cursor) == 0

    cursor = Identity.find({"name": {"<=": "a"}}, count=True)
    with cursor:
        assert len(cursor) == 2
        for i in cursor:
            assert i.name == "a"

    cursor = Identity.find({"name": {">": "c"}}, count=True)
    with cursor:
        assert len(cursor) == 0

    cursor = Identity.find({"name": {">": "b"}}, count=True)
    with cursor:
        assert len(cursor) == 1
        for i in cursor:
            assert i.name == "c"

    cursor = Identity.find({"name": {">=": "b"}}, count=True)
    with cursor:
        assert len(cursor) == 2
        for i in cursor:
            assert i.name in {"b", "c"}

    cursor = Identity.find({"name": {">": "a", "<": "c"}}, count=True)
    with cursor:
        assert len(cursor) == 1
        for i in cursor:
            assert i.name == "b"

    cursor = Identity.find({"name": "a", "_id": {"!=": i_a}}, count=True)
    with cursor:
        assert len(cursor) == 1
        for i in cursor:
            assert i.id_ == i_a2.id_


@pytest.mark.parametrize(
    "bad_str",
    [
        "'`´ \"$&=?+._",
        "a..b",
        ".a",
        "b.",
        "a..b",
        "...a....b...",
    ],
)
def test_find_one(identity_collection, bad_str: str):
    i = Identity(name="John Doe")
    i.save()

    i_found = Identity.find_one({"name": "John Doe"})

    assert i.key_ == i_found.key_

    with pytest.raises(ModelNotFoundError):
        Identity.find_one({"name": bad_str})

    with pytest.raises(ModelNotFoundError):
        Identity.find_one({bad_str: "John Doe"})


def test_find_one_multiple_matches(identity_collection):
    i = Identity(name="John Doe")
    i_2 = Identity(name="John Doe")
    i.save()
    i_2.save()

    Identity.find_one({"name": "John Doe"})
    with pytest.raises(MultipleModelsFoundError):
        Identity.find_one({"name": "John Doe"}, raise_on_multiple=True)


def test__before_save(extended_identity_collection):
    identity = ExtendedIdentity(name="John Doe")
    identity.save(override_extra="foo")
    identity = ExtendedIdentity.load(identity.key_)
    assert identity.extra == "foo"


def test_sub_models(extended_identity_collection):
    sub = SubModel(text="foo")
    identity = ExtendedIdentity(name="John Doe", sub=sub)
    identity.save()

    identity = ExtendedIdentity.load(identity.key_)
    assert isinstance(identity.sub, SubModel)
    assert identity.sub.text == "foo"

    identity.sub = None
    identity.reload()
    assert isinstance(identity.sub, SubModel)
    assert identity.sub.text == "foo"


def test_find_with_sub_models(extended_identity_collection):
    sub_1 = SubModel(text="foo")
    identity_1 = ExtendedIdentity(name="John Doe", sub=sub_1)
    identity_1.save()

    sub_2 = SubModel(text="bar")
    identity_2 = ExtendedIdentity(name="John Doe", sub=sub_2)
    identity_2.save()

    with (ExtendedIdentity.find({"sub.text": "foo"}, count=True)) as cursor:
        assert len(cursor) == 1
        found = cursor.next()
        assert found.key_ == identity_1.key_


def test_delete_collection(identity_collection):
    assert (Identity.delete_collection()) is True
    assert (Identity.delete_collection()) is False
    with pytest.raises(DataSourceNotFound):
        Identity.delete_collection(ignore_missing=False)


def test_truncate_collection(identity_collection):
    assert (Identity.truncate_collection()) is True

    Identity.delete_collection()

    assert (Identity.truncate_collection()) is False
    with pytest.raises(DataSourceNotFound):
        Identity.truncate_collection(ignore_missing=False)


def test_edge_model(
    identity_collection,
    link_collection,
    identity_alice: Identity,
    identity_bob: Identity,
):
    link = Link(_from=identity_alice, _to=identity_bob, type="Knows")
    link.save()
    assert link.from_ == identity_alice
    assert link.to_ == identity_bob
    assert link.from_key_ == identity_alice.key_
    assert link.to_key_ == identity_bob.key_

    link.reload()
    assert link.from_ == identity_alice.id_
    assert link.to_ == identity_bob.id_
    assert link.from_key_ == identity_alice.key_
    assert link.to_key_ == identity_bob.key_


def test_find_one_edge_model(
    identity_collection,
    link_collection,
    identity_alice: Identity,
    identity_bob: Identity,
):
    ab = Link(_from=identity_alice, _to=identity_bob, type="Knows")
    ab.save()

    with pytest.raises(ModelNotFoundError):
        Link.find_one({"_from": identity_bob, "_to": identity_alice})

    assert (
        ab.key_ == (Link.find_one({"_from": identity_alice, "_to": identity_bob})).key_
    )


@pytest.mark.parametrize(
    "sort,expected",
    [
        (
            [
                ("name", DESCENDING),
            ],
            [
                "david",
                "cecil",
                "bob",
                "alice",
            ],
        ),
        (
            [
                ("extra", DESCENDING),
                ("name", ASCENDING),
            ],
            [
                "bob",
                "cecil",
                "david",
                "alice",
            ],
        ),
        (
            [
                ("sub.text", ASCENDING),
                ("extra", ASCENDING),
            ],
            [
                "cecil",
                "david",
                "bob",
                "alice",
            ],
        ),
    ],
)
def test_find_with_sort(
    extended_identity_collection, sort: SortTypes, expected: List[str]
):
    identities = [
        ExtendedIdentity(name="alice", extra="xxx", sub=SubModel(text="nnn")),
        ExtendedIdentity(name="bob", extra="zzz", sub=SubModel(text="mmm")),
        ExtendedIdentity(name="cecil", extra="yyy", sub=SubModel(text="lll")),
        ExtendedIdentity(name="david", extra="yyy", sub=SubModel(text="mmm")),
    ]
    for identity in identities:
        identity.save()

    found_identities = (ExtendedIdentity.find(sort=sort)).to_list()
    assert [identity.name for identity in found_identities] == expected


def test_find_one_with_sort(identity_collection):
    identities = [
        Identity(name="Bob"),
        Identity(name="Alice"),
    ]
    for identity in identities:
        identity.save()

    found = Identity.find_one(sort=[("name", ASCENDING)])
    assert found.name == "Alice"

    found = Identity.find_one(sort=[("name", DESCENDING)])
    assert found.name == "Bob"
