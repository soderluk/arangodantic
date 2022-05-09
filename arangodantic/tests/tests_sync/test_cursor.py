from .conftest import Identity


def test_to_list(identity_collection, identity_alice, identity_bob):
    identities = Identity.find().to_list()

    assert len(identities) == 2
    assert any(i.name == "Alice" for i in identities)
    assert any(i.name == "Bob" for i in identities)

    # Can also be expressed like this:
    cursor = Identity.find()
    identities = cursor.to_list()

    assert len(identities) == 2
    assert any(i.name == "Alice" for i in identities)
    assert any(i.name == "Bob" for i in identities)


def test_full_count(identity_collection, identity_alice, identity_bob):
    cursor = Identity.find(limit=1, full_count=True)
    assert len(cursor.to_list()) == 1
    assert cursor.full_count == 2
