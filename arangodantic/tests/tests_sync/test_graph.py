from uuid import uuid4

import pytest

from arangodantic.exceptions import (
    GraphNotFoundError,
    ModelNotFoundError,
    UniqueConstraintError,
)

from .conftest import (
    Person,
    Relation,
    RelationGraph,
    SecondaryRelation,
    SecondaryRelationGraph,
)


def test_save_through_graph(relation_graph):
    alice = Person(name="Alice")
    bob = Person(name="Bob")

    RelationGraph.save(alice)
    assert alice.key_
    assert alice.rev_
    RelationGraph.save(bob)

    alice.name = "Alice in Wonderland"
    RelationGraph.save(alice)
    alice.reload()
    assert alice.name == "Alice in Wonderland"

    ab = Relation(_from=alice, _to=bob, kind="BFF")
    RelationGraph.save(ab)
    assert ab.key_
    assert ab.rev_
    ab.kind = "BF"
    RelationGraph.save(ab)
    ab.reload()
    assert ab.kind == "BF"


def test_save_through_graph_model_not_found(relation_graph):
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    cecil = Person(name="Cecil")

    RelationGraph.save(alice)
    RelationGraph.save(bob)
    RelationGraph.save(cecil)

    # Saving a new edge through graph should fail if one of the vertices has been
    # deleted
    bob_id = bob.id_
    bob.delete()
    ab = Relation(_from=alice, _to=bob_id, kind="BFF")
    with pytest.raises(ModelNotFoundError):
        RelationGraph.save(ab)

    # Updating an existing edge should fail if one of the vertices has been deleted
    ac = Relation(_from=alice, _to=cecil, kind="BFF")
    RelationGraph.save(ac)
    cecil.delete()
    with pytest.raises(ModelNotFoundError):
        RelationGraph.save(ac)


def test_unique_constraint_graph(relation_graph):
    # Create unique index on the "name" field.
    Person.get_collection().add_hash_index(fields=["name"], unique=True)

    pre_generated_key = str(uuid4())

    person = Person(name="John Doe", _key=pre_generated_key)
    RelationGraph.save(person)

    assert person.rev_ is not None

    loaded_person = Person.load(pre_generated_key)
    assert loaded_person.key_ == pre_generated_key

    # Colliding primary key
    person_2 = Person(name="Jane Doe", _key=pre_generated_key)
    with pytest.raises(UniqueConstraintError):
        RelationGraph.save(person_2)

    person_2.key_ = None
    person_2.save()

    # Colliding "name"
    person_3 = Person(name="Jane Doe")
    with pytest.raises(UniqueConstraintError):
        RelationGraph.save(person_3)

    person_3.name = "Jane Jr. Doe"
    RelationGraph.save(person_3)

    with pytest.raises(UniqueConstraintError):
        person_3.name = "Jane Doe"
        RelationGraph.save(person_3)


def test_deletion_through_graph(relation_graph, secondary_relation_graph):
    # Create some example persons
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    malory = Person(name="Malory")
    alice.save()
    bob.save()
    malory.save()

    # Create some edges between them in the primary graph
    ab = Relation(_from=alice, _to=bob, kind="BFF")
    ab.save()
    am = Relation(_from=alice, _to=malory, kind="hates")
    am.save()

    # Create some edges between them in the secondary graph
    ab2 = SecondaryRelation(_from=alice, _to=bob, kind="knows")
    ab2.save()
    am2 = SecondaryRelation(_from=alice, _to=malory, kind="hates")
    am2.save()

    # Delete document normally (not using graph) and check edges were left in place
    assert malory.delete()
    am.reload()
    am2.reload()
    # Clean up orphaned edges manually
    assert RelationGraph.delete(am)
    assert SecondaryRelationGraph.delete(am2)
    assert not RelationGraph.delete(am, ignore_missing=True)
    assert not SecondaryRelationGraph.delete(am2, ignore_missing=True)

    # Delete document through graph and verify edges were deleted from both graphs.
    assert RelationGraph.delete(bob)
    with pytest.raises(ModelNotFoundError):
        ab.reload()
    with pytest.raises(ModelNotFoundError):
        ab2.reload()
    with pytest.raises(ModelNotFoundError):
        RelationGraph.delete(ab)
    with pytest.raises(ModelNotFoundError):
        SecondaryRelationGraph.delete(ab2)
    assert not RelationGraph.delete(ab, ignore_missing=True)
    assert not SecondaryRelationGraph.delete(ab2, ignore_missing=True)

    assert not RelationGraph.delete(bob, ignore_missing=True)
    with pytest.raises(ModelNotFoundError):
        RelationGraph.delete(bob)


def test_delete_graph(relation_graph):
    assert RelationGraph.delete_graph()
    with pytest.raises(GraphNotFoundError):
        assert not RelationGraph.delete_graph()
    assert not RelationGraph.delete_graph(ignore_missing=True)

    assert RelationGraph.get_db().has_collection(Person.get_collection_name())
    assert RelationGraph.get_db().has_collection(Relation.get_collection_name())

    RelationGraph.ensure_graph()
    assert RelationGraph.delete_graph(drop_collections=True)
    with pytest.raises(GraphNotFoundError):
        assert not RelationGraph.delete_graph()
    assert not RelationGraph.get_db().has_collection(Person.get_collection_name())
    assert not RelationGraph.get_db().has_collection(Relation.get_collection_name())
