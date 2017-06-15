"""
This module contains the container classes to create objects
that persist directly in a Redis server.
"""

import collections
from functools import partial


class Container(object):
    """
    Base class for all containers. This class should not
    be used and does not provide anything except the ``db``
    member.
    :members:
    """

    def __init__(self, key, db=None, pipeline=None):
        self._db = db
        self.key = key
        self.pipeline = pipeline

    def clear(self):
        """
        Remove the container from the redis storage

        >>> s = Set('test')
        >>> s.add('1')
        1
        >>> s.clear()
        >>> s.members
        set([])


        """
        del self.db[self.key]

    def __getattribute__(self, att):
        if att in object.__getattribute__(self, 'DELEGATEABLE_METHODS'):
            return partial(getattr(object.__getattribute__(self, 'db'), att), self.key)
        else:
            return object.__getattribute__(self, att)


    @property
    def db(self):
        if self.pipeline is not None:
            return self.pipeline
        if self._db is not None:
            return self._db
        if hasattr(self, 'db_cache') and self.db_cache is not None:
            return self.db_cache
        else:
            from redisco import connection
            self.db_cache = connection
            return self.db_cache

    DELEGATEABLE_METHODS = ()


class Set(Container):
    """A set stored in Redis."""

    def add(self, value):
        """Add the specified member to the Set."""
        self.sadd(value)

    def remove(self, value):
        """Remove the value from the redis set."""
        if not self.srem(value):
            raise KeyError, value
        
    def pop(self):
        """Remove and return (pop) a random element from the Set."""
        return self.spop()

    def discard(self, value):
        """Remove element elem from the set if it is present."""
        self.srem(value)

    def __len__(self):
        """Return the cardinality of set."""
        return self.scard()

    def __repr__(self):
        return "<%s '%s' %s>" % (self.__class__.__name__, self.key,
                self.members)

    # TODO: Note, the elem argument to the __contains__(), remove(),
    #       and discard() methods may be a set
    def __contains__(self, value):
        return self.sismember(value)

    def isdisjoint(self, other):
        """
        Return True if the set has no elements in common with other.

        :param other: another ``Set``
        :rtype: boolean

        >>> s1 = Set("key1")
        >>> s2 = Set("key2")
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'd', 'e'])
        3
        >>> s1.isdisjoint(s2)
        False
        >>> s1.clear()
        >>> s2.clear()
        """
        return not bool(self.db.sinter([self.key, other.key]))

    def issubset(self, other_set):
        """
        Test whether every element in the set is in other.

        :param other_set: another ``Set`` to compare to.

        >>> s1 = Set("key1")
        >>> s2 = Set("key2")
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add('b')
        1
        >>> s2.issubset(s1)
        True
        >>> s1.clear()
        >>> s2.clear()

        """
        return self <= other_set

    def __le__(self, other_set):
        return self.db.sinter([self.key, other_set.key]) == self.all()

    def __lt__(self, other_set):
        """Test whether the set is a true subset of other."""
        return self <= other_set and self != other_set

    def __eq__(self, other_set):
        """
        Test equality of:
        1. keys
        2. members
        """
        if other_set.key == self.key:
            return True
        slen, olen = len(self), len(other_set)
        if olen == slen:
            return self.members == other_set.members
        else:
            return False


    def issuperset(self, other_set):
        """
        Test whether every element in other is in the set.

        :param other_set: another ``Set`` to compare to.

        >>> s1 = Set("key1")
        >>> s2 = Set("key2")
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add('b')
        1
        >>> s1.issuperset(s2)
        True
        >>> s1.clear()
        >>> s2.clear()

        """
        return self >= other_set

    def __ge__(self, other_set):
        """Test whether every element in other is in the set."""
        return self.db.sinter([self.key, other_set.key]) == other_set.all()

    def __gt__(self, other_set):
        """Test whether the set is a true superset of other."""
        return self >= other_set and self != other_set

    # SET Operations
    def union(self, key, *others):
        """
        Return a new ``Set`` representing the union of *n* sets.

        :param key: String representing the key where to store the result (the union)
        :param other_sets: list of other ``Set``.
        :rtype: ``Set``

        >>> s1 = Set('key1')
        >>> s2 = Set('key2')
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['d', 'e'])
        2
        >>> s3 = s1.union('key3', s2)
        >>> s3.key
        u'key3'
        >>> s3.members
        set(['a', 'c', 'b', 'e', 'd'])
        >>> s1.clear()
        >>> s2.clear()
        >>> s3.clear()

        """
        if not isinstance(key, str):
            raise ValueError("String expected.")
        self.db.sunionstore(key, [self.key] + [o.key for o in others])
        return Set(key, db=self.db)

    def intersection(self, key, *others):
    	"""
        Return a new ``Set`` representing the intersection of *n* sets.

        :param key: String representing the key where to store the result (the union)
        :param other_sets: list of other ``Set``.
        :rtype: Set

        >>> s1 = Set('key1')
        >>> s2 = Set('key2')
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s3 = s1.intersection('key3', s2)
        >>> s3.key
        u'key3'
        >>> s3.members
        set(['c'])
        >>> s1.clear()
        >>> s2.clear()
        >>> s3.clear()
        """

        if not isinstance(key, str):
            raise ValueError("String expected.")
        self.db.sinterstore(key, [self.key] + [o.key for o in others])
        return Set(key, db=self.db)

    def difference(self, key, *others):
        """
        Return a new ``Set`` representing the difference of *n* sets.

        :param key: String representing the key where to store the result (the union)
        :param other_sets: list of other ``Set``.
        :rtype: Set

        >>> s1 = Set('key1')
        >>> s2 = Set('key2')
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s3 = s1.difference('key3', s2)
        >>> s3.key
        u'key3'
        >>> s3.members
        set(['a', 'b'])
        >>> s1.clear()
        >>> s2.clear()
        >>> s3.clear()
        """

        if not isinstance(key, str):
            raise ValueError("String expected.")
        self.db.sdiffstore(key, [self.key] + [o.key for o in others])
        return Set(key, db=self.db)

    def update(self, *other_sets):
        """Update the set, adding elements from all other_sets.

        :param other_sets: list of ``Set``
        :rtype: None
        """
        self.db.sunionstore(self.key, [self.key] + [o.key for o in other_sets])

    def __ior__(self, other_set):
        self.db.sunionstore(self.key, [self.key, other_set.key])
        return self

    def intersection_update(self, *other_sets):
        """
        Update the set, keeping only elements found in it and all other_sets.

        :param other_sets: list of ``Set``
        :rtype: None
        """
        self.db.sinterstore(self.key, [o.key for o in [self.key] + other_sets])

    def __iand__(self, other_set):
        self.db.sinterstore(self.key, [self.key, other_set.key])
        return self

    def difference_update(self, *other_sets):
        """
        Update the set, removing elements found in others.

        :param other_sets: list of ``Set``
        :rtype: None
        """
        self.db.sdiffstore(self.key, [o.key for o in [self.key] + other_sets])

    def __isub__(self, other_set):
        self.db.sdiffstore(self.key, [self.key, other_set.key])
        return self

    def all(self):
        return self.db.smembers(self.key)

    members = property(all)
    """
    return the real content of the Set.
    """

    def copy(self, key):
        """
        Copy the set to another key and return the new Set.

        .. WARNING::
            If the new key already contains a value, it will be overwritten.
        """
        copy = Set(key=key, db=self.db)
        copy.clear()
        copy |= self
        return copy

    def __iter__(self):
        return self.members.__iter__()

    def sinter(self, *other_sets):
        """
        Performs an intersection between Sets and return the *RAW* result.

        .. NOTE::
            This function return an actual ``set`` object (from python) and not a ``Set``. See func:``intersection``.
        """
        return self.db.sinter([self.key] + [s.key for s in other_sets])

    def sunion(self, *other_sets):
        """
        Performs a union between two sets and returns the *RAW* result.

        .. NOTE::
            This function return an actual ``set`` object (from python) and not a ``Set``.
        """
        return self.db.sunion([self.key] + [s.key for s in other_sets])

    def sdiff(self, *other_sets):
        """
        Performs a difference between two sets and returns the *RAW* result.

        .. NOTE::
            This function return an actual ``set`` object (from python) and not a ``Set``.
            See function difference.

        """
        return self.db.sdiff([self.key] + [s.key for s in other_sets])


    DELEGATEABLE_METHODS = ('sadd', 'srem', 'spop', 'smembers',
            'scard', 'sismember', 'srandmember')


class List(Container):
    """
    This class represent a list object as seen in redis.
    """

    def all(self):
        """
        Returns all items in the list.
        """
        return self.lrange(0, -1)
    members = property(all)
    """Return all items in the list."""

    def __len__(self):
        return self.llen()

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.lindex(index)
        elif isinstance(index, slice):
            indices = index.indices(len(self))
            return self.lrange(indices[0], indices[1])
        else:
            raise TypeError

    def __setitem__(self, index, value):
        self.lset(index, value)

    def append(self, value):
        """Append the value to the list."""
        self.rpush(value)
    push = append

    def extend(self, iterable):
        """
        Extend list by appending elements from the iterable.

        :param iterable: an iterable objects.
        """
        map(lambda i: self.rpush(i), iterable)

    def count(self, value):
        """
        Return number of occurrences of value.

        :param value: a value tha *may* be contained in the list
        """
        return self.members.count(value)

    def index(self, value):
        """Return first index of value."""
        return self.all().index(value)

    def pop(self):
        """Remove and return the last item"""
        return self.rpop()

    def pop_onto(self, key):
        """
        Remove an element from the list,
        atomically add it to the head of the list indicated by key
        """
        return self.rpoplpush(key)

    def shift(self):
        """Remove and return the first item."""
        return self.lpop()

    def unshift(self, value):
        """Add an element at the beginning of the list."""
        self.lpush(value)

    def remove(self, value, num=1):
        """Remove first occurrence of value."""
        self.lrem(value, num)

    def reverse(self):
        """
        Reverse the list in place.

        :return: None
        """
        r = self[:]
        r.reverse()
        self.clear()
        self.extend(r)

    def copy(self, key):
        """Copy the list to a new list.

        ..WARNING:
            If destination key already contains a value, it clears it before copying.
        """
        copy = List(key, self.db)
        copy.clear()
        copy.extend(self)
        return copy

    def trim(self, start, end):
        """Trim the list from start to end."""
        self.ltrim(start, end)

    def __iter__(self):
        return self.members.__iter__()

    def __repr__(self):
        return "<%s '%s' %s>" % (self.__class__.__name__, self.key,
                self.members)

    DELEGATEABLE_METHODS = ('lrange', 'lpush', 'rpush', 'llen',
            'ltrim', 'lindex', 'lset', 'lpop', 'lrem', 'rpop', 'rpoplpush')

class TypedList(object):
    """Create a container to store a list of objects in Redis.

    Arguments:
        key -- the Redis key this container is stored at
        target_type -- can be a Python object or a redisco model class.

    Optional Arguments:
        type_args -- additional args to pass to type constructor (tuple)
        type_kwargs -- additional kwargs to pass to type constructor (dict)

    If target_type is not a redisco model class, the target_type should
    also a callable that casts the (string) value of a list element into
    target_type. E.g. str, unicode, int, float -- using this format:

        target_type(string_val_of_list_elem, *type_args, **type_kwargs)

    target_type also accepts a string that refers to a redisco model.
    """

    def __init__(self, key, target_type, type_args=[], type_kwargs={}, **kwargs):
        self.list = List(key, **kwargs)
        self.klass = self.value_type(target_type)
        self._klass_args = type_args
        self._klass_kwargs = type_kwargs
        from models.base import Model
        self._redisco_model = issubclass(self.klass, Model)

    def value_type(self, target_type):
        if isinstance(target_type, basestring):
            t = target_type
            from models.base import get_model_from_key
            target_type = get_model_from_key(target_type)
            if target_type is None:
                raise ValueError("Unknown Redisco class %s" % t)
        return target_type

    def typecast_item(self, value):
        if self._redisco_model:
            return self.klass.objects.get_by_id(value)
        else:
            return self.klass(value, *self._klass_args, **self._klass_kwargs)

    def typecast_iter(self, values):
        if self._redisco_model:
            return filter(lambda o: o is not None, [self.klass.objects.get_by_id(v) for v in values])
        else:
            return [self.klass(v, *self._klass_args, **self._klass_kwargs) for v in values]

    def all(self):
        """Returns all items in the list."""
        return self.typecast_iter(self.list.all())

    def __len__(self):
        return len(self.list)

    def __getitem__(self, index):
        val = self.list[index]
        if isinstance(index, slice):
            return self.typecast_iter(val)
        else:
            return self.typecast_item(val)

    def typecast_stor(self, value):
        if self._redisco_model:
            return value.id
        else:
            return value

    def append(self, value):
        self.list.append(self.typecast_stor(value))

    def extend(self, iter):
        self.list.extend(map(lambda i: self.typecast_stor(i), iter))

    def __setitem__(self, index, value):
        self.list[index] = self.typecast_stor(value)

    def __iter__(self):
        for i in xrange(len(self.list)):
            yield self[i]

    def __repr__(self):
        return repr(self.typecast_iter(self.list))

class SortedSet(Container):
    """
    This class represents a SortedSet in redis.
    Use it if you want to arrange your set in any order.

    """

    def add(self, member, score):
        """Adds member to the set."""
        self.zadd(member, score)

    def remove(self, member):
        """Removes member from set."""
        self.zrem(member)

    def incr_by(self, member, increment):
        """Increments the member by increment."""
        self.zincrby(member, increment)

    def rank(self, member):
        """Return the rank (the index) of the element."""
        return self.zrank(member)

    def revrank(self, member):
        """Return the rank of the member in reverse order."""
        return self.zrevrank(member)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.zrange(index.start, index.stop)
        else:
            return self.zrange(index, index)[0]

    def score(self, member):
        """
        Returns the score of member.
        """
        return self.zscore(member)

    def __len__(self):
        return self.zcard()

    def __contains__(self, val):
        return self.zscore(val) is not None

    @property
    def members(self):
        """
        Returns the members of the set.
        """
        return self.zrange(0, -1)

    @property
    def revmembers(self):
        """
        Returns the members of the set in reverse.
        """
        return self.zrevrange(0, -1)

    def __iter__(self):
        return self.members.__iter__()

    def __reversed__(self):
        return self.revmembers.__iter__()

    def __repr__(self):
        return "<%s '%s' %s>" % (self.__class__.__name__, self.key,
                self.members)

    @property
    def _min_score(self):
        """
        Returns the minimum score in the SortedSet.
        """
        try:
            return self.zscore(self.__getitem__(0))
        except IndexError:
            return None

    @property
    def _max_score(self):
        """
        Returns the maximum score in the SortedSet.
        """
        try:
            # fix bug: bai jin ping, 2016/6/1
            # self.zscore(self.__getitem__(-1))
            return self.zscore(self.__getitem__(-1))
        except IndexError:
            return None

    def lt(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", "(%f" % v,
                start=offset, num=limit)

    def le(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than or equal to v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", v,
                start=offset, num=limit)

    def gt(self, v, limit=None, offset=None):
        """Returns the list of the members of the set that have scores
        greater than v.
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("(%f" % v, "+inf",
                start=offset, num=limit)

    def ge(self, v, limit=None, offset=None):
        """Returns the list of the members of the set that have scores
        greater than or equal to v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("(%f" % v, "+inf",
                start=offset, num=limit)

    def between(self, min, max, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        between min and max.

        .. Note::
            The min and max are inclusive when comparing the values.

        :param min: the minimum score to compare to.
        :param max: the maximum score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        >>> s = SortedSet("foo")
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.add('c', 30)
        1
        >>> s.between(20, 30)
        ['b', 'c']
        >>> s.clear()
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(min, max,
                start=offset, num=limit)

    def eq(self, value):
        """
        Returns the elements that have ``value`` for score.
        """
        return self.zrangebyscore(value, value)

    DELEGATEABLE_METHODS = ('zadd', 'zrem', 'zincrby', 'zrank',
            'zrevrank', 'zrange', 'zrevrange', 'zrangebyscore', 'zcard',
            'zscore', 'zremrangebyrank', 'zremrangebyscore')


class NonPersistentList(object):
    def __init__(self, l):
        self._list = l

    @property
    def members(self):
        return self._list

    def __iter__(self):
        return iter(self.members)

    def __len__(self):
        return len(self._list)


class Hash(Container, collections.MutableMapping):

    def __getitem__(self, att):
        return self.hget(att)

    def __setitem__(self, att, val):
        self.hset(att, val)

    def __delitem__(self, att):
        self.hdel(att)

    def __len__(self):
        return self.hlen()

    def __iter__(self):
        return self.hgetall().__iter__()

    def __contains__(self, att):
        return self.hexists(att)

    def __repr__(self):
        return "<%s '%s' %s>" % (self.__class__.__name__,
                                 self.key, self.hgetall())

    def keys(self):
        return self.hkeys()

    def values(self):
        return self.hvals()

    def _get_dict(self):
        return self.hgetall()

    def _set_dict(self, new_dict):
        self.clear()
        self.update(new_dict)

    dict = property(_get_dict, _set_dict)


    DELEGATEABLE_METHODS = ('hlen', 'hset', 'hdel', 'hkeys',
            'hgetall', 'hvals', 'hget', 'hexists', 'hincrby',
            'hmget', 'hmset')
