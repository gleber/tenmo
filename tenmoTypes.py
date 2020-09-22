#!/usr/bin/env python3

from typing import List, Union, Sequence

import datetime
import json
import sys
import fileinput
import collections
import ulid
import pprint

strict = False

EventExecutionBegins = collections.namedtuple(
    'EventExecutionBegins',
    ['event_ulid',
     'timestamp',
     'execution_id',
     'parent_id',
     'creator_id',
     'process_id',
     'description'], defaults=([None]*4))
EventExecutionEnds = collections.namedtuple(
    'EventExecutionEnds',
    ['event_ulid',
     'timestamp',
     'execution_id'])
EventOperation = collections.namedtuple(
    'EventOperation',
    ['event_ulid',
     'operation_id',
     'timestamp',
     'execution_id',
     'type',
     'entity_id',
     'incarnation_id',
     'entity_description',
     'incarnation_description'], defaults=[None]*2)
EventMessage = collections.namedtuple(
    'EventMessage',
    ['event_ulid',
     'message_id',
     'timestamp',
     'interaction_id',
     'sender',
     'target',
     'payload',
     'incarnations_ids',
     'interaction_description',
     ], defaults=[None, None])
Event = Union[EventExecutionBegins, EventExecutionEnds, EventOperation, EventMessage]

Execution = collections.namedtuple(
    'Execution',
    ['execution_id',
     'begin_timestamp',
     'parent_id',
     'creator_id',
     'process_id',
     'description',
     'end_timestamp'], defaults=[None])
Operation = collections.namedtuple(
    'Operation',
    ['operation_id',
     'ts',
     'execution_id',
     'op_type',
     'entity_id',
     'incarnation_id',
     'entity_description',
     'incarnation_description'], defaults=[None]*2)
Entity = collections.namedtuple(
    'Entity',
    ['entity_id',
     'incarnations',
     'description'], defaults=[None, None])
Incarnation = collections.namedtuple(
    'Incarnation',
    ['incarnation_id',
     'entity_id',
     'parent_id',
     'creator_id', 'description'])

Interaction = collections.namedtuple(
    'Interaction',
    ['interaction_id',
     'ts',
     'initiator_participant',
     'responder_participant',
     'messages',
     'description'], defaults=[None, None])
Message = collections.namedtuple(
    'Message',
    ['message_id',
     'interaction_id',
     'ts',
     'sender',
     'target',
     'incarnations_ids',
     'payload'], defaults=[None, None])
Assert = collections.namedtuple(
    'Assert',
    ['source',
     'target',
     'comment'], defaults=[None])


Universe = collections.namedtuple('Universe', ['executions', 'operations', 'incarnations', 'entities', 'processes', 'interactions', 'messages', 'asserts'])


def observe(events : Sequence[Event]) -> Universe:
    global strict
    executions = dict()
    operations = dict()
    incarnations = dict()
    entities = dict()
    processes = dict()
    interactions = dict()
    messages = dict()
    asserts = dict()
    u = Universe(executions=executions, operations=operations, incarnations=incarnations, entities=entities, processes=processes, interactions=interactions, messages=messages, asserts=asserts)
    for e in events:
        if isinstance(e, EventExecutionBegins):
            executions[e.execution_id] = Execution(
                execution_id=e.execution_id,
                begin_timestamp=e.timestamp,
                parent_id=e.parent_id,
                creator_id=e.creator_id,
                process_id=e.process_id,
                description=e.description,
                end_timestamp=None,
            )
        elif isinstance(e, EventExecutionEnds):
            ex = executions[e.execution_id]
            executions[ex.execution_id] = ex._replace(end_timestamp = e.timestamp)

    for e in events:
        if isinstance(e, EventOperation):
            operations[e.event_ulid] = Operation(**dict(e._asdict()))
            if (strict and (e.type == 'w' and e.incarnation_id not in incarnations)) or (not strict and (e.type in ['w', 'r'])):
                entities[e.entity_id] = entities.get(e.entity_id, Entity(entity_id=e.entity_id, description=e.entity_description, incarnations=[]))
                incarnations[e.incarnation_id] = Incarnation(incarnation_id=e.incarnation_id, entity_id=e.entity_id, creator_id=e.execution_id, description=e.incarnation_description)
                entities[e.entity_id].incarnations.append(e.incarnation_id)

    if strict:
        for e in events:
            if isinstance(e, EventOperation):
                operations[e.event_ulid] = e
                i = incarnations.get(e.incarnation_id, None)
                if e.type == 'r':
                    assert i is not None

    return u
