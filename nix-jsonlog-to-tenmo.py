#! /usr/bin/env nix-shell
#! nix-shell -i python3 -p "python3.withPackages(ps: [ps.numpy ps.psycopg2 ps.requests])"

import sys
sys.path.insert(0, '/home/gleber/code/tenmo/')

import os
import datetime
import json
import fileinput
import collections
import ulid
import pprint
from tenmoTypes import *
import tenmoGraph
import tenmoPg
import hashlib

src = sys.argv[1]

observedActions = collections.defaultdict(list)

tenmoEvents = collections.deque()

for line in fileinput.input(src):
    if not line[18:22] == "@nix":
        continue
    ts = datetime.datetime.fromtimestamp(float(line[0:17]))
    jsonStr = line[23:]
    act = json.loads(jsonStr)
    act['__ts'] = ts
    pprint.pprint(act)
    if 'id' in act:
        observedActions[act['id']].append(act)
    if act['action'] == 'start':
        # print(act)
        par = act.get('parent', None)
        if par is not None:
            par = str(par)
        eb = EventExecutionBegins(
            event_ulid = ulid.ulid(),
            timestamp = ts,
            execution_id = str(act['id']),
            parent_id = par,
            description = act.get('text', None),
        )
        tenmoEvents.append(eb)
    elif act['action'] == 'stop':
        ee = EventExecutionEnds(
                event_ulid = ulid.ulid(),
                timestamp = ts,
                execution_id = str(act['id']),
            )
        tenmoEvents.append(        ee)
        # print(ee)
        # print(act)
    elif act['action'] == 'result':
        if act['type'] == 108: # resConsumed
            nixNso = act['fields'][0]
            ul = ulid.ulid()
            tenmoEvents.append(
                EventOperation(
                    event_ulid = ul,
                    operation_id = "%d-%s" % (act['id'], ul),
                    timestamp = ts,
                    execution_id = str(act['id']),
                    type = 'r',
                    incarnation_id = 'i://%s' % (nixNso,),
                    # incarnation_id = 'i://%s@%d' % (nixNso, datetime.datetime.timestamp(ts)*1000000000),
                    incarnation_description = 'NSO %s' % (nixNso, ),
                    entity_id = 'e://%s' % (nixNso, ),
                    entity_description = 'NSO %s' % (nixNso, ),
                )
            )
        elif act['type'] == 109: # resProduced
            nixNso = act['fields'][0]
            ul = ulid.ulid()
            tenmoEvents.append(
                EventOperation(
                    event_ulid = ul,
                    operation_id = "%d-%s" % (act['id'], ul),
                    timestamp = ts,
                    execution_id = str(act['id']),
                    type = 'w',
                    incarnation_id = 'i://%s' % (nixNso),
                    # incarnation_id = 'i://%s:%d' % (nixNso, datetime.datetime.timestamp(ts)*1000000000),
                    incarnation_description = 'NSO %s' % (nixNso,),
                    entity_id = 'e://%s' % (nixNso, ),
                    entity_description = 'NSO %s' % (nixNso, ),
                )
            )
            # print(act)

# print(observedActions)
# pprint.pprint(observedActions)

# pprint.pprint(tenmoEvents)

tenmoPg.send(tenmoEvents, os.environ['TENMO_PGURI'])

# universe = observe(tenmoEvents)
# pprint.pprint(universe)

# tenmoGraph.universe_print_dot(universe)
