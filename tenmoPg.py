#! /usr/bin/env nix-shell
#! nix-shell -i python3 -p "python3.withPackages(ps: [ps.numpy ps.psycopg2 ps.requests ps.websockets])"

import sys
import threading
from tenmoTypes import *
from tenmoGraph import universe_print_dot
import select
import time
import datetime
import pprint
import traceback

import io
import json
import psycopg2
import psycopg2.extensions
from psycopg2.extras import Json, DictCursor, RealDictCursor



def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

class PgJson(Json):
    def dumps(self, o):
        return json.dumps(o, default=json_default)

conn = None
def getPgConn(pgUri: str):
    global conn
    if conn is None:
        conn = psycopg2.connect(pgUri, cursor_factory=RealDictCursor)
    return conn

def send(events : Sequence[Event], pgUri: str):
    conn = getPgConn(pgUri)
    for e in events:
        event_type = type(e).__name__
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO events(ulid, created_at, event_type, payload) VALUES (%s, %s, %s, %s)",
                            [e.event_ulid, e.timestamp, event_type, PgJson(e._asdict())])

def listen(pgUri: str, cb):
    conn = getPgConn(pgUri)
    listenConn(conn, cb)

def listenConn(conn, cb):
    curs = conn.cursor()
    curs.execute("LISTEN events_changed;")

    seconds_passed = 0
    while True:
        conn.commit()
        if select.select([conn],[],[],5) == ([],[],[]):
            seconds_passed += 5
            print("{} seconds passed without a notification...".format(seconds_passed))
        else:
            seconds_passed = 0
            conn.poll()
            conn.commit()
            while conn.notifies:
                notify = conn.notifies.pop()
                cb(notify, conn)

def print_notify(notify, conn):
    print("Got NOTIFY:", datetime.datetime.now(), notify.pid, notify.channel, notify.payload)

def ensure_entity(conn, curs, event):
    p = event['payload']
    if 'entity_id' in p:
        curs.execute("INSERT INTO entities (entity_id, description) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                     [p['entity_id'], p.get('entity_description', '')])

def ensure_process(conn, curs, event):
    p = event['payload']
    if 'process_id' in p and p['process_id'] is not None:
        curs.execute("INSERT INTO process (process_id) VALUES (%s) ON CONFLICT DO NOTHING",
                     [p['process_id']])

def ensure_incarnation(conn, curs, event):
    p = event['payload']
    creator_id = p['execution_id']
    if p['type'] == 'r':
        creator_id = None
    curs.execute("""INSERT INTO incarnations AS old (incarnation_id, entity_id, parent_id, creator_id, description)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (incarnation_id)
                    DO UPDATE SET creator_id = COALESCE(old.creator_id, EXCLUDED.creator_id)""",
                 [p['incarnation_id'], p.get('entity_id', None), p.get('parent_id', None), creator_id, p.get('incarnation_description', None)])

def insert_execution(conn, curs, event):
    p = event['payload']
    curs.execute("""INSERT INTO executions AS old
                           (execution_id, begin_timestamp, parent_id, creator_id, process_id, description)
                    VALUES (%s,           %s,              %s,        %s,         %s,         %s)
                    ON CONFLICT DO NOTHING""",
                 [p['execution_id'],
                  p['timestamp'],
                  p.get('parent_id', None),
                  p.get('creator_id', None),
                  p.get('process_id', None),
                  p.get('description', '')])

def finish_execution(conn, curs, event):
    p = event['payload']
    with conn.cursor() as c:
        c.execute("""UPDATE executions AS old
                     SET end_timestamp = %s
                     WHERE execution_id = %s
                     RETURNING execution_id""",
                  [p['timestamp'], p['execution_id']])
        return c.rowcount == 1

def insert_operation(conn, curs, event):
    p = event['payload']
    curs.execute("""INSERT INTO operations AS old
                           ( operation_id
                           , ts
                           , execution_id
                           , op_type
                           , entity_id
                           , incarnation_id
                           , entity_description
                           , incarnation_description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""",
                 [p.get('operation_id', event['ulid'].strip()),
                  p.get('timestamp', event['created_at']),
                  p['execution_id'],
                  p['type'],
                  p.get('entity_id', None),
                  p['incarnation_id'],
                  p.get('entity_description', ''),
                  p.get('incarnation_description', '')])


def ensure_interaction(conn, curs, event):
    p = event['payload']
    curs.execute("""INSERT INTO interactions AS old
                           (interaction_id, ts, initiator_participant, responder_participant, description)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (interaction_id)
                    DO UPDATE
                        SET ts = COALESCE(old.ts, EXCLUDED.ts),
                            initiator_participant = COALESCE(old.initiator_participant, EXCLUDED.initiator_participant),
                            responder_participant = COALESCE(old.responder_participant, EXCLUDED.responder_participant),
                            description = COALESCE(old.description, EXCLUDED.description)""",
                 [p['interaction_id'],
                  p.get('timestamp', event['created_at']),
                  p['sender'],
                  p['target'],
                  p.get('interaction_description', None)])


def insert_message(conn, curs, event):
    p = event['payload']
    curs.execute("""INSERT INTO messages AS old
                           ( message_id
                           , interaction_id
                           , ts
                           , sender
                           , target
                           , payload
                           , incarnations_ids)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""",
                 [p.get('message_id', event['ulid'].strip()),
                  p['interaction_id'],
                  p.get('timestamp', event['created_at']),
                  p['sender'],
                  p['target'],
                  p.get('payload', None),
                  p.get('incarnations_ids', None)])


def process_one_event(conn, curs, event):
    print('process_one_event: ', event['ulid'])
    pprint.pprint(event)
    try:
        if event['event_type'] == 'EventExecutionBegins':
            ensure_process(conn, curs, event)
            insert_execution(conn, curs, event)
            return True

        elif event['event_type'] == 'EventExecutionEnds':
            return finish_execution(conn, curs, event)

        elif event['event_type'] == 'EventOperation':
            ensure_entity(conn, curs, event)
            ensure_incarnation(conn, curs, event)
            insert_operation(conn, curs, event)
            return True

        elif event['event_type'] == 'EventMessage':
            ensure_interaction(conn, curs, event)
            insert_message(conn, curs, event)
            return True
    except Exception as e:
        pprint.pprint(e)
        print(traceback.format_exc())
        return False

def process_events_batch(pgUri, signal):
    conn = psycopg2.connect(pgUri, cursor_factory=RealDictCursor)
    n = 0
    print('process_events_batch')
    while True:
        processed = 0
        with conn.cursor() as curs:
            print('select one to process')
            curs.execute("SELECT * FROM events WHERE status = 'i' AND attempts < 50 LIMIT 1 FOR UPDATE")
            for r in curs:
                print('got', r['ulid'])
                processed += 1
                with conn.cursor() as c:
                    c.execute("UPDATE events SET status = 'c', attempts = attempts + 1 WHERE ulid = %s", [r['ulid']])
                    print('claimed', c.rowcount)
                    conn.commit()
                with conn.cursor() as c:
                    if process_one_event(conn, c, r):
                        print('releasing')
                        c.execute("UPDATE events SET status = 'p' WHERE ulid = %s", [r['ulid']])
        conn.commit()
        print('comitted')

        if processed == 0:
            with conn.cursor() as curs:
                print('populating graph')
                curs.execute('call populate_graph()')
            conn.commit()

            signal.wait(30)
            signal.clear()
            continue

def clean_events(pgUri: str):
    """
    Repeatedly unclaims events which stay in claimed mode longer than 5 seconds.
    """
    conn = psycopg2.connect(pgUri, cursor_factory=RealDictCursor)
    while True:
        processed = 0
        with conn:
            with conn.cursor() as curs:
                curs.execute("UPDATE events SET status = 'i' WHERE status = 'c' AND (clock_timestamp() - modified) > interval '00:00:05'")
                processed = curs.rowcount

        if processed > 0:
            print('Un-claimed %d rows' % processed)
            continue

        time.sleep(5)


def process_events_forever(pgUri: str):
    conn = getPgConn(pgUri)
    curs = conn.cursor()
    curs.execute("LISTEN events_changed;")
    conn.commit()
    signal = threading.Event()
    worker = threading.Thread(target=process_events_batch, args=(pgUri,signal,))
    worker.start()
    cleaner = threading.Thread(target=clean_events, args=(pgUri,))
    cleaner.start()
    seconds_passed = 0
    while True:
        conn.commit()
        if select.select([conn],[],[],5) == ([],[],[]):
            seconds_passed += 5
            print("{} seconds passed without a notification...".format(seconds_passed))
        else:
            seconds_passed = 0
            conn.poll()
            conn.commit()
            while conn.notifies:
                print('Got notification')
                notify = conn.notifies.pop()
                signal.set()
                # cb(notify, conn)
    worker.join()
    cleaner.join()

def fromPgDict(r):
    d = dict(r)
    if 'stored_at' in d:
        del d['stored_at']
    return d

def entityFromPg(row):
    ent = Entity(**fromPgDict(row))
    return (row['entity_id'], ent._replace(incarnations = []))

def processFromPg(row):
    return (row['process_id'], Process(**fromPgDict(row)))

def incarnationFromPg(row):
    return (row['incarnation_id'], Incarnation(**fromPgDict(row)))

def operationFromPg(row):
    return (row['operation_id'], Operation(**fromPgDict(row)))

def executionFromPg(row):
    return (row['execution_id'], Execution(**fromPgDict(row)))

def interactionFromPg(row):
    inter = Interaction(**fromPgDict(row))
    return (row['interaction_id'], inter._replace(messages = []))

def messageFromPg(row):
    return (row['message_id'], Message(**fromPgDict(row)))

def assertFromPg(row):
    return Assert(**fromPgDict(row))

def load_universe(pgUri: str):
    conn = getPgConn(pgUri)
    with conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM executions")
            executions = dict( executionFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM incarnations")
            incarnations = dict( incarnationFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM operations")
            operations = dict( operationFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM processes")
            processes = dict( processFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM entities")
            entities = dict( entityFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM interactions")
            interactions = dict( interactionFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM messages")
            messages = dict( messageFromPg(r) for r in c )
        with conn.cursor() as c:
            c.execute("SELECT * FROM asserts")
            asserts = set( assertFromPg(r) for r in c )

        for iid, i in incarnations.items():
            entities[i.entity_id].incarnations.append(i.incarnation_id)
        for mid, m in messages.items():
            interactions[m.interaction_id].messages.append(m.message_id)
        u = Universe(executions=executions, operations=operations, incarnations=incarnations, entities=entities, processes=processes, interactions=interactions, messages=messages, asserts=asserts)
        # pprint.pprint(u)
        return u

def serve(pgUri):
    import tenmoServe

    def serveUniverse(pgUri):
        print('serving dot')
        output = io.BytesIO()
        u = load_universe(pgUri)
        universe_print_dot(u, output)
        return output.getvalue()

    tenmoServe.serve(pgUri, '/dot', serveUniverse)

if __name__ == "__main__":
    if sys.argv[2] == 'listen':
        listen(sys.argv[1], print_notify)
    elif sys.argv[2] == 'dot':
        universe_print_dot(load_universe(sys.argv[1]))
    elif sys.argv[2] == 'serve':
        serve(sys.argv[1])
    elif sys.argv[2] == 'process':
        process_events_forever(sys.argv[1])
