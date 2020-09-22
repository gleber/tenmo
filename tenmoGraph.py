import sys
from tenmoTypes import *

def trim(s, d=25, f=5):
    if s is None:
        return '#unknown#'
    l = len(s)
    if l <= d:
        return s
    x = s[0:f] + '...' + s[-d+f+3:]
    assert len(x) == d
    return x

def universe_print_dot(u, f=None):
    if f is None:
        p = lambda s: print(s, file=sys.stdout)
    else:
        p = lambda s: f.write(('%s\n' % (s,)).encode('utf-8'))
    p('digraph u {')
    p('node [style=filled];')
    for eid, ex in u.executions.items():
        p('"%s" [id="%s" label="%s" shape=rectangle fillcolor="#FFD9B2"]' % (eid, eid, trim(ex.description)))

    for enid, en in u.entities.items():
        p('subgraph "cluster_%s" {' % (en.entity_id))
        p('id="%s";' % (en.entity_id,))
        p('style=dotted;')
        p('fontsize=7;')
        p('label="%s";' % (trim(en.description, d=50),))
        for inc_id in en.incarnations:
            p('"%s";' % inc_id)
        # p((' -> '.join(map((lambda x: ('"%s"') % x), sorted(en.incarnations)))) + ' [color=red weight=100];')
        p('}')

    for iid, inc in u.incarnations.items():
        p('"%s" [id="%s" fillcolor="#B2FFB2" label="%s" style="dotted, filled" shape=diamond];' % (inc.incarnation_id, inc.incarnation_id, trim(inc.description)))
        if inc.parent_id is not None:
            p('"%s" -> "%s" [penwidth=0.3 arrowsize=.5 weight=22];' % (inc.parent_id, inc.incarnation_id))


    for eid, ex in u.executions.items():
        if ex.parent_id:
            p('"%s" -> "%s" [weight=25];' % (ex.parent_id or 'root', ex.execution_id))
        if ex.creator_id:
            p('"%s" -> "%s" [style=dotted weight=20];' % (ex.creator_id, ex.execution_id))


    for oid, op in u.operations.items():
        if op.op_type == 'r':
            a, b = op.incarnation_id, op.execution_id
            p('"%s" -> "%s" [style=dashed weight=10];' % (a, b))
        else:
            b, a = op.incarnation_id, op.execution_id
            p('"%s" -> "%s" [style=dashed weight=15];' % (a, b))


    for inid, inter in u.interactions.items():
        p('subgraph "cluster_interaction_%s" {' % (inter.interaction_id))
        p('id="%s";' % (inter.interaction_id,))
        p('style=dotted;')
        p('fontsize=7;')
        p('label="%s";' % (trim(inter.description, d=50),))
        for msg_id in inter.messages:
            p('"%s" [label="" shape=circle fixedsize=true width=0.2 height=0.2 fillcolor="#B2B2FF"];' % msg_id)
        # p((' -> '.join(map((lambda x: ('"%s"') % x), sorted(en.incarnations)))) + ' [color=red weight=100];')
        p('}')

    for msg_id, msg in u.messages.items():
        p('"%s" -> "%s" -> "%s" [weight=5 style=dotted penwidth=0.5 arrowsize=.5];' % (msg.sender, msg.message_id, msg.target))

    for ass in u.asserts:
        p('"%s" -> "%s" [weight=5 label="%s" style=dashed penwidth=0.5 arrowsize=.5 labelfontsize=10 color=red];' % (ass.source, ass.target, trim(ass.comment)))

    p('}')
