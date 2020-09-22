-- Migration script in pure SQL.
-- This script is ran at server boot-up.
--

\set on_error_stop true

-- Migration temporary sequence.
--
create temporary sequence migration_steps;

-- Migrations table.
--
create table if not exists migrations (
	id integer primary key,
	created_at timestamptz not null default now()
);

-- Migration procedure.
--
drop procedure if exists migrate;

create procedure migrate(migration text) as $$
	declare
		step numeric := nextval('migration_steps');
	begin
		if exists(select 1 from migrations where id = step) then
			raise notice 'migrations: skipping step %', step;
		else
			raise notice 'migrations: running step %', step;
			execute migration;
			insert into migrations (id) values (step);
		end if;
	end;
$$ language plpgsql;

-- Migration steps.
--

-- 1
call migrate(
  $$
	create table events (
		ulid char(26) primary key,
    status char(1) not null default 'i', -- 'i'nitial, 'c'laimed, 'p'rocessed
    attempts integer not null default 0,
		stored_at timestamptz not null default now(),
		modified timestamptz not null default now(),
    created_at timestamptz not null,
    event_type text not null,
		payload jsonb not null
  );
  $$
);

-- 2
call migrate(
  $$
  create table entities (
    entity_id text primary key,
    stored_at timestamptz not null default now(),
    description text
  );
  create table processes (
    process_id text primary key,
    stored_at timestamptz not null default now(),
    description text
  );
  create table executions (
    execution_id text primary key,
    stored_at timestamptz not null default now(),
    begin_timestamp timestamptz not null,
    parent_id text references executions (execution_id),
    creator_id text references executions (execution_id),
    process_id text references processes (process_id),
    description text default '',
    end_timestamp timestamptz
  );
  create table incarnations (
    incarnation_id text primary key,
    stored_at timestamptz not null default now(),
    entity_id text references entities (entity_id),
    creator_id text references executions (execution_id),
    parent_id text references incarnations (incarnation_id),
    description text default ''
  );
  create table operations (
    operation_id text primary key,
    stored_at timestamptz not null default now(),
    ts timestamptz not null,
    execution_id text references executions (execution_id),
    op_type char(1), -- 'w' or 'r'
    entity_id text not null references entities (entity_id),
    incarnation_id text not null references incarnations (incarnation_id),
    entity_description text default '',
    incarnation_description text default ''
  );
  $$
);

-- 3
call migrate(
  $migrate$
  CREATE OR REPLACE FUNCTION notify_events_changes()
  RETURNS trigger AS $$
  BEGIN
  PERFORM pg_notify(
    'events_changed',
    json_build_object(
      'operation', TG_OP,
      'record', row_to_json(NEW)
    )::text
  );
  RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER events_changed
  AFTER INSERT OR UPDATE
  ON events
  FOR EACH ROW
  EXECUTE PROCEDURE notify_events_changes()

  $migrate$
);

-- 4
call migrate(
  $migrate$
  CREATE OR REPLACE FUNCTION update_modified_column()
  RETURNS TRIGGER AS $$
  BEGIN
    NEW.modified = now();
    RETURN NEW;
  END;
  $$ language 'plpgsql';

  CREATE TRIGGER update_events_modtime BEFORE UPDATE ON events FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
  $migrate$
);

-- 5
call migrate(
  $migrate$
  create table annotations (
    annotation_id text primary key,
    stored_at timestamptz not null default now(),
    execution_id text references executions (execution_id),
    ts timestamptz not null,
		payload jsonb not null
  );

  create table interactions(
    interaction_id text primary key,
    stored_at timestamptz not null default now(),
    ts timestamptz not null,
    initiator_participant text references executions (execution_id),
    responder_participant text references executions (execution_id),
    description text default ''
  );

  create table messages (
    message_id text primary key,
    stored_at timestamptz not null default now(),
    interaction_id text not null references interactions (interaction_id),
    ts timestamptz not null,
    sender text not null references executions (execution_id),
    target text not null references executions (execution_id),
    incarnations_ids text[] DEFAULT array[]::text[],
		payload jsonb default '{}'::jsonb
  );

  create table asserts(
    source text not null,
    target text not null,
    comment text default '',
    primary key (source, target)
  );

  $migrate$
);

-- 6
call migrate(
  $migrate$
  create table graph (
    source text not null,
    verb text not null,
    target text not null,
    tags text[] default array[]::text[],
    primary key (source, target, verb)
  );

  CREATE OR REPLACE FUNCTION get_shortest_path(start text, destination text, depth_limit integer)
  RETURNS TABLE(depth integer, path text[], verbs text[]) AS $$
  BEGIN
  RETURN QUERY
  WITH RECURSIVE search_step(id, link, verb, depth, route, verbs, cycle) AS (
    SELECT r.source, r.target, r.verb, 1,
           ARRAY[r.source],
           ARRAY[r.verb]::text[],
           false
      FROM graph r where r.source=start

     UNION ALL

    SELECT r.source, r.target, r.verb, sp.depth+1,
           sp.route || r.source,
           sp.verbs || r.verb,
           r.source = ANY(route)
      FROM graph r, search_step sp
     WHERE r.source = sp.link AND NOT cycle AND sp.depth < depth_limit
  )
  SELECT sp.depth, (sp.route || destination) AS route, array_append(sp.verbs, '<destination>') as verbs
  FROM search_step AS sp
  WHERE link = destination AND NOT cycle AND NOT (destination = ANY(sp.route))
  ORDER BY depth ASC;

  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION get_shortest_path(start text, destination text)
  RETURNS TABLE(depth integer, path text[], verbs text[]) AS $$
  BEGIN
  RETURN QUERY
  SELECT * FROM get_shortest_path(start, destination, 100);
  END;
  $$ LANGUAGE plpgsql;

  create type triple as (source text, verb text, target text);

  CREATE OR REPLACE PROCEDURE populate_graph()
  LANGUAGE SQL
  AS $$

  insert into graph (source, verb, target)
  select incarnation_id, 'read_by', execution_id from operations where op_type = 'r'
  union all
  select execution_id, 'reads', incarnation_id from operations where op_type = 'r'
  union all
  select execution_id, 'writes', incarnation_id from operations where op_type = 'w'
  union all
  select incarnation_id, 'written_by', execution_id from operations where op_type = 'w'
  on conflict do nothing;

  insert into graph (source, verb, target)
  select execution_id, 'child_of', parent_id  from executions where parent_id is not null
  union all
  select parent_id, 'parent_of', execution_id  from executions where parent_id is not null
  union all
  select execution_id, 'created_by', creator_id  from executions where creator_id is not null
  union all
  select creator_id, 'creator_of', execution_id  from executions where creator_id is not null
  on conflict do nothing;

  insert into graph (source, verb, target)
  select incarnation_id, 'instance_of', entity_id from incarnations where entity_id is not null
  union all
  select entity_id, 'entity_of', incarnation_id from incarnations where entity_id is not null
  union all
  select incarnation_id, 'part_of', parent_id from incarnations where parent_id is not null
  union all
  select parent_id, 'divides_into', incarnation_id from incarnations where parent_id is not null
  on conflict do nothing;

  insert into graph (source, verb, target)
  select sender, 'sent_to', target from messages
  union all
  select target, 'received_from', sender from messages
  on conflict do nothing;

  insert into graph (source, verb, target)
  select t.source, 'assert', t.target from asserts t
  union all
  select t.target, 'assert_reverse', t.source from asserts t
  on conflict do nothing;

  insert into graph (source, verb, target)
  select ((unnest(ARRAY[(incarnation_id, 'after', prev_incarnation_id)::triple, (prev_incarnation_id, 'before', incarnation_id)::triple]))).* from (
    select tt.entity_id, tt.incarnation_id, LAG(tt.incarnation_id, 1) OVER (partition by entity_id order by incarnation_id) prev_incarnation_id from (
      select entity_id, unnest(array_agg(t.incarnation_id order by t.incarnation_id)) as incarnation_id from incarnations t group by entity_id having array_length(array_agg(t.incarnation_id order by t.incarnation_id), 1) > 1) as tt)
      as ttt
      where ttt.prev_incarnation_id is not null
      ON CONFLICT DO NOTHING;

  $$;

  CALL populate_graph();

  $migrate$
);


-- 7
call migrate(
  $migrate$

  CREATE OR REPLACE FUNCTION get_all_paths_from(start text, depth_limit integer)
  RETURNS TABLE(depth integer, verbs text[], path text[]) AS $$
  BEGIN
  RETURN QUERY

  WITH RECURSIVE search_step(id, link, verb, depth, route, verbs, cycle) AS (
    SELECT r.source, r.target, r.verb, 1,
           ARRAY[r.source],
           ARRAY[r.verb]::text[],
           false
      FROM graph r where r.source=start

     UNION ALL

    SELECT r.source, r.target, r.verb, sp.depth+1,
           sp.route || r.source,
           sp.verbs || r.verb,
           r.source = ANY(route)
      FROM graph r, search_step sp
     WHERE r.source = sp.link AND NOT cycle AND sp.depth < depth_limit
  )
  SELECT sp.depth, array_append(sp.verbs, '<end>') AS verbs, sp.route || sp.link AS path
  FROM search_step AS sp
  WHERE NOT cycle
  ORDER BY depth ASC;

  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION get_all_paths_from(start text)
  RETURNS TABLE(depth integer, verbs text[], path text[]) AS $$
  BEGIN
  RETURN QUERY
  SELECT * FROM get_all_paths_from(start, 100);
  END;
  $$ LANGUAGE plpgsql;

  $migrate$
);


-- 8
call migrate(
  $migrate$


  CREATE OR REPLACE FUNCTION get_all_paths_from_by_verbs(start text, crawl_verbs text[], depth_limit integer)
  RETURNS TABLE(depth integer, verbs text[], path text[]) AS $$
  BEGIN
  RETURN QUERY

  WITH RECURSIVE search_step(id, link, verb, depth, route, verbs, cycle) AS (
    SELECT r.source, r.target, r.verb, 1,
           ARRAY[r.source],
           ARRAY[r.verb]::text[],
           false
      FROM graph r where r.source=start and r.verb = ANY(crawl_verbs)

     UNION ALL

    SELECT r.source, r.target, r.verb, sp.depth+1,
           sp.route || r.source,
           sp.verbs || r.verb,
           r.source = ANY(route)
      FROM graph r, search_step sp
     WHERE r.source = sp.link AND NOT cycle and r.verb = ANY(crawl_verbs) AND sp.depth < depth_limit
  )
  SELECT sp.depth, array_append(sp.verbs, '<end>') AS verbs, sp.route || sp.link AS path
  FROM search_step AS sp
  WHERE NOT cycle
  ORDER BY depth ASC;

  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION get_all_paths_from_by_verbs(start text, crawl_verbs text[])
  RETURNS TABLE(depth integer, verbs text[], path text[]) AS $$
  BEGIN
  RETURN QUERY
  SELECT * FROM get_all_paths_from_by_verbs(start, crawl_verbs, 100);
  END;
  $$ LANGUAGE plpgsql;

  $migrate$
);


-- 9
call migrate(
  $migrate$

  create or replace function get_closure_from(start text, depth_limit integer)
  returns table(depth integer, obj text) as $$
  begin
  return query
  select t.depth, t.path[array_upper(t.path,1)] from get_all_paths_from(start, depth_limit) as t;
  end;
  $$ language plpgsql;

  create or replace function get_closure_from(start text)
  returns table(depth integer, obj text) as $$
  begin
  return query
  select * from get_closure_from(start, 100);
  end;
  $$ language plpgsql;

  create or replace function get_closure_from_filtered(start text, filter_verbs text[], depth_limit integer)
  returns table(depth integer, obj text) as $$
  begin
  return query
  select t.depth, t.path[array_upper(t.path,1)] from get_all_paths_from(start, depth_limit) as t where t.verbs[array_upper(t.verbs,1)-1] = ANY(filter_verbs);
  end;
  $$ language plpgsql;

  create or replace function get_closure_from_filtered(start text, filter_verbs text[])
  returns table(depth integer, obj text) as $$
  begin
  return query
  select * from get_closure_from_filtered(start, filter_verbs, 100);
  end;
  $$ language plpgsql;

  CREATE OR REPLACE FUNCTION get_closure_from_by_verbs(start text, crawl_verbs text[], depth_limit integer)
  RETURNS TABLE(depth integer, obj text) AS $$
  BEGIN
  RETURN QUERY
  select t.depth, t.path[array_upper(t.path,1)] from get_all_paths_from_by_verbs(start, crawl_verbs, depth_limit) as t;
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION get_closure_from_by_verbs(start text, crawl_verbs text[])
  RETURNS TABLE(depth integer, obj text) AS $$
  BEGIN
  RETURN QUERY
  SELECT * FROM get_closure_from_by_verbs(start, crawl_verbs, 100);
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION get_closure_from_by_verbs_filtered(start text, crawl_verbs text[], filter_verbs text[], depth_limit integer)
  RETURNS TABLE(depth integer, obj text) AS $$
  BEGIN
  RETURN QUERY
  select t.depth, t.path[array_upper(t.path,1)] from get_all_paths_from_by_verbs(start, crawl_verbs, depth_limit) as t where t.verbs[array_upper(t.verbs,1)-1] = ANY(filter_verbs);
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION get_closure_from_by_verbs_filtered(start text, crawl_verbs text[], filter_verbs text[])
  RETURNS TABLE(depth integer, obj text) AS $$
  BEGIN
  RETURN QUERY
  SELECT * FROM get_closure_from_by_verbs_filtered(start, crawl_verbs, filter_verbs, 100);
  END;
  $$ LANGUAGE plpgsql;


  $migrate$
);


-- 10
call migrate(
 $migrate$


 CREATE OR REPLACE FUNCTION provenance_set(start text)
 RETURNS TABLE(depth integer, obj text) AS $$
 BEGIN
 RETURN QUERY
 select * from get_closure_from_by_verbs_filtered(start, ARRAY['written_by','reads']::text[], '{reads}'::text[], 2) as t where t.depth <= 2;
 END;
 $$ LANGUAGE plpgsql;

 CREATE OR REPLACE FUNCTION provenance_set_indirect(start text, depth_limit integer)
 RETURNS TABLE(depth integer, obj text) AS $$
 BEGIN
 RETURN QUERY
 select * from get_closure_from_by_verbs_filtered(start, ARRAY['written_by','reads']::text[], ARRAY['reads']::text[], depth_limit) as t;
 END;
 $$ LANGUAGE plpgsql;

 CREATE OR REPLACE FUNCTION provenance_set_indirect(start text)
 RETURNS TABLE(depth integer, obj text) AS $$
 BEGIN
 RETURN QUERY
 SELECT * FROM provenance_set_indirect(start, 100);
 END;
 $$ LANGUAGE plpgsql;

$migrate$
);


-- 10
call migrate(
  $migrate$

  CREATE OR REPLACE FUNCTION trace(start text, depth_limit integer)
  RETURNS TABLE(depth integer, obj text) AS $$
  BEGIN
  RETURN QUERY
  select * from get_closure_from_by_verbs(start, ARRAY['child_of']::text[], depth_limit) as t;
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION trace(start text)
  RETURNS TABLE(depth integer, obj text) AS $$
  BEGIN
  RETURN QUERY
  SELECT * FROM trace(start, 100);
  END;
  $$ LANGUAGE plpgsql;

  $migrate$
);


-- 11
call migrate(
 $migrate$

 select 1;

 $migrate$
);


-- 12
call migrate(
  $migrate$

  create or replace procedure assert(sourcein text, targetin text, comment text)
  language sql
  as $$

  insert into asserts (source, target, comment)
  select sourcein, targetin, comment
  on conflict do nothing;

  call populate_graph();

  $$;

  CREATE OR REPLACE PROCEDURE assert(sourceIn text, targetIn text)
  LANGUAGE SQL
  AS $$

  call assert(sourceIn, targetIn, '');

  $$;

  $migrate$
);




-- N
-- call migrate(
--  $migrate$
--    ...
--  $migrate$
-- );
