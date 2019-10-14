
drop table datoms;

create table datoms (
    e integer not null,
    a text not null,
    v jsonb not null
);


create table datoms (
    e jsonb not null,
    a jsonb not null,
    v jsonb not null,
    t jsonb not null
);



insert into datoms
    (e, a,                v)
values
    ('1', '"artist/name"',    '"Queen"'),
    ('1', '"artist/country"', '"GB"'),
    ('2', '"release/artist"', '1')
;


insert into datoms
    (e, a,                v)
values
    ('3', '"artist/name"',    '"Abba"'),
    ('3', '"artist/country"', '"USA"'),
    ('4', '"release/artist"', '3')
;



select
    d1.v, sum(d2.e::text::int)

from
    datoms as d1,
    datoms as d2

where
        d1.a = 'artist/name'
    and d2.a = 'release/artist'
    and d2.v = d1.e

group by d1.v

;


select
    d1.v, d2.e

from
    datoms as d1,
    datoms as d2

where
        d1.a = '"artist/name"'
    and d1.v IN ('"Queen"', '"Abba"')
    and d2.a = '"release/artist"'
    and d2.v = d1.e
;


select
    d1.v, d2.v

from
    __datoms as d1,
    __datoms as d2

where
        d1.a = '"artist/name"'
    and d1.v IN ('"Queen"', '"Abba"')
    and d2.a = '"release/artist"'
    and d2.v = d1.e
;


create table entities (
    id serial primary key,
    data jsonb
);


insert into entities (data)
values

    ('{"release/year": 1985, "release/artist": 4}')
;


insert into entities (id, data)
values

    (1, '{"artist/name": "Queen", "artist/tags": ["AAA", "BBB", "CCC"]}')
;


insert into entities (id, data)
values
    (1, '{"artist/name": "Queen", "artist/tags": []}')
;






select
    e1.id,
    e2.data->'release/year' as year

from
    entities as e1,
    entities as e2

where
        (e1.data->'artist/name')::text = 'Queen'
    and (e2.data->'release/artist')::text::integer = e1.id
;


select
    e1.id,
    (e2.data->>'release/year')::integer,
    jsonb_array_elements(e1.data->'artist/tags')

from
    entities as e1,
    entities as e2

where
        (e1.data->>'artist/name') = 'Queen'
    and (e2.data->>'release/artist')::integer = e1.id
    and (e2.data->>'release/year')::integer > 1970
;


select
    e1.id,
    (e2.data->>'release/year')::integer,
    jsonb_array_elements(e1.data->'artist/tags')

from
    entities as e1,
    entities as e2

where
        (e1.data->>'artist/name') = 'Queen'
    and (e2.data->>'release/artist')::integer = e1.id
    and (e2.data->>'release/year')::integer > 1970
;



select
    e1.id,
    jsonb_array_elements(e1.data->'artist/tags')


from
    entities as e1

where
        (e1.data->>'artist/name') = 'Queen'
;


select
    e1.id,
    e3.*

from
    entities as e1,

    (
        select
            e.id,
            jsonb_array_elements(e.data->'artist/tags')
        from entities e
    ) as e3

where
    e3.id = e1.id
;


select s.* from (values (1), (2), (3)) as s;

select s.a, s.b from (values (1, 'aa'), (2, 'cc'), (3, 'dd')) as s (a, b);


with
foo as (
    select * from (values (1, 'aa'), (2, 'cc'), (3, 'dd')) as v (a, b)
),
foo1 as
    (select * from foo
),
foo2 as (
    select * from foo
)
select * from foo1, foo2
;


with
foo as (
    select * from (values (1, 'aa'), (2, 'cc'), (3, 'dd')) as v (a, b)
)
select * from foo
;



SELECT array_agg(a ORDER BY b DESC) FROM table;

select s.a, s.b from (values (1, 'aa'), (2, 'cc'), (3, 'dd')) as s (a, b);


create table attributes (
    name text primary key
);

create table entities_ (
    id serial primary key
);

create table transactions (
    id serial primary key
);


create table datoms (
    id serial primary key,
    e integer not null references entities_(id),
    a text not null references attributes(name),
    v text not null,
    t integer not null references transactions(id)

);

insert into attributes values ('artist/name'), ('release/artist'), ('release/year');

insert into entities_ values (default), (default);

insert into transactions values (default), (default);

insert into datoms (e, a, v, t) values
    (1, 'artist/name', 'Queen', 1),
    (2, 'release/artist', '1', 1),
    (2, 'release/year', '1985', 1)
;


select
    d1.v::text,
    d3.v::integer

from
    datoms d1,
    datoms d2,
    datoms d3

where
        d1.a = 'artist/name'
    and d1.v::text = 'Queen'
    and d2.a = 'release/artist'
    and d2.v::integer = d1.e
    and d3.a = 'release/year'
    and d3.e = d2.e
;


from
    datoms d1,

    (
        select

            from
                datoms d2,
                datoms d3

        where

                d2.a = 'release/artist'
            and d2.v::integer = d1.e

            and d3.e = d2.e
            and d3.a = 'release/year'
            and d3.v::integer = 1970

    ) as sub

where
        d1.a = 'artist/name'

    and not d1.e = sub.e


    and d2.a = 'release/artist'
    and not d2.v::integer = d1.e

    and d3.e = d2.e
    and d3.a = 'release/year'
    and d3.v::integer = 1970



CREATE TYPE __t AS (type text, value text);


 select distinct d1.v from datoms2 d1, datoms2 d2 where d1.a = '"artist/name"' and d1.v = '"Queen"' and d2.v = d1.e and d2.a = '"release/artist"' and not exists(select * from datoms2 d3 where d3.e = d2.e and d3.a = '"release/year"' and d3.v = '1981');


select * from json_to_recordset('[{"a":1,"b":"foo"},{"a":"2","c": [1, 2, 3]}]') as (a jsonb, b jsonb, c jsonb) where c = '[1, 2, 3]'::jsonb;


select * from jsonb_array_elements('[1,true, [2,false]]') _ (a) where a = '1';


SELECT distinct d1.e, d3.v

FROM
    datoms d1, datoms d2, datoms d3, datoms d4

WHERE
        (d1.a = 'artist/name'
    AND d1.v = 'Queen'
    AND d2.a = 'release/artist'
    AND CAST(d2.v AS integer) = d1.e
    AND (
            (d3.e = d2.e
        AND d3.a = 'release/year'
        and d3.v::integer = 1985
        ) or
        (
            d4.e = d2.e
        AND d4.a = 'release/year'
        and d4.v::integer = 1984
    )
));



create table datoms3 (
    id serial primary key,
    e jsonb not null,
    a jsonb not null,
    v jsonb not null,
    t jsonb not null,
    time timestamp with time zone not null default current_timestamp
);


insert into datoms3 (e, a, v, t)
values
    ('1', '"artist/name"', '"Queen"', '1'),
    ('2', '"release/artist"', '1', '1'),
    ('2', '"release/year"', '1985', '1'),
    ('3', '"artist/name"', '"Beatles"', '1'),
    ('4', '"release/artist"', '3', '1'),
    ('4', '"release/year"', '1986', '1')
;


select distinct d3.*
from
datoms3 d1,
datoms3 d2,
datoms3 d3,
datoms3 d4

where
    d1.a = '"release/artist"'

and

(
   (d2.e = d1.e and d2.a = '"release/year"' and d2.v = '1989')
or

(d4.e = d1.e and d4.a = '"release/year"' and d4.v = '1980')



)

and d3.e = d1.v and d3.a = '"artist/name"'
;


create table datoms4 (
    id serial primary key,
    e integer not null,
    a text not null,
    v text not null,
    t integer not null
);

create table datoms5 (
    id serial primary key,
    e integer not null,
    a text not null,
    v text not null,
    t integer not null,
    unique (e,a)
);


insert into datoms4 (e, a, v, t)
values
    (1, 'artist/name', 'Queen', 1),
    (2, 'release/artist', '1', 1),
    (2, 'release/year', '1985', 1),
    (3, 'artist/name', 'Beatles', 1),
    (4, 'release/artist', '3', 1),
    (4, 'release/year', '1986', 1)
;


insert into datoms5 (e, a, v, t)
values (1, 'artist/name', 'Queen', 1);



SELECT
    d1.e AS f1, d1.v AS f2
FROM
    datoms4 d1, datoms4 d2, datoms4 d3
WHERE
    (d1.a = $2
AND d1.v = $3
AND d2.a = $4
AND CAST(d2.v AS integer) = d1.e
AND d3.e = d2.e
AND d3.a = $5

);

select
    e
from datoms4
where
    a = 'artist/name' and v = 'Queen'
;


select
    e
from datoms4
where
    a = 'release/artist' and v::integer in (1, 2, 3)
;


select
    e
from datoms4
where
    a = 'release/year' and v::integer = 1985 and e in (1, 2, 3)
;





with

_1 as (

select distinct
    e, a, v, t
from datoms4
where
    a = 'artist/name' and v = 'Abba'

),

_2 as (

select distinct
    e, a, v, t
from datoms4
where
    a = 'release/artist' and v::integer in (select e from _1)
),

_3 as (
select distinct
    e, a, v, t
from datoms4
where a = 'release/year' and v::integer = 1985
    and e in (select e from _2)
)


select distinct _1.v, _3.v
from _1, _3

;





select
    e, a, v, t
from datoms4 d1
where
    a = 'artist/name' and v = 'Queen'

union

select
    e, a, v, t
from datoms4
where
    a = 'release/artist' and v::integer in (select e from d1)

;





2019-09-07 16:33:24.810 MSK [63348] DETAIL:  parameters: $1 = 'Queen', $2 = 'artist/name', $3 = 'Queen', $4 = 'release/artist', $5 = 'release/year'




with

_1 as (

select distinct
    e, a, v, t
from datoms3
where
    a = '"artist/name"'::jsonb and v = '"Abba"'::jsonb

),

_2 as (

select distinct
    e, a, v, t
from datoms3
where
    a = '"release/artist"'::jsonb and v in (select e from _1)
),

_3 as (
select distinct
    e, a, v, t
from datoms3
where a = '"release/year"'::jsonb and v = '1985'::jsonb
    and e in (select e from _2)
)


select distinct _1.v, _3.v
from _1, _3

;









with

_1 as (

select distinct
    e, a, v, t
from datoms4
where
    a = 'artist/name' and v = 'Abba'

),

_2 as (

select distinct
    e, a, v, t
from datoms4
where
    a = 'release/artist' and v::integer in (select e from _1)
),

_3 as (
select distinct
    e, a, v, t
from datoms4
where a = 'release/year' and v::integer = 1985
    and e in (select e from _2)
)


select 1
from _1, _2, _3
where
        _1.e = _2.v::integer
    and _3.e = _2.e

;



with

_1 as (

select distinct *
from datoms3
where a = '"artist/name"' and v = '"Abba"'

),

_2 as (

select distinct *
from datoms3
where a = '"release/artist"' and v in (select e from _1)

),

_3 as (
select distinct *

from datoms3
where a = '"release/year"' and v = '1985' and e in (select e from _2)

)


select 1

;


select e,a,v,t
from datoms3
where a = '"artist/name"' and v = '"Abba"'

union

select e,a,v,t
from datoms3
where a = '"release/artist"' and v = '2'


create index _d3v on datoms3 (v) where a = '"release/artist"'::jsonb;

create index _d3av on datoms3 (a,v);



select d1.e

from
datoms3 d1

join datoms3 d2
on d2.a = '"release/artist"' and d2.v = d1.e

where

d1.a = '"artist/name"' and d1.v = '"Abba"'



select d1.e

from

join datoms3 d2 on d2.v = d1.e and d2.a = '"release/artist"'

where
d1.a = '"artist/name"' and d1.v = '"Abba"'


select d1.e

from
datoms3 d1

join datoms3 d2 on d2.v = d1.e


where

d1.a = '"artist/name"' and d1.v = '"Abba"'

and d2.a = '"release/artist"'



select d1.e

from datoms3 d1 cross join datoms3 d2

where

d1.a = '"artist/name"' and d1.v = '"Abba"'

and d2.v = d1.e and d2.a = '"release/artist"'

;


select s1.e

from

(select * from datoms3 d1 where d1.a = '"artist/name"' and d1.v = '"Abba"') s1

(select * from datoms3 d2 where d2.v = s1.e and d2.a = '"release/artist"')

join

;


with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e,
(select v from foo where a = 'release/artist' limit 1) as "release/artist",
(select v from foo where a = 'release/year' limit 1)   as "release/year"
from foo
group by e
;


with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e,
first_value(v) over (partition by e, a)
from foo
;


with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e,
first_value(case when a = 'release/artist' then v::integer end)  -- as "release/artist"
-- first_value(case when a = 'release/year' then v::integer end) as "release/year",
-- first_value(case when a = 'release/tag' then v end) as "release/tag"
OVER (PARTITION BY e)
from foo
;


with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e as "db/id",
max(v::integer) filter (where a = 'release/artist') as "release/artist",
max(v::integer) filter (where a = 'release/year') as "release/year",
array_agg(v) filter (where a = 'release/tag') as "release/tag"
from foo
group by e
;



with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e,

array_agg(v) filter (where a = 'release/tag')
over (partition by e,a)
as "release/tag"
from foo
;




with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e,
first_value(v) over w
from foo

WINDOW w AS (
    partition by e
    where a = 'release/artist'

)
;


(select v::integer from foo where a = 'release/artist' and e = aaa) as "release/artist",
(select v::integer from foo where a = 'release/year') as "release/year",



WITH "sub1" AS (SELECT * FROM "datoms4" WHERE ("e" in (6, 7))) SELECT (SELECT CAST("v" AS integer) FROM "sub1" WHERE ("a" = 'release/year')) AS "release/year", (SELECT CAST("v" AS integer) FROM "sub1" WHERE ("a" = 'release/artist')) AS "release/artist", array((SELECT CAST("v" AS text) FROM "sub1" WHERE ("a" = 'release/tag'))) AS "release/tag";


with foo as (
    select * from datoms4 where e in (6, 7)
)
select
e, a, array_agg(v)
from foo
group by e, a
;




with
sub1 as (
    select * from datoms4
    where a = 'artist/name' and v = 'Queen'
),
sub2 as (
    select * from datoms4
    where a = 'release/artist' and v::integer in (select e from sub1)

),
sub3 as (
    select * from datoms4
    where a = 'release/year' and e in (select e from sub2)

)

select distinct
    sub1.e, sub3.v

from sub1, sub3
;









explain analyze
with

sub1 as (
    select e from datoms4
    where a = 'artist/name'
),
sub2 as (
    select e from datoms4
    where a = 'release/artist' and v::integer in (select e from sub1)

),
sub3 as (
    select v from datoms4
    where a = 'release/year' and e in (select e from sub2)

)

select distinct
    sub1.e, sub3.v

from sub1, sub3
;







explain analyze

with
sub1 as (
    select distinct
    d.e from datoms4 d
    where a = 'artist/name'
),
sub2 as (
    select distinct
    d.e from datoms4 d
    where a = 'release/artist' and v::integer in (select e from sub1)

),
sub3 as (
    select distinct
    d.v from datoms4 d
    where a = 'release/year' and e in (select e from sub2)

)
select distinct sub1.e, sub3.v
from sub1, sub3
;




explain analyze
with
sub1 as (
    select distinct
    d.e, d.v from datoms4 d
    where a = 'release/year'
),
sub2 as (
    select distinct
    d.v from datoms4 d, sub1
    where a = 'release/artist' and
    d.e = sub1.e
),
sub3 as (
    select distinct
    d.v from datoms4 d, sub2
    where a = 'artist/name' and
    d.e = sub2.v::integer
)
select distinct sub1.v, sub3.v
from sub1, sub3
;



explain analyze
with
sub1 as (
    select distinct
    d.e, d.v from datoms4 d
    where a = 'release/year'
),
sub2 as (
    select distinct
    d.v from datoms4 d
    where a = 'release/artist' and
    d.e in (select e from sub1)
),
sub3 as (
    select distinct
    d.v from datoms4 d, sub2
    where a = 'artist/name' and
    d.e in (select v::integer from sub2)
)
select distinct sub1.v, sub3.v
from sub1, sub3
;






explain analyze

with

sub1 as (
    select distinct
    d.e, d.a, d.v from datoms4 d
    where d.a = 'artist/name'
),
sub2 as (
    select distinct
    d.e, d.a, d.v from datoms4 d, sub1
    where d.a = 'release/artist' and d.v::integer = sub1.e

),
sub3 as (
    select distinct
    d.e, d.a, d.v from datoms4 d, sub2
    where d.a = 'release/year' and d.e = sub2.e
)
select distinct
    sub1.v, sub3.v::integer

from sub1, sub2, sub3

where

    sub2.v::integer = sub1.e
and sub3.e = sub2.e

;

-- from sub1, sub2, sub3
-- where
--     sub1.a = 'artist/name'
-- and sub2.a = 'release/artist'
-- and sub2.v::integer = sub1.e
-- and sub3.a = 'release/year'
-- and sub3.e = sub2.e


;






WITH "sub1" AS (
    SELECT * FROM "__test_datoms"
    WHERE ("sub1"."a" = 'artist/name' AND "sub1"."v" = 'Queen'))

SELECT DISTINCT
"sub1"."e" AS "f1"
FROM "sub1"
;



WITH "sub1" AS (
    SELECT * FROM "__test_datoms" "d1"
    WHERE ("d1"."a" = 'artist/name' AND "d1"."v" = 'Queen')

)
SELECT DISTINCT

"sub1"."e" AS "f1"

FROM "sub1"
;







WITH
"sub1" AS (
    SELECT * FROM "__test_datoms" "d1"
    WHERE ("d1"."a" = ? AND "d1"."v" = ?)
),
"sub2" AS (
    SELECT * FROM "__test_datoms" "d2"
    WHERE "d2"."a" = ?
) SELECT DISTINCT
"sub1"."e" AS "f1"

FROM "sub1", "sub2"








WITH
sub1 AS (
    SELECT d1.* FROM __test_datoms d1 WHERE (d1.a = 'artist/name' AND d1.v = 'Queen')
),
sub2 AS (
    SELECT d2.* FROM __test_datoms d2, sub1 WHERE (d2.a = 'release/artist' AND CAST(d2.v AS bigint) = sub1.e)
)
SELECT DISTINCT
* from sub2
;




WITH sub1 AS (SELECT d1.* FROM __test_datoms d1 WHERE (d1.a = 'artist/name' AND d1.v = 'Queen')), sub2 AS (SELECT d2.* FROM __test_datoms d2, sub1 WHERE (d2.a = ? AND CAST(d2.v AS bigint) = sub1.e)), sub3 AS (SELECT d3.* FROM __test_datoms d3, sub2 WHERE (d3.e = sub2.e AND d3.a = ?)) SELECT DISTINCT sub1.e AS f1, CAST(sub3.v AS bigint) AS f2 FROM sub1, sub2, sub3 WHERE (CAST(sub2.v AS bigint) = sub1.e AND sub3.e = sub2.e)

:artist/name Queen :release/artist :release/year





explain analyze
WITH sub1 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v
   FROM datoms4 d
   WHERE d.a = 'artist/name' ),
     sub2 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v
   FROM datoms4 d,
        sub1
   WHERE (d.a = 'release/artist'
          AND CAST(d.v AS bigint) = sub1.e) ),
     sub3 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v
   FROM datoms4 d,
        sub2
   WHERE (d.e = sub2.e
          AND d.a = 'release/year') )
SELECT DISTINCT sub1.e AS f1,
                sub1.v AS f2,
                CAST(sub3.v AS bigint) AS f3
FROM sub1,
     sub2,
     sub3
WHERE (CAST(sub2.v AS bigint) = sub1.e
       AND sub3.e = sub2.e)
;






explain analyze
WITH sub1 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v
   FROM datoms4 d
   WHERE d.a = 'release/artist' ),
     sub2 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v
   FROM datoms4 d,
        sub1
   WHERE (d.e = sub1.e
          AND d.a = 'release/year') ),
     sub3 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v
   FROM datoms4 d,
        sub1
   WHERE (d.e = CAST(sub1.v AS bigint)
          AND d.a = 'artist/name') )
SELECT DISTINCT sub1.e AS f1,
                CAST(sub2.v AS bigint) AS f2,
                sub3.v AS f3
FROM sub1,
     sub2,
     sub3
WHERE (sub2.e = sub1.e
       AND sub3.e = CAST(sub1.v AS bigint))
;




WITH sub1 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v,
                   d.t
   FROM __test_datoms d
   WHERE d.a = 'release/artist' ),
     sub2 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v,
                   d.t
   FROM __test_datoms d,
        sub1
   WHERE (d.e = sub1.e
          AND d.a = 'release/year') ),
     sub3 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v,
                   d.t
   FROM __test_datoms d,
        sub1
   WHERE ((d.e = CAST(sub1.v AS bigint)
           AND d.a = 'artist/name')
          AND d.v = 'AAA') )
SELECT DISTINCT sub1.e AS f1,
                CAST(sub2.v AS bigint) AS f2
FROM sub1,
     sub2,
     sub3
WHERE (sub2.e = sub1.e
       AND (NOT ((sub3.e = CAST(sub1.v AS bigint)))))
;



WITH sub1 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v,
                   d.t
   FROM __test_datoms d
   WHERE d.a = 'release/artist' ),
     sub2 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v,
                   d.t
   FROM __test_datoms d,
        sub1
   WHERE (d.e = sub1.e
          AND d.a = 'release/year') ),
     sub3 AS
  (SELECT DISTINCT d.e,
                   d.a,
                   d.v,
                   d.t
   FROM __test_datoms d,
        sub1
   WHERE ((d.e = CAST(sub1.v AS bigint)
           AND d.a = 'artist/name')
          AND d.v = 'AAA') )

SELECT DISTINCT
*
FROM sub1 s1 ,sub2
left join sub3 on sub3.e = CAST(s1.v AS bigint)
;





WITH sub1 AS (SELECT DISTINCT d.e, d.a, d.v, d.t FROM __test_datoms d WHERE d.a = 'release/artist' ), sub2 AS (SELECT DISTINCT d.e, d.a, d.v, d.t FROM __test_datoms d, sub1 WHERE (d.e = sub1.e AND d.a = 'release/year') ), sub3 AS (SELECT DISTINCT d.e, d.a, d.v, d.t FROM __test_datoms d, sub1 WHERE ((d.e = CAST(sub1.v AS bigint) AND d.a = 'artist/name') AND d.v = 'AAA') ) SELECT DISTINCT * FROM sub1 s1 ,sub2 left join sub3 on sub3.e = CAST(s1.v AS bigint) ;


    WITH sub1 AS (SELECT
        DISTINCT d.e,
        d.a,
        d.v,
        d.t
    FROM
        _tests_datoms d
    WHERE
        d.a = 'artist/name' ) SELECT
        DISTINCT sub1.v AS f1
    FROM
        sub1 ;




    WITH "sub1" AS (SELECT
        DISTINCT "d"."e",
        "d"."a",
        "d"."v",
        "d"."t"
    FROM
        "_tests8_datoms" "d"
    WHERE
        "d"."a" = 'person/full-name' ),
    "sub2" AS (SELECT
        DISTINCT "d"."e",
        "d"."a",
        "d"."v",
        "d"."t"
    FROM
        "_tests8_datoms" "d",
        "sub1"
    WHERE
        ("d"."a" = 'band/members'
        AND CAST("d"."v" AS bigint) = sub1.e)
    ) SELECT
        DISTINCT sub1.v AS "f1",
        sub1.e AS "f2",
        sub2.e AS "f3"
    FROM
        "sub1",
        "sub2"
    WHERE
        sub2.v = sub1.e











WITH "sub1" AS
  (SELECT DISTINCT "d"."e",
                   "d"."a",
                   "d"."v",
                   "d"."t"
   FROM "_tests10_datoms" "d"
   WHERE "d"."a" = 'band/name' ),
     "sub2" AS
  (SELECT DISTINCT "d"."e",
                   "d"."a",
                   "d"."v",
                   "d"."t"
   FROM "_tests10_datoms" "d",
        "sub1"
   WHERE ("d"."e" = sub1.e
          AND "d"."a" = 'band/members') ),
     "sub3" AS
  (SELECT DISTINCT "d"."e",
                   "d"."a",
                   "d"."v",
                   "d"."t"
   FROM "_tests10_datoms" "d",
        "sub2"
   WHERE (("d"."e" = CAST("sub2"."v" AS bigint)
           AND "d"."a" = 'person/gender')
          AND CAST("d"."v" AS bigint) = 'gender/female') )
SELECT DISTINCT sub1.v AS "f1"
FROM "sub1",
     "sub2",
     "sub3"
WHERE (sub2.e = sub1.e
       AND sub3.e = CAST("sub2"."v" AS bigint))
;




select v from (

select

1 as n, v
from datoms4
where e = 1 and a = 'artist/name2'

union

select
2 as n, 'N/A' as v
from datoms4 where e = 1

order by n
limit 1

) _foo

;





-------
