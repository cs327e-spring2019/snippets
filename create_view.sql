-- create view example for Milestone 7
-- note: the project id is required when creating views
create view oscars.oscar_nominations as
select entity, count(*) as nomination_count
from `cs327e-sp2019.oscars.Academy_Award` 
where category in ('ACTOR IN A LEADING ROLE', 'ACTOR IN A SUPPORTING ROLE', 'ACTRESS', 'ACTRESS IN A LEADING ROLE', 
'ACTRESS IN A SUPPORTING ROLE', 'DIRECTING (Comedy Picture)', 'DIRECTING (Dramatic Picture)')
group by entity
order by nomination_count desc

-- query the view as follows: 
-- select * from `cs327e-sp2019.oscars.oscar_nominations`
-- note: you must qualify the fully qualify the view with its project id and dataset. 