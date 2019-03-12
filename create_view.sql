-- sample view for Milestone 7

create view oscars.oscar_nominations as
select entity, count(*) as nomination_count
from `cs327e-sp2019.oscars.Academy_Award` 
where category in ('ACTOR IN A LEADING ROLE', 'ACTOR IN A SUPPORTING ROLE', 'ACTRESS', 'ACTRESS IN A LEADING ROLE', 
'ACTRESS IN A SUPPORTING ROLE', 'DIRECTING (Comedy Picture)', 
'DIRECTING (Dramatic Picture)')
group by entity
order by nomination_count desc