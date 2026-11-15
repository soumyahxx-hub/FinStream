-- Run manually after data acquisition. Sources remain separate by design.
select source_system, count(*) as transaction_count
from {{ ref('stg_transactions') }}
group by 1
order by 1
