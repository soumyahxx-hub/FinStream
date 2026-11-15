select event_id
from {{ ref('fct_transactions') }}
where amount < 0
