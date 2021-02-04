select 
    party_affiliation, 
    total, 
    avg, 
    days_since, 
    is_donor, 
    rating as cenblock_rating 
from voters v 
left join cenblock_ratings c on v.blockgeoid = c.blockgeoid;