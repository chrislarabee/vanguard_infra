from typing import List

import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base


class Base:
    @classmethod
    def gen_column_list(cls) -> List[str]:
        """
        Returns:
            List[str, ...]: A list of strings, the names of all the 
                columns of the model.
        """
        return [i.key for i in sa.inspect(cls).mapper.column_attrs]

# Using this approach instead of as_declarative decorator helps vscode's
# error checking.
Base = declarative_base(cls=Base)

class CensusBlock(Base):
    __tablename__ = 'cenblocks'

    blockgeoid = Column(Integer, primary_key=True)
    totalpop = Column(Integer)
    total_donors = Column(Float)
    donation_total = Column(Float)
    percentunder18 = Column(Float)
    percent18to19 = Column(Float)
    percent20 = Column(Float)
    percent21 = Column(Float)
    percent22to24 = Column(Float)
    percent25to29 = Column(Float)
    percent30to34 = Column(Float)
    percent35to39 = Column(Float)
    percent40to44 = Column(Float)
    percent45to49 = Column(Float)
    percent50to54 = Column(Float)
    percent55to59 = Column(Float)
    percent60to61 = Column(Float)
    percent62to64 = Column(Float)
    percent65to66 = Column(Float)
    percent67to69 = Column(Float)
    percent70to74 = Column(Float)
    percent75to79 = Column(Float)
    percent80to84 = Column(Float)
    percent85andup = Column(Float)
    percent25to44 = Column(Float)
    percent40to59 = Column(Float)
    percentcollegeeducated = Column(Float)
    percentunder10k = Column(Float)
    lowpercentunder10k = Column(Float)
    highpercentunder10k = Column(Float)
    percent10kto14k = Column(Float)
    lowpercent10to14k = Column(Float)
    highpercent10to14k = Column(Float)
    percent15kto19k = Column(Float)
    lowpercent15to19k = Column(Float)
    highpercent15to19k = Column(Float)
    percent20kto24k = Column(Float)
    lowpercent20to24k = Column(Float)
    highpercent20to24k = Column(Float)
    percent25kto29k = Column(Float)
    lowpercent25to29k = Column(Float)
    highpercent25to29k = Column(Float)
    percent30kto34k = Column(Float)
    lowpercent30to34k = Column(Float)
    highpercent30to34k = Column(Float)
    percent35kto39k = Column(Float)
    lowpercent35to39k = Column(Float)
    highpercent35to39k = Column(Float)
    percent40kto44k = Column(Float)
    lowpercent40to44k = Column(Float)
    highpercent40to44k = Column(Float)
    percent45kto49k = Column(Float)
    lowpercent45to49k = Column(Float)
    highpercent45to49k = Column(Float)
    percent50kto59k = Column(Float)
    lowpercent50to59k = Column(Float)
    highpercent50to59k = Column(Float)
    percent60kto74k = Column(Float)
    lowpercent60to74k = Column(Float)
    highpercent60to74k = Column(Float)
    percent75kto99k = Column(Float)
    lowpercent75to99k = Column(Float)
    highpercent75to99k = Column(Float)
    percent100kto124k = Column(Float)
    lowpercent100to124k = Column(Float)
    highpercent100to124k = Column(Float)
    percent125kto149k = Column(Float)
    lowpercent125to149k = Column(Float)
    highpercent125to149k = Column(Float)
    percent150kto199k = Column(Float)
    lowpercent150to199k = Column(Float)
    highpercent150to199k = Column(Float)
    percent200kandup = Column(Float)
    lowpercent200kandup = Column(Float)
    highpercent200kandup = Column(Float)
    percentabove100k = Column(Float)
    lowpercentabove100k = Column(Float)
    highpercentabove100k = Column(Float)
    percent50kto99k = Column(Float)
    lowpercent50kto99k = Column(Float)
    highpercent50kto99k = Column(Float)
    percentemployerbasedonly = Column(Float)
    percentdirectpurchaseonly = Column(Float)
    percentmedicareonly = Column(Float)
    percentemployeranddirectpurchase = Column(Float)
    percentemployerbasedandmedicare = Column(Float)
    percentmedicareandmedicaid = Column(Float)
    percentnohealthinsurance = Column(Float)

    def __repr__(self):
        return (
            f"<Cenblock(blockgeoid={self.blockgeoid}), totalpop={self.totalpop}>"
        )


class Voter(Base):
    __tablename__ = 'voters'
    
    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    suffix = Column(String)
    party_affiliation = Column(String)
    street1 = Column(String)
    street2 = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(Integer) 
    plus4 = Column(Float)
    # Some voters have more than 1 precinct/district_num.
    # 'precinct = Column(String)
    # 'district_num = Column(Integer) 
    ohvfid = Column(String)
    blockgeoid = Column(Integer) 
    demdonationamounts = Column(String)
    demcommitteecodes = Column(String)
    repdonationamounts = Column(String)
    repcommitteecodes = Column(String)
    otherpartydonationamounts = Column(String)
    otherpartycommitteecodes = Column(String)
    total = Column(Float)
    avg = Column(Float)
    days_since = Column(Integer)
    is_donor = Column(Integer)

    def __repr__(self):
        return (
            f"<Voter(id={self.id}, name={self.first_name} {self.last_name})>"
        )


class Call(Base):
    __tablename__ = 'calls'

    id = Column(Integer, primary_key=True)
    ohvfid = Column(String)
    call_result = Column(Integer)


class District(Base):
    __tablename__ = 'districts'

    district_num = Column(Integer, primary_key=True)
    district = Column(String)
    totalpop = Column(Integer)
    total_donors = Column(Float)
    donation_total = Column(Float)
