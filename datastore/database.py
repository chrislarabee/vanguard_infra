import sqlite3

def create_district_table():
    return (
        """
        CREATE TABLE districts (
            district_num INTEGER,
            district VARCHAR,
            totalpop INTEGER,
            total_donors FLOAT,
            donation_total FLOAT
        );
        """
    )


def create_cenblocks_table():
    return (
        """
        CREATE TABLE cenblocks (
            blockgeoid INTEGER,
            totalpop INTEGER,
            total_donors FLOAT,
            donation_total FLOAT
            percentunder18 FLOAT,
            percent18to19 FLOAT,
            percent20 FLOAT,
            percent21 FLOAT,
            percent22to24 FLOAT,
            percent25to29 FLOAT,
            percent30to34 FLOAT,
            percent35to39 FLOAT,
            percent40to44 FLOAT,
            percent45to49 FLOAT,
            percent50to54 FLOAT,
            percent55to59 FLOAT,
            percent60to61 FLOAT,
            percent62to64 FLOAT,
            percent65to66 FLOAT,
            percent67to69 FLOAT,
            percent70to74 FLOAT,
            percent75to79 FLOAT,
            percent80to84 FLOAT,
            percent85andup FLOAT,
            percent25to44 FLOAT,
            percent40to59 FLOAT,
            percentcollegeeducated FLOAT,
            percentunder$10k FLOAT,
            lowpercentunder$10k FLOAT,
            highpercentunder$10k FLOAT,
            percent$10kto$14k FLOAT,
            lowpercent$10to$14k FLOAT,
            highpercent$10to$14k FLOAT,
            percent$15kto$19k FLOAT,
            lowpercent$15to$19k FLOAT,
            highpercent$15to$19k FLOAT,
            percent$20kto$24k FLOAT,
            lowpercent$20to$24k FLOAT,
            highpercent$20to$24k FLOAT,
            percent$25kto$29k FLOAT,
            lowpercent$25to$29k FLOAT,
            highpercent$25to$29k FLOAT,
            percent$30kto$34k FLOAT,
            lowpercent$30to$34k FLOAT,
            highpercent$30to$34k FLOAT,
            percent$35kto$39k FLOAT,
            lowpercent$35to$39k FLOAT,
            highpercent$35to$39k FLOAT,
            percent$40kto$44k FLOAT,
            lowpercent$40to$44k FLOAT,
            highpercent$40to$44k FLOAT,
            percent$45kto$49k FLOAT,
            lowpercent$45to$49k FLOAT,
            highpercent$45to$49k FLOAT,
            percent$50kto$59k FLOAT,
            lowpercent$50to$59k FLOAT,
            highpercent$50to$59k FLOAT,
            percent$60kto$74k FLOAT,
            lowpercent$60to$74k FLOAT,
            highpercent$60to$74k FLOAT,
            percent$75kto$99k FLOAT,
            lowpercent$75to$99k FLOAT,
            highpercent$75to$99k FLOAT,
            percent$100kto$124k FLOAT,
            lowpercent$100to$124k FLOAT,
            highpercent$100to$124k FLOAT,
            percent$125kto$149k FLOAT,
            lowpercent$125to$149k FLOAT,
            highpercent$125to$149k FLOAT,
            percent$150kto$199k FLOAT,
            lowpercent$150to$199k FLOAT,
            highpercent$150to$199k FLOAT,
            percent$200kandup FLOAT,
            lowpercent$200kandup FLOAT,
            highpercent$200kandup FLOAT,
            percentabove$100k FLOAT,
            lowpercentabove$100k FLOAT,
            highpercentabove$100k FLOAT,
            percent$50kto$99k FLOAT,
            lowpercent$50kto$99k FLOAT,
            highpercent$50kto$99k FLOAT,
            percentemployerbasedonly FLOAT,
            percentdirectpurchaseonly FLOAT,
            percentmedicareonly FLOAT,
            percentemployeranddirectpurchase FLOAT,
            percentemployerbasedandmedicare FLOAT,
            percentmedicareandmedicaid FLOAT,
            percentnohealthinsurance FLOAT
        );
        """
    )

def create_voter_table():
    return (
        """
        CREATE TABLE voters (
            first_name VARCHAR,
            middle_name VARCHAR,
            last_name VARCHAR,
            suffix VARCHAR,
            party_affiliation VARCHAR,
            street1 VARCHAR,
            street2 VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip INTEGER,
            plus4 FLOAT,
            precinct VARCHAR,
            district_num INTEGER,
            ohvfid VARCHAR,
            blockgeoid INTEGER, 
            demdonationamounts VARCHAR,
            demcommitteecodes VARCHAR,
            repdonationamounts VARCHAR,
            repcommitteecodes VARCHAR,
            otherpartydonationamounts VARCHAR,
            otherpartycommitteecodes VARCHAR,
            total FLOAT,
            avg FLOAT,
            days_since INTEGER,
            is_donor INTEGER
        );
        """
    )