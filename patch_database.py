"""
This script was necessary to transfer the pulled data from a .csv format
into a SQLite database. My initial script had an error in it that caused
rows to duplicate.

This script pulls the data from the csv in chunks (the file is about 76GB)
and then pushes the data to the database. The database has a unique
constraint on the id which is used to slake off the duplicate values.
"""

# Third party imports
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
import pandas as pd
import sqlite3 as sql

# Set chunksize parameter
chunksize = 10000

# Set chunk counter
c_no = 6290

# Form rename dict
rn_dict = {
    "id_no": "id",
    "subrace": "srace",
    "grand_company": "gc"
}

# Load in dataframe
for chunk in pd.read_csv(
        "lodestone.csv",
        chunksize=chunksize,
        skiprows=range(1, c_no*chunksize)
    ):

    # Work out how to do this within a chunk
    chunk.rename(rn_dict, axis="columns", inplace=True)

    # Fill na values
    chunk.fillna(
        {
            "race": "",
            "srace": "",
            "gender": "",
            "nameday": "",
            "guardian": "",
            "gc": "",
            "gc_rank": ""
        },
        inplace=True
    )
    chunk.fillna(0, inplace=True)

    # Create engine for sqlite3 database
    eng = sa.create_engine("sqlite:///database/database.db")

    # Push the chunk to the database
    with eng.connect() as con:
        for _, r in chunk.iterrows():
            try:
                con.execute(sa.text(
                    f"""
                        INSERT INTO char(
                            id,
                            private,
                            fname,
                            lname,
                            race,
                            srace,
                            gender,
                            nameday,
                            guardian,
                            gc,
                            gc_rank,
                            pld,
                            mrd,
                            drk,
                            gnb,
                            whm,
                            sch,
                            ast,
                            sge,
                            mnk,
                            drg,
                            nin,
                            sam,
                            rpr,
                            vpr,
                            brd,
                            mch,
                            dnc,
                            blm,
                            acn,
                            rdm,
                            blu,
                            crp,
                            bsm,
                            arm,
                            gsm,
                            ltw,
                            wvr,
                            alc,
                            cul,
                            mnr,
                            btn,
                            fsh
                        ) VALUES (
                            {r.id},
                            {r.private},
                            "{r.fname}",
                            "{r.lname}",
                            "{r.race}",
                            "{r.srace}",
                            "{r.gender}",
                            "{r.nameday}",
                            "{r.guardian}",
                            "{r.gc}",
                            "{r.gc_rank}",
                            {r.pld},
                            {r.mrd},
                            {r.drk},
                            {r.gnb},
                            {r.whm},
                            {r.sch},
                            {r.ast},
                            {r.sge},
                            {r.mnk},
                            {r.drg},
                            {r.nin},
                            {r.sam},
                            {r.rpr},
                            {r.vpr},
                            {r.brd},
                            {r.mch},
                            {r.dnc},
                            {r.blm},
                            {r.acn},
                            {r.rdm},
                            {r.blu},
                            {r.crp},
                            {r.bsm},
                            {r.arm},
                            {r.gsm},
                            {r.ltw},
                            {r.wvr},
                            {r.alc},
                            {r.cul},
                            {r.mnr},
                            {r.btn},
                            {r.fsh}
                        );
                    """
                ))
            except IntegrityError:
                # This is the unique constraint working as hoped
                continue
                # exit()
            except Exception as e:
                # Unexpected exception
                print(e)
                continue

    # Print output message
    print("Chunk " + str(c_no) + " pushed to database.")

    # Increment chunk counter
    c_no = c_no + 1

print("done")
