# Standard library imports
from lxml import html
import os
import requests
import sys
import time

# Third party imports
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

# Pull start time of script
t_start = time.time()

# Set final id to pull
end_id = 1000000

# Load file name
if len(sys.argv) == 1:
    file = "database/database.db"
else:
    # Ensure command passes csv file
    if sys.argv[1][-2:] != 'db':
        raise ValueError("Must input file with db extension.")

    file = sys.argv[1]

# Make column list for dataframe
col_list = [
    "id", "private", "fname", "lname", "race", "srace",
    "gender", "nameday", "guardian", "gc",
    "gc_rank", "pld", "mrd", "drk", "gnb", "whm", "sch",
    "ast", "sge", "mnk", "drg", "nin", "sam", "rpr",
    "vpr", "brd", "mch", "dnc", "blm", "acn", "rdm",
    "pct", "blu", "crp", "bsm", "arm", "gsm", "ltw",
    "wvr", "alc", "cul", "mnr", "btn", "fsh"
]

# Connect to existing database if it exists
if os.path.isfile(file):
    # Connect to database
    eng = sa.create_engine("sqlite:///" + file)
    # Form connection from engine
    with eng.connect() as con:
        max_id = con.execute("SELECT MAX(id) FROM char").fetchone()[0]
    if max_id is None:
        max_id = 1
else:
    raise NameError("No database found")

# Make base url to be filled by a for loop
url = "https://eu.finalfantasyxiv.com/lodestone/character/{}/"


# Define function to help convert levels to numeric values
def convert_lvl(str):
    if str == '-':
        return 0
    elif len(str) < 4:
        return int(str)
    else:
        return str

# Start session to reduce pull time
session = requests.Session()

# Loop over range of values to pull
for i in range(max_id, end_id):
    print(i)
    # Refresh session
    if i % 100 == 0:
        session = requests.Session()

    # Pull html
    pulled = False
    # This loop is necessary because my connection is not particularly stable.
    while not pulled:
        try:
            h = session.get(url.format(i))
            pulled = True
        except ConnectionError:
            print(f"Connection error for character {i}")
            time.sleep(2)
        except Exception as e:
            print("Uncaught error:")
            print(e)
            time.sleep(2)

    # Check for a successful pull
    if h.status_code == 200:
        x = html.fromstring(h.content)
    else:
        print("Request returned " + str(h.status_code))
        continue

    
    fname = x.xpath("/html/head/title/text()")[0].split("|")[0].split(" ")[0]
    lname = x.xpath("/html/head/title/text()")[0].split("|")[0].split(" ")[1]

    # Check for private profile
    try:
        private = "profile is private" in x.xpath("//p[@class='parts__zero']/text()")[0]
        print(fname + " " + lname + " (" + str(i) + ") is private.")
    except Exception:
        private = False

    # If char is private push the only data we can pull to database.
    if private:
        try:
            with eng.connect() as con:
                con.execute(sa.text(
                    f"""
                    INSERT INTO char(id, private, fname, lname)
                    VALUES ({i}, {private}, "{fname}", "{lname}");
                    """
                ))
        except IntegrityError:
            pass
        continue

    # Pull Race and Gender
    race = x.xpath("//div[@class='character-block']/div[@class='character-block__box']/p[@class='character-block__name']/text()")[0]
    srace = x.xpath("//div[@class='character-block']/div[@class='character-block__box']/p[@class='character-block__name']/text()")[1].split(" / ")[0]
    gender = x.xpath("//div[@class='character-block']/div[@class='character-block__box']/p[@class='character-block__name']/text()")[1].split(" / ")[1]
    print("Character race is: " + race)
    print("Character subrace is: " + srace)
    print("Character gender is: " + gender)

    # Pull Nameday
    nameday = x.xpath("//p[@class='character-block__birth']/text()")[0]
    print("Character Nameday: " + str(nameday))

    # Pull Guardian
    guardian = x.xpath("//div[@class='character-block__box'][contains(p, 'Nameday')]/p[@class='character-block__name']/text()")[0]
    print("Guardian: " + str(guardian))

    # Pull Grand company and rank
    try:
        gc = x.xpath("//div[@class='character-block__box'][contains(p, 'Grand Company')]/p[@class='character-block__name']/text()")[0].split(" / ")[0]
        gc_rank = x.xpath("//div[@class='character-block__box'][contains(p, 'Grand Company')]/p[@class='character-block__name']/text()")[0].split(" / ")[1]
    except:
        gc = ""
        gc_rank = ""
    print("Grand Company: " + gc)
    print("Grand Company rank: " + gc_rank)

    # Pull first row of levels (Tank / Healer)
    pal_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Gladiator')]/text()")[0])
    mar_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Marauder')]/text()")[0])
    dkn_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Dark Knight']]/text()")[0])
    gbk_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Gunbreaker']]/text()")[0])
    whm_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Conjurer')]/text()")[0])
    sch_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Scholar']]/text()")[0])
    ast_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Astrologian']]/text()")[0])
    sag_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Sage']]/text()")[0])

    # Pull second row of levels (Damage)
    mnk_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Pugilist')]/text()")[0])
    lnc_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Lancer')]/text()")[0])
    nin_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Rogue')]/text()")[0])
    sam_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Samurai']]/text()")[0])
    rpr_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Reaper']]/text()")[0])
    vpr_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Viper']]/text()")[0])
    brd_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Archer')]/text()")[0])
    mch_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Machinist']]/text()")[0])
    dnc_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Dancer']]/text()")[0])
    blk_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Thaumaturge')]/text()")[0])
    smn_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[contains(img/@data-tooltip, 'Arcanist')]/text()")[0])
    red_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Red Mage']]/text()")[0])
    pct_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Pictomancer']]/text()")[0])
    blu_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Blue Mage (Limited Job)']]/text()")[0])

    # Pull third row (DoH)
    crp_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Carpenter']]/text()")[0])
    bls_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Blacksmith']]/text()")[0])
    arm_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Armorer']]/text()")[0])
    gld_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Goldsmith']]/text()")[0])
    ltw_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Leatherworker']]/text()")[0])
    wev_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Weaver']]/text()")[0])
    alc_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Alchemist']]/text()")[0])
    cul_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Culinarian']]/text()")[0])

    # Pull fourth row (DoL)
    mnr_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Miner']]/text()")[0])
    bot_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Botanist']]/text()")[0])
    fsh_lvl = convert_lvl(x.xpath("//div[@class='character__level__list']/ul/li[img[@data-tooltip='Fisher']]/text()")[0])


    # Print for verification
    print("First name is " + fname)
    print("Family name is " + lname)

    print("Paladin level is " + str(pal_lvl))
    print("Marauder level is " + str(mar_lvl))
    print("Dark Knight level is " + str(dkn_lvl))
    print("Gunbreaker level is " + str(gbk_lvl))
    print("White Mage level is " + str(whm_lvl))
    print("Scholar level is " + str(sch_lvl))
    print("Astrologian level is " + str(ast_lvl))
    print("Sage level is " + str(sag_lvl))

    print("Monk level: " + str(mnk_lvl))
    print("Lancer level: " + str(lnc_lvl))
    print("Ninja level: " + str(nin_lvl))
    print("Samurai level: " + str(sam_lvl))
    print("Reaper level: " + str(rpr_lvl))
    print("Viper level: " + str(vpr_lvl))
    print("Bard level: " + str(brd_lvl))
    print("Machinist level: " + str(mch_lvl))
    print("Dancer level: " + str(dnc_lvl))
    print("Black Mage level: " + str(blk_lvl))
    print("Summoner level: " + str(smn_lvl))
    print("Pictomancer level: " + str(pct_lvl))
    print("Blue Mage level: " + str(blu_lvl))

    print("Carpenter level: " + str(crp_lvl))
    print("Blacksmith level: " + str(bls_lvl))
    print("Armoror level: " + str(arm_lvl))
    print("Goldsmith level: " + str(gld_lvl))
    print("Leatherworker level: " + str(ltw_lvl))
    print("Weaver level: " + str(wev_lvl))
    print("Alchemist level: " + str(alc_lvl))
    print("Culinarian level: " + str(cul_lvl))

    print("Miner level: " + str(mnr_lvl))
    print("Botanist level: " + str(bot_lvl))
    print("Fisher level: " + str(fsh_lvl))

    # Push data into database
    try:
        with eng.connect() as con:
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
                    pct,
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
                )
                VALUES (
                    {i},
                    {private},
                    "{fname}",
                    "{lname}",
                    "{race}",
                    "{srace}",
                    "{gender}",
                    "{nameday}",
                    "{guardian}",
                    "{gc}",
                    "{gc_rank}",
                    {pal_lvl},
                    {mar_lvl},
                    {dkn_lvl},
                    {gbk_lvl},
                    {whm_lvl},
                    {sch_lvl},
                    {ast_lvl},
                    {sag_lvl},

                    {mnk_lvl},
                    {lnc_lvl},
                    {nin_lvl},
                    {sam_lvl},
                    {rpr_lvl},
                    {vpr_lvl},
                    {brd_lvl},
                    {mch_lvl},
                    {dnc_lvl},
                    {blk_lvl},
                    {smn_lvl},
                    {red_lvl},
                    {pct_lvl},
                    {blu_lvl},

                    {crp_lvl},
                    {bls_lvl},
                    {arm_lvl},
                    {gld_lvl},
                    {ltw_lvl},
                    {wev_lvl},
                    {alc_lvl},
                    {cul_lvl},

                    {mnr_lvl},
                    {bot_lvl},
                    {fsh_lvl}
                );
            """
            ))
    except IntegrityError:
            pass

# Pull time at end of script
t_end = time.time()

# Print the runtime of the program
print("\n\nRuntime = " + str(t_end - t_start) + " seconds.")

if __name__ == "__main__":
    print("Name == main")
