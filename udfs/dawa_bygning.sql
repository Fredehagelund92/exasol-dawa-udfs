CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_bygning (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
id VARCHAR(255),
bygningstype VARCHAR(255),
metode3d VARCHAR(255),
målested VARCHAR(255),
bbrbygning_id VARCHAR(255),
synlig boolean,
overlap boolean,
geometri VARCHAR(100000)
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

    bygninger = utils.get('bygning', ctx.TXIDTIL, ctx.TXIDFRA)

    for bygning in bygninger:

        geometri = utils.parse_polygon(bygning['geometri'])
        ctx.emit(
            bygning['id'],
            bygning['bygningstype'],
            bygning['metode3d'],
            bygning['målested'],
            bygning['bbrbygning_id'],
            bygning['synlig'],
            bygning['overlap'],
            geometri
        )

/
