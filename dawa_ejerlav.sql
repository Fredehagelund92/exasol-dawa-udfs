CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_ejerlav (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
kode decimal(9,0),
navn varchar(255),
geo_ændret timestamp,
geo_version decimal(9,0),
ændret timestamp,
visueltcenter varchar(10000),
bbox varchar(10000),
geometri varchar(10000)
) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

    ejerlaver = utils.get('ejerlav', ctx.TXIDTIL, ctx.TXIDFRA)

    for ejerlav in ejerlaver:

        changed_at = utils.parse_datetime(ejerlav['ændret'])
        geo_changed_at = utils.parse_datetime(ejerlav['ændret'])

        visueltcenter = utils.parse_point(ejerlav['visueltcenter'])
        bbox = utils.parse_polygon(ejerlav['bbox'])
        geometri = utils.parse_multipolygon(ejerlav['geometri'])

        ctx.emit(
            ejerlav['kode'],
            ejerlav['navn'],
            geo_changed_at,
            ejerlav['geo_version'],
            changed_at,
            visueltcenter,
            bbox,
            geometri
        )

/
