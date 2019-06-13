CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_dagi_postnummer (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
ændret timestamp,
geo_ændret timestamp,
geo_version decimal(9,0),
visueltcenter varchar(10000),
bbox varchar(10000),
geometri varchar(10000),
dagi_id varchar(255),
nr varchar(255),
navn varchar(255)
) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

    postnumre = utils.get('dagi_postnummer', ctx.TXIDTIL, ctx.TXIDFRA)

    for postnummer in postnumre:

        changed_at = utils.parse_datetime(postnummer['ændret'])
        geo_changed_at = utils.parse_datetime(postnummer['ændret'])

        visueltcenter = utils.parse_point(postnummer['visueltcenter'])
        bbox = utils.parse_polygon(postnummer['bbox'])
        geometri = utils.parse_multipolygon(postnummer['geometri'])

        ctx.emit(
            changed_at,
            geo_changed_at,
            postnummer['geo_version'],
            visueltcenter,
            bbox,
            geometri,
			postnummer['dagi_id'],
            postnummer['nr'],
            postnummer['navn']
        )

/
