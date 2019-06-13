CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_afstemningsområde (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
ændret TIMESTAMP,
geo_ændret TIMESTAMP,
geo_version DECIMAL(9,0),
visueltcenter VARCHAR(255),
bbox VARCHAR(10000),
geometri VARCHAR(10000),
dagi_id VARCHAR(255),
nummer VARCHAR(255),
navn VARCHAR(255),
afstemningsstednavn VARCHAR(255),
afstemningsstedadresse VARCHAR(255),
kommunekode VARCHAR(255),
opstillingskreds_dagi_id VARCHAR(255)
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	afstemningsområder = utils.get('afstemningsområde', ctx.TXIDTIL, ctx.TXIDFRA)

	for afstemningsområde in afstemningsområder:
		geo_changed_at = utils.parse_datetime(afstemningsområde['geo_ændret'])
		changed_at = utils.parse_datetime(afstemningsområde['ændret'])

		visueltcenter = utils.parse_point(afstemningsområde['visueltcenter'])
		bbox = utils.parse_polygon(afstemningsområde['bbox'])
		geometri = utils.parse_multipolygon(afstemningsområde['geometri'])
		ctx.emit(
			geo_changed_at,
			changed_at,
			afstemningsområde['geo_version'],
			visueltcenter,
			bbox,
			geometri,
			afstemningsområde['dagi_id'],
			afstemningsområde['nummer'],
			afstemningsområde['navn'],
			afstemningsområde['afstemningsstednavn'],
			afstemningsområde['afstemningsstedadresse'],
			afstemningsområde['kommunekode'],
            afstemningsområde['opstillingskreds_dagi_id']

		)

/
