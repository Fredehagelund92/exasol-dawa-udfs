
CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_vejstykker (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
id varchar(255),
kommunekode varchar(255),
kode varchar(255),
navn varchar(255),
adresseringsnavn varchar(255),
navngivenvej_id varchar(255),
oprettet timestamp,
ændret timestamp
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	vejstykker = utils.get('vejstykke', ctx.TXIDTIL, ctx.TXIDFRA)

	for vejstykke in vejstykker:
		created_at = utils.parse_datetime(vejstykke['oprettet'])
		changed_at = utils.parse_datetime(vejstykke['ændret'])
		ctx.emit(
			vejstykke['id'],
			vejstykke['kommunekode'],
			vejstykke['kode'],
			vejstykke['navn'],
			vejstykke['adresseringsnavn'],
			vejstykke['navngivenvej_id'],
			created_at,
			changed_at
		)

/
