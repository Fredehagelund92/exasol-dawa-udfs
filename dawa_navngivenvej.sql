CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_navngivenvej (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
id varchar(255),
darstatus varchar(255),
oprettet TIMESTAMP,
ændret TIMESTAMP,
navn varchar(255),
adresseringsnavn varchar(255),
administrerendekommune varchar(255),
beskrivelse varchar(255),
retskrivningskontrol varchar(255),
udtaltvejnavn varchar(255),
beliggenhed_oprindelse_kilde varchar(255),
beliggenhed_oprindelse_nøjagtighedsklasse varchar(255),
beliggenhed_oprindelse_registrering TIMESTAMP,
beliggenhed_oprindelse_tekniskstandard varchar(255)
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	navngivneveje = utils.get('navngivenvej', ctx.TXIDTIL, ctx.TXIDFRA)

	for navngivenvej in navngivneveje:
		created_at = utils.parse_datetime(navngivenvej['oprettet'])
		changed_at = utils.parse_datetime(navngivenvej['ændret'])
		placed_at = utils.parse_datetime(navngivenvej['beliggenhed_oprindelse_registrering'])
		ctx.emit(
			navngivenvej['id'],
			navngivenvej['darstatus'],
			created_at,
			changed_at,
			navngivenvej['navn'],
			navngivenvej['adresseringsnavn'],
			navngivenvej['administrerendekommune'],
			navngivenvej['beskrivelse'],
			navngivenvej['retskrivningskontrol'],
			navngivenvej['udtaltvejnavn'],
			navngivenvej['beliggenhed_oprindelse_kilde'],
			navngivenvej['beliggenhed_oprindelse_nøjagtighedsklasse'],
			placed_at,
			navngivenvej['beliggenhed_oprindelse_tekniskstandard']

		)

/
