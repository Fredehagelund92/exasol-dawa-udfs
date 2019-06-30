
CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_adresser (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
status DECIMAL(9,0),
oprettet TIMESTAMP,
ændret TIMESTAMP,
ikrafttrædelsesdato TIMESTAMP,
dør varchar(255),
id varchar(255),
adgangsadresseid varchar(255),
etage varchar(255),
kilde decimal(9,0),
esdhreference varchar(255),
journalnummer varchar(255)

) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	adresser = utils.get('adresse', ctx.TXIDTIL, ctx.TXIDFRA)

	for adresse in adresser:

		created_at = utils.parse_datetime(adresse['oprettet'])
		changed_at = utils.parse_datetime(adresse['ændret'])
		started_at = utils.parse_datetime(adresse['ikrafttrædelsesdato'])

		ctx.emit(
			adresse['status'],
			created_at,
			changed_at,
			started_at,
			adresse['dør'],
			adresse['id'],
			adresse['adgangsadresseid'],
			adresse['etage'],
			adresse['kilde'],
			adresse['esdhreference'],
			adresse['journalnummer'],
		)

/