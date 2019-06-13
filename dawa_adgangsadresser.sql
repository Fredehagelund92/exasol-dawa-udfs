
CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_adgangsadresser (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
id VARCHAR(255),
status VARCHAR(255),
oprettet VARCHAR(255),
ændret VARCHAR(255),
ikrafttrædelsesdato VARCHAR(255),
kommunekode VARCHAR(255),
vejkode VARCHAR(255),
husnr VARCHAR(255),
supplerendebynavn VARCHAR(255),
postnr VARCHAR(255),
ejerlavkode DECIMAL(9,0),
matrikelnr VARCHAR(255),
esrejendomsnr VARCHAR(255),
etrs89koordinat_øst: float
etrs89koordinat_nord: float
nøjagtighed VARCHAR(255),
kilde DECIMAL(9,0),
husnummerkilde DECIMAL(9,0),
tekniskstandard VARCHAR(255),
tekstretning: float
adressepunktændringsdato VARCHAR(255),
esdhreference VARCHAR(255),
journalnummer VARCHAR(255),
højde FLOAT,
adgangspunktid VARCHAR(255),
supplerendebynavn_dagi_id VARCHAR(255),
vejpunkt_id VARCHAR(255),
navngivenvej_id VARCHAR(255)

) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	adgangsadresser = utils.get('adgangsadresse', ctx.TXIDTIL, ctx.TXIDFRA)

	for adgangsadresse in adgangsadresser:

		created_at = utils.parse_datetime(adresse['oprettet'])
		changed_at = utils.parse_datetime(adresse['ændret'])
		started_at = utils.parse_datetime(adresse['ikrafttrædelsesdato'])

		ctx.emit(
			adgangsadresse['id'],
			adgangsadresse['status'],
			created_at,
			changed_at,
			started_at,
			adgangsadresse['kommunekode'],
			adgangsadresse['vejkode'],
			adgangsadresse['husnr'],
			adgangsadresse['supplerendebynavn'],
			adgangsadresse['postnr'],
			adgangsadresse['ejerlavkode'],
			adgangsadresse['matrikelnr'],
			adgangsadresse['esrejendomsnr'],
			adgangsadresse['etrs89koordinat_øst'],
			adgangsadresse['etrs89koordinat_nord'],
			adgangsadresse['nøjagtighed'],
			adgangsadresse['kilde'],
			adgangsadresse['husnummerkilde'],
			adgangsadresse['tekniskstandard'],
			adgangsadresse['tekstretning'],
			adgangsadresse['adressepunktændringsdato'],
			adgangsadresse['esdhreference'],
			adgangsadresse['journalnummer'],
			adgangsadresse['højde'],
			adgangsadresse['adgangspunktid'],
			adgangsadresse['supplerendebynavn_dagi_id'],
			adgangsadresse['vejpunkt_id'],
			adgangsadresse['navngivenvej_id'],
		)

/