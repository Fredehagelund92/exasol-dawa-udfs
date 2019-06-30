CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_afstemningsområdetilknytning (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
    adgangsadresseid varchar(255),
    kommunekode varchar(255),
    afstemningsområdenummer varchar(255)
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	afstemningsområdetilknytninger = utils.get('afstemningsområdetilknytning', ctx.TXIDTIL, ctx.TXIDFRA)

	for afstemningsområdetilknytning in afstemningsområdetilknytninger:

		ctx.emit(
			afstemningsområdetilknytning['adgangsadresseid'],
			afstemningsområdetilknytning['kommunekode'],
			afstemningsområdetilknytning['afstemningsområdenummer']
		)

/
