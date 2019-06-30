CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_bygningtilknytning (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
	bygningid decimal(19,0),
    adgangsadresseid varchar(255)
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	bygningtilknytninger = utils.get('bygningtilknytning', ctx.TXIDTIL, ctx.TXIDFRA)

	for bygningtilknytning in bygningtilknytninger:

		ctx.emit(
			bygningtilknytning['bygningid'],
			bygningtilknytning['adgangsadresseid']
		)

/
