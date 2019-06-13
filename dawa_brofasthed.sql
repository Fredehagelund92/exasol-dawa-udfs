CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_brofasthed (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
    stedid varchar(255),
    brofast boolean
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	brofastheder = utils.get('brofasthed', ctx.TXIDTIL, ctx.TXIDFRA)

	for brofasthed in brofastheder:

		ctx.emit(
			brofasthed['stedid'],
			brofasthed['brofast']
		)

/
