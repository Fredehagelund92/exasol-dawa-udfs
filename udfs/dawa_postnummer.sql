
CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_postnumre (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
nr varchar(255),
navn varchar(255),
stormodtager boolean
) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	postnumre = utils.get('postnummer', ctx.TXIDTIL, ctx.TXIDFRA)

	for postnummer in postnumre:

		ctx.emit(
			postnummer['nr'],
			postnummer['navn'],
			bool(postnummer['stormodtager'])
		)

/
