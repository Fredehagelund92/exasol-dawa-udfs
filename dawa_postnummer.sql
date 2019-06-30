
CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_postnummer (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
txid decimal(18,0),
tidspunkt TIMESTAMP,
operation varchar(255),
nr varchar(255),
navn varchar(255),
stormodtager boolean
) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

	postnumre = utils.get('postnummer', ctx.TXIDTIL, ctx.TXIDFRA)

	for postnummer in postnumre:

		time_at = None
		if 'tidspunkt' in postnummer:
			time_at = utils.parse_datetime(postnummer['tidspunkt'])

		ctx.emit(
			postnummer['txid'] if 'txid' in postnummer else None,
			time_at,
			postnummer['operation'] if 'txid' in postnummer else None,
			postnummer['nr'],
			postnummer['navn'],
			bool(postnummer['stormodtager'])
		)

/
