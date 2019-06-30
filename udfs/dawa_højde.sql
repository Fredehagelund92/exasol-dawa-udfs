CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_højde (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
husnummerid varchar(255),
højde float
) AS

utils = exa.import_script("ETL_UDFS.DAWA_UTILS")

def run(ctx):

    højder = utils.get('højde', ctx.TXIDTIL, ctx.TXIDFRA)

    for højde in højder:

        ctx.emit(
            højde['husnummerid'],
            højde['højde']
        )

/
