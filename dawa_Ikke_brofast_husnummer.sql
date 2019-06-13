CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_Ikke_brofast_husnummer (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
husnummerid varchar(255)
) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

    ikke_brofast_husnumre = utils.get('ikke_brofast_husnummer', ctx.TXIDTIL, ctx.TXIDFRA)

    for ikke_brofast_husnummer in ikke_brofast_husnumre:

        ctx.emit(
            ikke_brofast_husnummer['husnummerid']
        )

/
