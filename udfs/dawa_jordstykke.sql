CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDFS.dawa_jordstykke (TXIDFRA DECIMAL(18,0), TXIDTIL DECIMAL(18,0))
emits (
ejerlavkode decimal(9,0),
matrikelnr varchar(255),
kommunekode varchar(255),
regionskode varchar(255),
sognekode varchar(255),
retskredskode varchar(255),
esrejendomsnr varchar(255),
udvidet_esrejendomsnr varchar(255),
sfeejendomsnr varchar(255),
geometri varchar(10000),
featureid varchar(255),
fælleslod boolean,
moderjordstykke decimal(18,0),
registreretareal decimal(18,0),
arealberegningsmetode varchar(255),
vejareal decimal(18,0),
vejarealberegningsmetode varchar(255),
vandarealberegningsmetode varchar(255),
visueltcenter varchar(10000),
bbox varchar(10000)
) AS


utils = exa.import_script("ETL_UDFS.DAWA_UTILS")


def run(ctx):

    jordstykker = utils.get('jordstykke', ctx.TXIDTIL, ctx.TXIDFRA)

    for jordstykke in jordstykker:
        visueltcenter = utils.parse_point(jordstykke['visueltcenter'])
        bbox = utils.parse_polygon(jordstykke['bbox'])
        geometri = utils.parse_polygon(jordstykke['geometri'])

        ctx.emit(
            jordstykke['ejerlavkode'],
            jordstykke['matrikelnr'],
            jordstykke['kommunekode'],
            jordstykke['regionskode'],
            jordstykke['sognekode'],
            jordstykke['retskredskode'],
            jordstykke['esrejendomsnr'],
            jordstykke['udvidet_esrejendomsnr'],
            jordstykke['sfeejendomsnr'],
			geometri,
            jordstykke['featureid'],
            bool(jordstykke['fælleslod']),
            jordstykke['moderjordstykke'],
            jordstykke['registreretareal'],
            jordstykke['arealberegningsmetode'],
            jordstykke['vejareal'],
            jordstykke['vejarealberegningsmetode'],
            jordstykke['vandarealberegningsmetode'],
			bbox,
			visueltcenter

           
        )

/
