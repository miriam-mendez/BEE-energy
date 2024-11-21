variables = [
    {
        'paramId':165,
        'name': '10 metre U wind component',
        'shortName': '10u',
    },
    {
        'paramId':166,
        'name': '10 metre V wind component',
        'shortName': '10v',
    },
    {
        'paramId':167,
        'name': '2 metre temperature',
        'shortName': '2t',
    },
    {
        'paramId':168,
        'name': '2 metre dewpoint temperature',
        'shortName': '2d',
    },
    {
        'paramId':3020,
        'name': 'Visibility',
        'shortName': 'vis',
    },
    {
        'paramId':169,
        'name': 'Surface short-wave (solar) radiation downwards',
        'shortName': 'ssrd',
    },
    {
        'paramId':175,
        'name': 'Surface long-wave (thermal) radiation downwards',
        'shortName': 'strd',
    }
]

query = {
    key: {"eq": value}
    for key, value in {
        "class": "d1",              # fixed 
        "dataset": "extremes-dt",   # fixed climate-dt access
        "expver": "0001",           # fixed experiment version 
        "stream": "oper",            # fixed climate
        "time": f"{'/'.join([f'{i:02}00' for i in range(24)])}",  # choose the hourly slot(s)
        "type": "fc",               # fixed forecasted fields
        "levtype": "sfc",           # Surface fields (levtype=sfc), Height level fields (levtype=hl), Pressure level fields (levtype=pl), Model Level (Levtype=ml)
        "param": "/".join([str(x['paramId']) for x  in variables]), # To set by the user
    }.items()
}

collection = ["EO.ECMWF.DAT.DT_EXTREMES"]

