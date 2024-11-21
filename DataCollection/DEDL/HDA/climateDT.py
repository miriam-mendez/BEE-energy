variables = [
    # {
    #     'paramId':130,
    #     'name':'Temperature',
    #     'shortName': 't',
    # },
    {
        'paramId':134,
        'name':'Surface pressure',
        'shortName': 'sp',
    },
    {
        'paramId':164,
        'name':'Total cloud cover',
        'shortName': 'tcc',
    },
    {
        'paramId':165,
        'name':'10 metre U wind component',
        'shortName': '10u',
    },
    {
        'paramId':166,
        'name':'10 metre V wind component',
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

query ={
    key: {"eq": value}
    for key, value in {
        "class": "d1",             # fixed 
        "dataset": "climate-dt",   # fixed climate-dt access
        "activity": "ScenarioMIP", # activity + experiment + model (go together)
        "experiment": "SSP3-7.0",  # activity + experiment + model (go together)
        "model": "IFS-NEMO",       # activity + experiment + model (go together)
        "generation": "1",         # fixed Specifies the generation of the dataset, which can be incremented as required (latest is 1)
        "realization": "1",        # fixed Specifies the climate realization. Default 1. Based on perturbations of initial conditions
        "resolution": "high",      # standard/ high 
        "expver": "0001",          # fixed experiment version 
        "stream": "clte",          # fixed climate
        "time": f"{'/'.join([f'{i:02}00' for i in range(24)])}",  # All the hourly slot(s)
        "type": "fc",              # fixed forecasted fields
        "levtype": "sfc",          # Surface fields (levtype=sfc), Height level fields (levtype=hl), Pressure level fields (levtype=pl), Model Level (Levtype=ml)
        "param": "/".join([str(x['paramId']) for x  in variables]), # To set by the user
    }.items()
}

collection =["EO.ECMWF.DAT.DT_CLIMATE_ADAPTATION"]

