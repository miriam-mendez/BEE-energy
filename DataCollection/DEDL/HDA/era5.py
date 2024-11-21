variables = [
    {
        'CDSLongName': '10m_u_component_of_wind',
        'CDSShortName': '10u',
        'name': 'windSpeedEast'
    },
    {
        'CDSLongName': '10m_v_component_of_wind',
        'CDSShortName': '10v',
        'name': 'windSpeedNorth'
    },
    {
        'CDSLongName': '2m_dewpoint_temperature',
        'CDSShortName': '2d',
        'name': 'dewAirTemperature'
    },
    {
        'CDSLongName': '2m_temperature',
        'CDSShortName': '2t',
        'name': 'airTemperature'
    },
    {
        'CDSLongName': 'leaf_area_index_high_vegetation',
        'CDSShortName': 'lai_hv',
        'name': 'highVegetationRatio'
    },
    {
        'CDSLongName': 'leaf_area_index_low_vegetation',
        'CDSShortName': 'lai_lv',
        'name': 'lowVegetationRatio'
    },
    {
        'CDSLongName': 'total_precipitation',
        'CDSShortName': 'tp',
        'name': 'totalPrecipitation'
    },
    {
        'CDSLongName': 'surface_solar_radiation_downwards',
        'CDSShortName': 'ssrd',
        'name': 'GHI'
    },
    {
        'CDSLongName': 'forecast_albedo',
        'CDSShortName': 'fal',
        'name': 'albedo'
    },
    {
        'CDSLongName': 'soil_temperature_level_4',
        'CDSShortName': 'stl4',
        'name': 'soilTemperature'
    },
    {
        'CDSLongName': 'volumetric_soil_water_layer_4',
        'CDSShortName': 'swvl4',
        'name': 'soilWaterRatio'
    },
    {
        'CDSLongName': 'surface_pressure',
        'CDSShortName': 'sp',
        'name': 'surfacePressure'
    }]

query = {
    key: {"eq": value}
    for key, value in {
        "format": "grib",
        "variable": [x['CDSLongName'] for x in variables],
        'day': [f'{i:02}' for i in range(1, 32)],
        "time": [f'{i:02}:00' for i in range(24)]
    }.items()
}

collection = ["EO.ECMWF.DAT.REANALYSIS_ERA5_SINGLE_LEVELS"]

