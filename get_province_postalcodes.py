import geopandas
import os
import argparse


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", 
                        help="Ouput path to store the shape file", type=str, 
                        default="catalonia/postalcodes.geojson")
    parser.add_argument("-p", "--provinces", 
                        help="Array of spanish provincial codes that the shape file will contian", nargs='+',type=str, 
                        default = ['08', '17', '25', '43'])
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    print(args.provinces)

    os.makedirs(os.path.dirname(args.output),exist_ok=True)
    gdf = geopandas.read_file("codigos_postales.shp")

    gdf_catalonia = gdf[(gdf['COD_POSTAL'].str.startswith(tuple(args.provinces))  | (gdf['COD_POSTAL'] == '22583') | (gdf['COD_POSTAL'] == '22584'))]
    
    gdf_catalonia.to_file(args.output, driver="GeoJSON")
    
