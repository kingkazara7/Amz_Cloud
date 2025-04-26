from pystac_client import Client
from rasterio.coords import BoundingBox
from typing import List
from typing import Tuple
import shapely
from shapely.geometry import box
from shapely.geometry import mapping
import pandas as pd

def get_all_collections():
    landsat_stac = Client.open('https://landsatlook.usgs.gov/stac-server')
    return [i.id for i in landsat_stac.get_collections()]

def query_landsat_stac(intersects: Tuple[int, int, int, int] = None, datetime: str = None, collections: List[str] = None, query: dict = None, max_items: int = None) -> List[dict]:
    """
    Query the Landsat STAC server for data with the follwoing input parameters:
    
    Parameters:
        intersects: A GeoJSON object representing the geographical area of interest (default is None)
        daterange (str): A string specifying the date range for the query in the format 'YYYY-MM-DD/YYYY-MM-DD' (default is None)
        collections (List[str]): A list of strings specifying the Landsat collections to search within e.g. ['landsat-c2l2-sr'] (default is None)
        query (dict): A dictionary for additional query parameters (default is None)
        max_items (int): An integer specifying the maximum number of items to return (default is None)

    """
    stac = Client.open('https://landsatlook.usgs.gov/stac-server')
    
    if intersects or datetime or collections is not None :
        intersects = mapping(box(*intersects))
        query = stac.search(collections=collections,
                            intersects=intersects,
                            datetime=datetime,
                            query=query)
        return query.item_collection().to_dict()
    else:
        return print('Must set at least one of the following parameters: collections, intersects, datetime or query before continuing.')


if __name__ == '__main__':
    for i in get_all_collections():
        print(i)
    #example usage:
  
    #(min_longitude, min_latitude, max_longitude, max_latitude)
    geo_bbox = (-115.359, 35.6763, -113.6548, 36.4831)
    query_return = query_landsat_stac(intersects=geo_bbox, datetime='2015-07-01/2015-08-01', collections=['landsat-c2l2-sr'], query={})
    print(query_return)


