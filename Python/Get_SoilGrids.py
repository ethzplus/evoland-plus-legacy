# download soil data from SoilGrids using OWSLib
from owslib.wcs import WebCoverageService
import rasterio
from rasterio import plot

# get bounding box from reference_raster
ref_grid = /data/spatial_reference_grid/ref_grid.tif

with rasterio.open(ref_grid) as src:
    bbox = src.bounds

maps=['https://maps.isric.org/mapserv?map=/map/ocs.map','https://maps.isric.org/mapserv?map=/map/sand.map']
identifiers=['ocs_0-30cm_mean','sand_0-30cm_mean']
file_names=['./data/Peru_soil_carbon_stock_0-5_mean.tif','./data/Peru_sand_carbon_stock_0-5_mean.tif']
for i in range(len(maps)):

    map=maps[i]
    identifier=identifiers[i]
    file_name=file_names[i]

    print(f"start download: {file_name}" )
    wcs = WebCoverageService(map, version='1.0.0')

    # The getCoverage method can now be used to fetch the map segment within the bounding box. Note the other parameters, Section 1 showed how to obtain them.
    response = wcs.getCoverage(
        identifier=identifier,
        crs='urn:ogc:def:crs:EPSG:4326',
        bbox=bbox,
        resx=0.002, resy=0.002,
        format='GEOTIFF_INT16')

    # create  a .tif file and save data into this file
    with open(file_name, 'wb') as file:
        file.write(response.read())
    print(f"download complete: {file_name}" )
    print("="*80)
