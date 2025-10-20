import geopandas as gpd
import rasterio
from rasterio.features import rasterize
from scipy.ndimage import distance_transform_edt
import numpy as np
from rasterio.mask import mask
from shapely.geometry import mapping


def calculate_distances(shapefile_path, reprojected_raster_path, repo_peru_boundary_path, output_path):
    # Load hydrological units and Peru boundary
    hydro_units = gpd.read_file(shapefile_path)
    peru_boundary = gpd.read_file(repo_peru_boundary_path)
    peru_boundary_geom = [mapping(shp) for shp in peru_boundary.geometry]

    # Open template raster to get dimensions and CRS
    with rasterio.open(reprojected_raster_path) as template:
        meta = template.meta.copy()
        template_crs = template.crs

    # Reproject hydro_units to match the CRS of the template raster
    hydro_units = hydro_units.to_crs(template_crs)

    # Rasterize hydrological units within the template raster's extent
    rasterized_hydro = rasterize(
        [(geom, 1) for geom in hydro_units.geometry],
        out_shape=(meta['height'], meta['width']),
        transform=meta['transform'],
        fill=0,
        dtype=rasterio.uint8
    )
    print("Rasterized hydrological units.")

    # Create a temporary raster of rasterized hydrological units
    with rasterio.open('temp.tif', 'w', **meta) as temp_raster:
        temp_raster.write(rasterized_hydro, 1)

    # Apply Peru boundary as mask
    with rasterio.open('temp.tif') as temp_raster:
        out_image, out_transform = mask(temp_raster, peru_boundary_geom, crop=True, all_touched=True, invert=False)
        out_meta = temp_raster.meta.copy()
        out_meta.update(
            {"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})

    # Calculate distances only within the masked (Peru boundary) area
    distances = distance_transform_edt(out_image[0] == 0) * abs(out_transform[0])
    print("Calculated distances within the Peru boundary.")

    # Save the distances within Peru boundary to a new raster
    with rasterio.open(output_path, 'w', **out_meta) as out_raster:
        out_raster.write(distances.astype(np.int32), 1)
        print(f"Saved distances to {output_path}.")