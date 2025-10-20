from owslib.wcs import WebCoverageService
import rasterio
import os
from pyproj import CRS, Transformer

def download_soilgrids(ref_grid, out_root):
    """
    Download SoilGrids variables (mean only, all depths) for the extent of a reference raster.
    
    Parameters
    ----------
    ref_grid : str
        Path to reference raster (used for bounding box).
    out_root : str
        Root directory where variable folders will be created.
    """

    # SoilGrids variables
    soil_vars = [
        "bdod",  # bulk density
        "cfvo",  # coarse fragments
        "clay",
        "silt",
        "sand",
        "phh2o", # soil pH in H2O
        "soc",   # soil organic carbon density
        "ocs"    # organic carbon stock
    ]

    # Standard depth intervals (no aggregates)
    depths = [
        "0-5cm",
        "5-15cm",
        "15-30cm",
        "30-60cm",
        "60-100cm",
        "100-200cm"
    ]

    quantile = "mean"

    # SoilGrids WCS URL template
    wcs_url_template = "https://maps.isric.org/mapserv?map=/map/{var}.map"

    # Open reference raster
    with rasterio.open(ref_grid) as src:
        bbox_src = src.bounds
        crs_src = CRS.from_wkt(src.crs.to_wkt())

    # Reproject bbox to EPSG:4326 if needed
    if crs_src.to_epsg() != 4326:
        transformer = Transformer.from_crs(crs_src, CRS.from_epsg(4326), always_xy=True)
        minx, miny = transformer.transform(bbox_src.left, bbox_src.bottom)
        maxx, maxy = transformer.transform(bbox_src.right, bbox_src.top)
        bbox = (minx, miny, maxx, maxy)
    else:
        bbox = (bbox_src.left, bbox_src.bottom, bbox_src.right, bbox_src.top)

    # SoilGrids native resolution (~250m)
    resx = resy = 0.0020833333

    # Loop variables
    for var in soil_vars:
        wcs_url = wcs_url_template.format(var=var)
        print(f"\nConnecting to WCS for variable '{var}' …")
        wcs = WebCoverageService(wcs_url, version="1.0.0")

        available_coverages = list(wcs.contents.keys())

        # Create variable directory
        var_out_dir = os.path.join(out_root, var)
        os.makedirs(var_out_dir, exist_ok=True)

        for depth in depths:
            cov_id = f"{var}_{depth}_{quantile}"
            if cov_id not in available_coverages:
                print(f"  Skipping {cov_id} (not available)")
                continue

            out_name = f"{cov_id}.tif"
            out_path = os.path.join(var_out_dir, out_name)

            print(f"  Downloading: {cov_id} → {out_path}")
            try:
                response = wcs.getCoverage(
                    identifier=cov_id,
                    crs="urn:ogc:def:crs:EPSG:4326",
                    bbox=bbox,
                    resx=resx, resy=resy,
                    format="GEOTIFF_INT16"
                )
                with open(out_path, "wb") as f:
                    f.write(response.read())
                print(f"    Done: {out_path}")
            except Exception as e:
                print(f"    ERROR downloading {cov_id}: {e}")
