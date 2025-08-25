import geopandas as gpd
import logging
import pyproj
import osmnx as ox
from src.utils.constants import AFRICA_CRS
from src.utils.constants import CountryISO

logger = logging.getLogger(__name__)

def check_unmatched(adm_level_1_proj, joined):
    # Find unmatched entries where shapeID is NaN (no match from adm_level_1)
    unmatched = joined[joined.isna().any(axis=1)].copy()
    buffer_distance = 100

    while (not unmatched.empty) and (buffer_distance < 1000):
        logger.info(f"{len(unmatched)} centroids unmatched; applying intersect join with buffer {buffer_distance}")

        adm_level_1_proj["geometry"] = adm_level_1_proj.geometry.buffer(buffer_distance)

        unmatched = gpd.sjoin(
            unmatched[["id", "name", "geometry", "centroid", "geometry_cp"]],
            adm_level_1_proj, 
            how="left", 
            predicate="intersects"
        ).drop(columns="index_right")

        # Replace unmatched rows in original joined DataFrame
        joined.update(unmatched[~unmatched.isna().any(axis=1)])
        unmatched = unmatched[unmatched.isna().any(axis=1)].copy()

        buffer_distance += 100
    return joined

def adm_spatial_join(adm_level_1_name, adm_level_1, adm_level_2):

    projected_crs = AFRICA_CRS
    adm_level_1_proj = adm_level_1.to_crs(projected_crs)
    adm_level_2_proj = adm_level_2.to_crs(projected_crs)

    # Compute centroids
    adm_level_2_proj["centroid"] = adm_level_2_proj.geometry.centroid
    adm_level_2_proj["geometry_cp"] = adm_level_2_proj["geometry"] # temporarily saving polygon geometry
    adm_level_2_proj = gpd.GeoDataFrame(adm_level_2_proj, geometry="centroid", crs=adm_level_2_proj.crs) # assign centroid as geometry

    # Rename adm_level_1 df columns ahead of spatial join
    adm_hierarchy_cols = [c for c in adm_level_1_proj.columns if "id" in c]
    adm_level_1_proj = adm_level_1_proj[adm_hierarchy_cols + ["geometry"]]\
        .rename(columns={"id":f"{adm_level_1_name}_id"})

    logger.info(f"Calculating centroid-based spatial relationship between administrative levels")
    # Perform the spatial join (assign admin1 to each admin2 polygon)
    joined = gpd.sjoin(
        adm_level_2_proj,
        adm_level_1_proj, 
        how="left", 
        predicate="within"
    ).drop(columns="index_right")

    joined = check_unmatched(adm_level_1_proj, joined.copy())

    joined = gpd.GeoDataFrame(
        joined.drop(
            columns=["geometry","centroid"]# drop centroids and other columns not required
        ).rename(
            columns={"geometry_cp":"geometry"}
        ), 
        geometry="geometry", 
        crs=adm_level_2_proj.crs # re-assign polygon as geometry
    ).to_crs("EPSG:4326") # convert back to original CRS
    
    return joined

def get_country_name_from_iso(iso_code: str) -> str:
    """Get country name from ISO code using CountryISO enum."""
    try:
        return CountryISO[iso_code.upper()].value
    except KeyError:
        raise ValueError(f"Invalid ISO code: {iso_code}. Available codes: {list(CountryISO.__members__.keys())}")

def get_custom_crs(country_name): 
    """Get custom projected CRS for the country."""
    country = ox.geocode_to_gdf(country_name)
    centre = country.representative_point().geometry[0]

    # Custom Southern Hemisphere UTM band centred on the country
    projected_crs_string = f"+proj=tmerc +lat_0={centre.y} +lon_0={centre.x} +k=0.9996 +llcrnrlat={country.bounds.miny[0]} +llcrnrlon={country.bounds.minx[0]} +urcrnrlat={country.bounds.maxy[0]} +urcrnrlon={country.bounds.maxx[0]} +south +datum=WGS84 +units=m +no_defs +type=crs"
    print("\n".join(projected_crs_string.split(" ")))

    return pyproj.CRS(projected_crs_string)