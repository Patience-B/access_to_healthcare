import geopandas as gpd
import pandas as pd
import osmnx as ox
import networkx as nx
import pyproj
import os
import logging
from typing import Tuple, List
from abc import ABC, abstractmethod
from shapely.geometry import Point, LineString, Polygon


from src.utils.s3_utils import check_s3_file_exists, save_to_s3_bucket, save_csv_to_s3_bucket
from src.utils.db_utils import db_to_df, connect_to_aws_db 
from src.utils.constants import SECRET_NAME, REGION_NAME, CountryISO
from src.utils.geo_utils import get_country_name_from_iso, get_custom_crs

class IsochroneGenerator(ABC):
    """Abstract base class for isochrone generation."""

    @abstractmethod
    def generate(
        self, 
        location_id: int, 
        location_coords: Tuple[float, float],
        tier_name: str,
        save_path: str,
        save_format: str = "csv"
    ) -> None:
        pass


class WalkingDistanceIsochroneGenerator(IsochroneGenerator):
    def __init__(self, ranges: List[int] = [15, 30, 45, 60]):
        self.ranges = ranges
        self.network_type = "walk"
        self.travel_speed = 4.5  # km/h for walking
        self.max_distance = 9.0  # km

    def generate(
        self, 
        location_id: int, 
        location_coords: Tuple[float, float],
        tier_name: str,
        save_path: str,
        save_format: str = "csv"
    ) -> None:
        """Generate walking distance isochrones."""
        return get_isochrone_polygons_for_facility(
            facility_coords=location_coords,
            tier_name=tier_name,
            facility_id=location_id,
            trip_times=self.ranges,
            network_type=self.network_type,
            travel_speed=self.travel_speed,
            max_distance=self.max_distance,
            save_path=save_path,
            save_format=save_format
        )


class DrivingDistanceIsochroneGenerator(IsochroneGenerator):
    def __init__(self, ranges: List[int] = [30, 60, 90, 120]):
        self.ranges = ranges
        self.network_type = "drive"
        self.travel_speed = 30  # km/h for driving
        self.max_distance = 60  # km

    def generate(
        self, 
        location_id: int, 
        location_coords: Tuple[float, float],
        tier_name: str,
        save_path: str,
        save_format: str = "csv"
    ) -> None:
        """Generate driving distance isochrones."""
        return get_isochrone_polygons_for_facility(
            facility_coords=location_coords,
            tier_name=tier_name,
            facility_id=location_id,
            trip_times=self.ranges,
            network_type=self.network_type,
            travel_speed=self.travel_speed,
            max_distance=self.max_distance,
            save_path=save_path,
            save_format=save_format
        )


def get_isochrone_polygons_for_facility(
    facility_coords: Tuple[float, float],
    tier_name: str,
    facility_id: int,
    trip_times: List[int],
    network_type: str,
    travel_speed: float,
    max_distance: float,
    save_path: str = "./botswana/",
    save_folder: str = "facility_isochrones",
    save_format: str = "csv",
    save_to_s3: bool = True
):
    """
    Generate isochrone polygons for a facility and save to local file system or S3.
    
    Args:
        facility_coords: (latitude, longitude) tuple
        tier_name: Facility tier name
        facility_id: Unique facility identifier
        trip_times: List of trip times in minutes
        network_type: Type of network ('walk' or 'drive')
        travel_speed: Travel speed in km/h
        max_distance: Maximum distance in km
        save_path: Local save path
        save_folder: Folder name for saving
        save_format: File format ('csv', 'geojson')
        save_to_s3: Whether to save to S3 bucket
    """
    
    assert tier_name in ['Tier2 health center', 'Tier3 provincial hospital', 
                        'Tier1 health post', 'Tier4 central hospital']
    
    assert network_type in ['walk', 'drive'], "network_type must be 'walk' or 'drive'"

    # Set up save paths
    transport_mode = "walking" if network_type == "walk" else "driving"
    FULL_SAVE_PATH = f"{save_path}/{save_folder}/{transport_mode}_{facility_id}.{save_format}"
    
    # Extract country name from save_path for S3 naming
    country_folder = os.path.basename(save_path.rstrip('/'))
    # S3_PATH = f"{country_folder}_isochrones/{transport_mode}_facility_{facility_id}_isochrones"
    S3_PATH = f"{country_folder}_isochrones/{tier_name}/{transport_mode}_facility_{facility_id}_isochrones"

    # Skip facility if it already exists locally
    if os.path.isfile(FULL_SAVE_PATH):
        print(f"{tier_name} {facility_id} ({transport_mode}) already exists locally.", flush=True)
        return

    # Check if exists in S3
    if save_to_s3 and check_s3_file_exists(f"{S3_PATH}.{save_format}"):
        print(f"{tier_name} {facility_id} ({transport_mode}) already exists in S3.", flush=True)
        return

    # Download the street network
    try:
        G = ox.graph_from_point(
            facility_coords,
            dist=max_distance * 1000,  # Convert km to meters
            dist_type="network",
            network_type=network_type
        )
        print(f"Downloaded {network_type} graph for {tier_name} {facility_id}")
    except Exception as e:
        print(f"Cannot download graph for {tier_name} {facility_id}: {e}", flush=True)
        return 

    # Find the centermost node
    center_node = ox.nearest_nodes(G, facility_coords[1], facility_coords[0])
    
    # Project the graph to 4326 first
    G = ox.project_graph(G, to_crs=4326)

    # Get country name from save_path for custom CRS
    country_folder = os.path.basename(save_path.rstrip('/'))
    try:
        country_name = get_country_name_from_iso(country_folder.upper())
    except ValueError:
        # Fallback to default if ISO code not found
        country_name = "Botswana"
    
    # Project the graph to the custom CRS
    projected_crs = get_custom_crs(country_name)
    G = ox.project_graph(G, to_crs=projected_crs)

    # Add time attribute based on transportation mode
    meters_per_minute = travel_speed * 1000 / 60  # km/hr to m/min
    for u, v, k, data in G.edges(data=True, keys=True):
        data['time'] = data['length'] / meters_per_minute

    # Generate isochrones
    isochrone_polys = {}
    edge_buff = 25 
    node_buff = 0

    for trip_time in sorted(trip_times, reverse=True):
        try:
            subgraph = nx.ego_graph(G, center_node, radius=trip_time, distance='time')

            node_points = [Point((data['x'], data['y'])) for node, data in subgraph.nodes(data=True)]
            nodes_gdf = gpd.GeoDataFrame({'id': list(subgraph.nodes())}, geometry=node_points)
            nodes_gdf = nodes_gdf.set_index('id')

            edge_lines = []
            for n_fr, n_to in subgraph.edges():
                f = nodes_gdf.loc[n_fr].geometry
                t = nodes_gdf.loc[n_to].geometry
                edge_lookup = G.get_edge_data(n_fr, n_to)[0].get('geometry', LineString([f, t]))
                edge_lines.append(edge_lookup)

            # Buffer nodes and edges
            n = nodes_gdf.buffer(node_buff).geometry
            e = gpd.GeoSeries(edge_lines).buffer(edge_buff).geometry
            all_gs = list(n) + list(e)
            new_iso = gpd.GeoSeries(all_gs).unary_union
            
            # Fill in surrounded areas
            if new_iso is not None:
                if hasattr(new_iso, 'exterior'):
                    new_iso = Polygon(new_iso.exterior)
                isochrone_polys[f"{trip_time}min_isochrone"] = gpd.GeoSeries([new_iso]).make_valid().iloc[0]
            else:
                isochrone_polys[f"{trip_time}min_isochrone"] = None
                
        except Exception as e:
            print(f"Error generating {trip_time}min isochrone for facility {facility_id}: {e}")
            isochrone_polys[f"{trip_time}min_isochrone"] = None

    # Add facility attributes
    isochrone_polys["lat"] = facility_coords[0]
    isochrone_polys["long"] = facility_coords[1]
    isochrone_polys["facility_id"] = facility_id
    isochrone_polys["tier_name"] = tier_name
    isochrone_polys["transport_mode"] = transport_mode

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(isochrone_polys, index=[0])
    
    # Ensure directory exists for local save
    os.makedirs(os.path.join(save_path, save_folder), exist_ok=True)
    
    print(f"Saving {tier_name} {facility_id} ({transport_mode}) to:", FULL_SAVE_PATH)

    # Save locally
    try:
        if save_format == "geojson":
            gdf.to_file(FULL_SAVE_PATH, driver='GeoJSON')
        elif save_format == "csv":
            gdf.to_csv(FULL_SAVE_PATH, index=False)
        
        print(f"{tier_name} {facility_id} ({transport_mode}) successfully saved locally.", flush=True)
    except Exception as e:
        print(f"Error saving locally: {e}")

    # Save to S3 if requested
    if save_to_s3:
        try:
            save_csv_to_s3_bucket(gdf, S3_PATH)
            print(f"{tier_name} {facility_id} ({transport_mode}) successfully saved to S3.", flush=True)
        except Exception as e:
            print(f"Error saving to S3: {e}")

    return gdf


def generate_isochrones(iso_code: str, transport_mode: str = "driving", tier_filter: str = "Tier4 central hospital"):
    """
    Main function to generate isochrones for all facilities in a country.
    
    Args:
        iso_code: Country ISO code
        transport_mode: Transportation mode ('walking' or 'driving')
        tier_filter: Health facility tier to filter for
    """
    
    # Validate inputs
    assert transport_mode.lower() in ['walking', 'driving'], "transport_mode must be 'walking' or 'driving'"
    assert tier_filter in ['Tier1 health post', 'Tier2 health center', 
                          'Tier3 provincial hospital', 'Tier4 central hospital'], \
                          f"Invalid tier_filter: {tier_filter}"
    
    # Get country name from ISO code
    try:
        country_name = get_country_name_from_iso(iso_code)
        print(f"Processing isochrones for {country_name} ({iso_code.upper()})")
    except ValueError as e:
        print(f"Error: {e}")
        return
    
    # Set up database connection and load data
    engine = connect_to_aws_db(SECRET_NAME, REGION_NAME)
    
    # Load facility data
    schema = iso_code.lower()
    query = f"SELECT * FROM {schema}.location"
    facility_df = pd.read_sql(query, engine)
    
    # Create country-specific directory structure
    country_path = f"./{country_name.lower().replace(' ', '_')}"
    os.makedirs(country_path, exist_ok=True)
    
    # Save facility data locally
    # facility_df.to_csv(f"{country_path}/{iso_code.lower()}_facility_df.csv", index=False)
    # print(facility_df.head())
    
    # Load population data
    # population_df = pd.read_sql(f"SELECT building_id, longitude, latitude, population_sum FROM {schema}.population", engine)
    # print("Population data shape:", population_df.shape)
    # print(population_df.head())
    
    # Filter facilities by specified tier
    filtered_facilities = facility_df[facility_df["tier_name"] == tier_filter]
    print(f"Found {len(filtered_facilities)} {tier_filter} facilities")
    
    # Create tier-specific save folder name (clean up tier name for folder)
    tier_folder_name = tier_filter.lower().replace(' ', '_')
    save_folder = f"facility_isochrones/{tier_folder_name}"
    
    # Create appropriate isochrone generator based on transport mode
    if transport_mode.lower() == "walking":
        isochrone_generator = WalkingDistanceIsochroneGenerator(ranges=[15, 30, 45, 60])
        print(f"Using walking isochrone generator with ranges: [15, 30, 45, 60] minutes")
    else:  # driving
        isochrone_generator = DrivingDistanceIsochroneGenerator(ranges=[30, 60, 90, 120])
        print(f"Using driving isochrone generator with ranges: [30, 60, 90, 120] minutes")
    
    # Generate isochrones for each filtered facility
    for idx, facility in filtered_facilities.iterrows():
        try:
            facility_coords = (facility['latitude'], facility['longitude'])
            facility_id = facility['id']
            tier_name = facility['tier_name']
            
            print(f"Processing facility {facility_id} ({tier_name}): {facility_coords}")
            
            # Generate isochrones using the updated save path and folder structure
            get_isochrone_polygons_for_facility(
                facility_coords=facility_coords,
                tier_name=tier_name,
                facility_id=facility_id,
                trip_times=isochrone_generator.ranges,
                network_type=isochrone_generator.network_type,
                travel_speed=isochrone_generator.travel_speed,
                max_distance=isochrone_generator.max_distance,
                save_path=country_path,
                save_folder=save_folder,
                save_format="csv",
                save_to_s3=True
            )
            
        except Exception as e:
            print(f"Error processing facility {facility_id}: {e}")
            continue
    
    print(f"Completed {transport_mode} isochrone generation for {tier_filter} facilities in {country_name} ({iso_code})")


# Example usage
if __name__ == "__main__":
    # Example 1: Generate driving isochrones for Tier 4 facilities
    generate_isochrones(
        iso_code="BWA", 
        transport_mode="driving", 
        tier_filter="Tier4 central hospital"
    )
    
    # Example 2: Generate walking isochrones for Tier 1 facilities
    # generate_isochrones(
    #     iso_code="UGA", 
    #     transport_mode="walking", 
    #     tier_filter="Tier1 health post"
    # )
    
    # Example 3: Test single facility with specific parameters
    # country_name = get_country_name_from_iso("BWA")  # Gets "Botswana"
    # country_path = f"./{country_name.lower().replace(' ', '_')}"
    # tier_folder = "facility_isochrones/tier4_central_hospital"
    # 
    # get_isochrone_polygons_for_facility(
    #     facility_coords=(-24.5678, 25.1234),  # latitude, longitude
    #     tier_name="Tier4 central hospital",
    #     facility_id=2827,
    #     trip_times=[30, 60, 90, 120],
    #     network_type="drive",
    #     travel_speed=30,
    #     max_distance=60,
    #     save_path=country_path,
    #     save_folder=tier_folder,
    #     save_to_s3=True
    # )
