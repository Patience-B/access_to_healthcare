AFRICA_CRS = "ESRI:102022"
AFRICA_HEALTH_FACILITIES_URL = "https://raw.githubusercontent.com/afrimapr/afrihealthsites/master/data/df_who_sites.rda"

# Health indicators configuration - WHO OData API endpoints (NEW FORMAT)
HEALTH_INDICATORS = {
    "under_5_mortality": "https://ghoapi.azureedge.net/api/MDG_0000000007",
    "maternal_mortality": "https://ghoapi.azureedge.net/api/MDG_0000000026",
    "life_expectancy": "https://ghoapi.azureedge.net/api/WHOSIS_000001",
    "hiv_prevalence": "https://ghoapi.azureedge.net/api/HIV_0000000001",
    "tuberculosis_incidence": "https://ghoapi.azureedge.net/api/MDG_0000000020",
    "malaria_incidence": "https://ghoapi.azureedge.net/api/MALARIA005",
    "dtp3_immunization_coverage": "https://ghoapi.azureedge.net/api/WHS4_100",
    "skilled_birth_attendance": "https://ghoapi.azureedge.net/api/MDG_0000000025",
    "contraceptive_prevalence": "https://ghoapi.azureedge.net/api/SDGFPALL",
    "uhc_service_coverage_index": "https://ghoapi.azureedge.net/api/UHC_INDEX_REPORTED",
    "physicians_per_1000": "https://ghoapi.azureedge.net/api/HWF_0001",
    "nurses_per_1000": "https://ghoapi.azureedge.net/api/HWF_0006",
    "hospital_beds_per_1000": "https://ghoapi.azureedge.net/api/WHS6_102",
    "out_of_pocket_expenditure": "https://ghoapi.azureedge.net/api/GHED_OOP_pc_US_SHA2011",
}

PROJECTION = "EPSG:4326"

CRS_MAPPING = {
    "RWA": "EPSG:32735",  # Rwanda - UTM Zone 35S
    "TGO": "EPSG:32631",  # Togo - UTM Zone 31N
    "GHA": "EPSG:32630",  # Ghana - UTM Zone 30N
    "BWA": "EPSG:32734",  # Botswana - UTM Zone 34S
    "SEN": "EPSG:32628",  # Senegal - UTM zone 28N
    "MWI": "EPSG:32736",  # Malawi - UTM zone 36S
    "SLE": "EPSG:32629",
    "SWZ": "EPSG:4326",  # eSwatini - UTM Zone 35S
    "BEN": "EPSG:32631",
    "AGO": "EPSG:32733",  # Angola - UTM Zone 33S
    "BDI": "EPSG:32735",  # Burundi - UTM Zone 35S
    "BFA": "EPSG:32630",  # Burkina Faso - UTM Zone 30N
    "CAF": "EPSG:32635",  # Central African Republic - UTM Zone 35N
    "CIV": "EPSG:32630",  # Côte d'Ivoire - UTM Zone 30N
    "CMR": "EPSG:32632",  # Cameroon - UTM Zone 32N
    "COD": "EPSG:32735",  # Democratic Republic of the Congo - UTM Zone 35S
    "DZA": "EPSG:32631",  # Algeria - UTM Zone 31N
    "GIN": "EPSG:32629",  # Guinea - UTM Zone 29N
    "KEN": "EPSG:32737",  # Kenya - UTM Zone 37S
    "LBR": "EPSG:32629",  # Liberia - UTM Zone 29N
    "MLI": "EPSG:32629",  # Mali - UTM Zone 29N
    "MOZ": "EPSG:32736",  # Mozambique - UTM Zone 36S
    "NER": "EPSG:32632",  # Niger - UTM Zone 32N
    "NGA": "EPSG:32632",  # Nigeria - UTM Zone 32N
    "SSD": "EPSG:32635",  # South Sudan - UTM Zone 35N
    "TZA": "EPSG:32737",  # Tanzania - UTM Zone 37S
    "UGA": "EPSG:32636",  # Uganda - UTM Zone 36N
    "ZAF": "EPSG:32735",  # South Africa - UTM Zone 35S
    "ZMB": "EPSG:32735",  # Zambia - UTM Zone 35S
}


FLOOR_HEIGHT_IN_METERS = 3

# --- AWS DB Secrets ---
SECRET_NAME = "healthcare-internal-africa-open-data-haod-credentials"
REGION_NAME = "eu-west-1"

from enum import Enum

class CountryISO(Enum):
    AGO = "Angola"
    BDI = "Burundi"
    BEN = "Benin"
    BFA = "Burkina Faso"
    BWA = "Botswana"
    CAF = "Central African Republic"
    CIV = "Côte d'Ivoire"
    CMR = "Cameroon"
    COD = "Democratic Republic of the Congo"
    DZA = "Algeria"
    GHA = "Ghana"
    GIN = "Guinea"
    KEN = "Kenya"
    LBR = "Liberia"
    MLI = "Mali"
    MOZ = "Mozambique"
    MWI = "Malawi"
    NER = "Niger"
    NGA = "Nigeria"
    RWA = "Rwanda"
    SEN = "Senegal"
    SLE = "Sierra Leone"
    SSD = "South Sudan"
    TGO = "Togo"
    TZA = "Tanzania"
    UGA = "Uganda"
    ZAF = "South Africa"
    ZMB = "Zambia"
    SWZ = "Swaziland"


