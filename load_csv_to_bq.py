#!/usr/bin/env python
# coding: utf-8

# In[3]:


from google.cloud import bigquery
from google.oauth2 import service_account


# Credentials Init

# In[4]:


credentials = service_account.Credentials.from_service_account_file(
'coastal-set-346210-7412c02a4f9f.json')
project_id = 'coastal-set-346210'

client = bigquery.Client(credentials= credentials,project=project_id)


# load cigar_af csv to bq table

# In[24]:




# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

table_id = "coastal-set-346210.Cigar_Review_DataSet.cigar_af_raw_data"

client.delete_table(table_id, not_found_ok=True)  # Make an API request.
print("Deleted table '{}'.".format(table_id))

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("int64_field_0", "INTEGER"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("length", "STRING"),
        bigquery.SchemaField("gauge", "STRING"),
        bigquery.SchemaField("strength", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("size", "STRING"),
        bigquery.SchemaField("filler", "STRING"),
        bigquery.SchemaField("binder", "STRING"),
        bigquery.SchemaField("wrapper", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("price", "STRING"),
        bigquery.SchemaField("issue", "STRING"),
        bigquery.SchemaField("reviews", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("box_date", "STRING"),
        bigquery.SchemaField("designation", "STRING"),
    ],
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://cigar_bucket/Raw_Data/cigaraf_detail.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))


# load cigar_geek csv to bq table

# In[25]:




# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

table_id = "coastal-set-346210.Cigar_Review_DataSet.cigar_geek_raw_data"

client.delete_table(table_id, not_found_ok=True)  # Make an API request.
print("Deleted table '{}'.".format(table_id))

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("int64_field_0", "INTEGER"),
        bigquery.SchemaField("Brand", "STRING"),
        bigquery.SchemaField("Cigar_Name", "STRING"),
        bigquery.SchemaField("Length", "FLOAT"),
        bigquery.SchemaField("Ring_Gauge", "INTEGER"),
        bigquery.SchemaField("Country_Manufactured", "STRING"),
        bigquery.SchemaField("Filler", "STRING"),
        bigquery.SchemaField("Binder", "STRING"),
        bigquery.SchemaField("Wrapper", "STRING"),
        bigquery.SchemaField("Color", "STRING"),
        bigquery.SchemaField("Strength", "STRING"),
        bigquery.SchemaField("Shape", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("Notes", "STRING"),
        bigquery.SchemaField("rating", "FLOAT"),
        bigquery.SchemaField("Average_Member_Purchase_Price", "FLOAT"),
    ],
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://cigar_bucket/Raw_Data/cigargeek_detail.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))


# load cigar_scanner csv to bq table

# In[26]:




# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

table_id = "coastal-set-346210.Cigar_Review_DataSet.cigar_scanner_raw_data"

client.delete_table(table_id, not_found_ok=True)  # Make an API request.
print("Deleted table '{}'.".format(table_id))

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("int64_field_0", "INTEGER"),
        bigquery.SchemaField("ProductId", "INTEGER"),
        bigquery.SchemaField("AdUrl", "STRING"),
        bigquery.SchemaField("AdditionalAttributes", "STRING"),
        bigquery.SchemaField("BandHistory", "STRING"),
        bigquery.SchemaField("CanonicalUrl", "STRING"),
        bigquery.SchemaField("CustomUUID", "STRING"),
        bigquery.SchemaField("CustomUserId", "STRING"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("ImageOfSingleHeight", "FLOAT"),
        bigquery.SchemaField("ImageOfSingleUrl", "STRING"),
        bigquery.SchemaField("ImageOfSingleWidth", "FLOAT"),
        bigquery.SchemaField("ImageUrl", "STRING"),
        bigquery.SchemaField("ImageUrlSmall", "STRING"),
        bigquery.SchemaField("Images", "STRING"),
        bigquery.SchemaField("IsCustom", "BOOLEAN"),
        bigquery.SchemaField("LineId", "INTEGER"),
        bigquery.SchemaField("MaxBoxQty", "FLOAT"),
        bigquery.SchemaField("MinBoxQty", "FLOAT"),
        bigquery.SchemaField("MyCigarFeatures", "STRING"),
        bigquery.SchemaField("MyNote", "STRING"),
        bigquery.SchemaField("MyRating", "STRING"),
        bigquery.SchemaField("Name", "STRING"),
        bigquery.SchemaField("RelatedLines", "STRING"),
        bigquery.SchemaField("Shapes", "STRING"),
        bigquery.SchemaField("SocialPosts", "INTEGER"),
        bigquery.SchemaField("Tags", "STRING"),
        bigquery.SchemaField("UserImageUrl", "STRING"),
        bigquery.SchemaField("Attributes_ManufacturerValueId", "STRING"),
        bigquery.SchemaField("Attributes_StrengthValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_OriginValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_WrapperValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_BinderValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_FillerValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_RollingTypeValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_WrapperColorValueId", "FLOAT"),
        bigquery.SchemaField("Attributes_Manufacturer", "STRING"),
        bigquery.SchemaField("Attributes_ManufacturerDescription", "STRING"),
        bigquery.SchemaField("Attributes_Origin", "STRING"),
        bigquery.SchemaField("Attributes_OriginDescription", "STRING"),
        bigquery.SchemaField("Attributes_Strength", "STRING"),
        bigquery.SchemaField("Attributes_Wrapper", "STRING"),
        bigquery.SchemaField("Attributes_WrapperDescription", "STRING"),
        bigquery.SchemaField("Attributes_WrapperColor", "STRING"),
        bigquery.SchemaField("Attributes_WrapperColorDescription", "STRING"),
        bigquery.SchemaField("Attributes_Binder", "STRING"),
        bigquery.SchemaField("Attributes_BinderDescription", "STRING"),
        bigquery.SchemaField("Attributes_Filler", "STRING"),
        bigquery.SchemaField("Attributes_FillerDescription", "STRING"),
        bigquery.SchemaField("Attributes_RollingType", "STRING"),
        bigquery.SchemaField("Attributes_RollingTypeDescription", "STRING"),
        bigquery.SchemaField("Attributes_RingGauge", "FLOAT"),
        bigquery.SchemaField("Attributes_RingGaugeDescription", "STRING"),
        bigquery.SchemaField("Attributes_Length", "STRING"),
        bigquery.SchemaField("Attributes_LengthDescription", "STRING"),
        bigquery.SchemaField("Attributes_Section", "STRING"),
        bigquery.SchemaField("Attributes_Shape", "STRING"),
        bigquery.SchemaField("Attributes_ShapeDescription", "STRING"),
        bigquery.SchemaField("Attributes_SinglePackaging", "STRING"),
        bigquery.SchemaField("Attributes_IsSpecific", "BOOLEAN"),
        bigquery.SchemaField("Attributes_MasterLine", "STRING"),
        bigquery.SchemaField("Attributes_CARating", "FLOAT"),
        bigquery.SchemaField("Attributes_CIRating", "FLOAT"),
        bigquery.SchemaField("Aux_ShippingDate", "TIMESTAMP"),
        bigquery.SchemaField("Aux_ShippingCutOffTime", "TIMESTAMP"),
        bigquery.SchemaField("Aux_Availabilities", "STRING"),
        bigquery.SchemaField("Aux_RatingSummaries", "STRING"),
        bigquery.SchemaField("NeptunePrices_NeptuneSinglePriceMin", "FLOAT"),
        bigquery.SchemaField("NeptunePrices_NeptuneSinglePriceMax", "FLOAT"),
        bigquery.SchemaField("NeptunePrices_NeptuneBoxPriceMin", "FLOAT"),
        bigquery.SchemaField("NeptunePrices_NeptuneBoxPriceMax", "FLOAT"),
        bigquery.SchemaField("NeptunePrices_DisplayNeptunePrices", "BOOLEAN"),
        bigquery.SchemaField("PartnerPrices_PartnerSinglePriceMin", "STRING"),
        bigquery.SchemaField("PartnerPrices_PartnerSinglePriceMax", "STRING"),
        bigquery.SchemaField("PartnerPrices_PartnerBoxPriceMin", "STRING"),
        bigquery.SchemaField("PartnerPrices_PartnerBoxPriceMax", "STRING"),
        bigquery.SchemaField("Prices_SinglePriceMin", "FLOAT"),
        bigquery.SchemaField("Prices_SinglePriceMax", "FLOAT"),
        bigquery.SchemaField("Prices_BoxPriceMin", "FLOAT"),
        bigquery.SchemaField("Prices_BoxPriceMax", "FLOAT"),
        bigquery.SchemaField("Prices_DisplayPartnerPrices", "STRING"),
        bigquery.SchemaField("RatingSummary_AverageRating", "FLOAT"),
        bigquery.SchemaField("RatingSummary_RatingCount", "INTEGER"),
        bigquery.SchemaField("RatingSummary_Rated5", "INTEGER"),
        bigquery.SchemaField("RatingSummary_Rated4", "INTEGER"),
        bigquery.SchemaField("RatingSummary_Rated3", "INTEGER"),
        bigquery.SchemaField("RatingSummary_Rated2", "INTEGER"),
        bigquery.SchemaField("RatingSummary_Rated1", "INTEGER"),
    ],
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://cigar_bucket/Raw_Data/cigar_scanner_products_combined.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))

