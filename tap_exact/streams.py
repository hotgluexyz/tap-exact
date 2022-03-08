"""Stream type classes for tap-exact."""

from msilib.schema import Property
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th
from tomlkit import string  # JSON Schema typing helpers

from tap_exact.client import ExactStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class UsersStream(ExactStream):
    """Define custom stream."""
    name = "users"
    path = "/users"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="The user's system ID"
        ),
        th.Property(
            "age",
            th.IntegerType,
            description="The user's age in years"
        ),
        th.Property(
            "email",
            th.StringType,
            description="The user's email address"
        ),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property(
            "state",
            th.StringType,
            description="State name in ISO 3166-2 format"
        ),
        th.Property("zip", th.StringType),
    ).to_dict()


class GroupsStream(ExactStream):
    """Define custom stream."""
    name = "groups"
    path = "/groups"
    primary_keys = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()


class ItemsStream(ExactStream):
    name = "items"
    path = "/"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", string),
        th.Property("title", th.ObjectType(
            th.Property("@type", string)
        )),
        th.Property("updated", th.DateTimeType),
        th.Property("author", th.ObjectType(
            th.Property("name", string),
        )),
        th.Property("link", th.ObjectType(
            th.Property("@rel", string),
            th.Property("@title", string),
            th.Property("@href", string),
        )),
        th.Property("category", th.ObjectType(
            th.Property("@scheme", string),
            th.Property("@term", string),
        )),


    )
    
{
 'id': "https://start.exactonline.com/api/v1/64850/bulk/logistics/Items(guid'559e9212-af04-46cc-8ca1-87f03adf0dcf')",
 'title': {'@type': 'text'},
 'updated': '2022-03-08T09:35:42Z',
 'author': {'name': None},
    'link': {'@rel': 'edit',
        '@title': 'Item',
        '@href': "Items(guid'559e9212-af04-46cc-8ca1-87f03adf0dcf')"},
    'category': {'@term': 'Exact.Web.Api.Models.Item',
    '@scheme': 'http://schemas.microsoft.com/ado/2007/08/dataservices/scheme'},
    'content': {'@type': 'application/xml',
 'm:properties': {'d:ID': {'@m:type': 'Edm.Guid',
 '#text': '559e9212-af04-46cc-8ca1-87f03adf0dcf'},
 'd:StandardSalesPrice': {'@m:type': 'Edm.Double', '#text': '99'},
   'd:Class_01': {'@m:null': 'true'},
   'd:Class_02': {'@m:null': 'true'},
   'd:Class_03': {'@m:null': 'true'},
   'd:Class_04': {'@m:null': 'true'},
   'd:Class_05': {'@m:null': 'true'},
   'd:Class_06': {'@m:null': 'true'},
   'd:Class_07': {'@m:null': 'true'},
   'd:Class_08': {'@m:null': 'true'},
   'd:Class_09': {'@m:null': 'true'},
   'd:Class_10': {'@m:null': 'true'},
   'd:Code': '002',
   'd:CopyRemarks': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:CostPriceCurrency': 'USD',
   'd:CostPriceNew': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:CostPriceStandard': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:AverageCost': {'@m:type': 'Edm.Double', '#text': '88'},
   'd:Barcode': {'@m:null': 'true'},
   'd:Created': {'@m:type': 'Edm.DateTime',
    '#text': '2022-03-06T23:12:21.343'},
   'd:CreatorFullName': 'Tomas Formanek',
   'd:Creator': {'@m:type': 'Edm.Guid',
    '#text': 'dd16d9fa-a0ec-48f7-95b7-29b906e5676d'},
   'd:Description': 'Pixel mobile phone',
   'd:Division': {'@m:type': 'Edm.Int32', '#text': '64850'},
   'd:EndDate': {'@m:type': 'Edm.DateTime', '@m:null': 'true'},
   'd:ExtraDescription': 'mobile phones',
   'd:FreeBoolField_01': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:FreeBoolField_02': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:FreeBoolField_03': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:FreeBoolField_04': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:FreeBoolField_05': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:FreeDateField_01': {'@m:type': 'Edm.DateTime', '@m:null': 'true'},
   'd:FreeDateField_02': {'@m:type': 'Edm.DateTime', '@m:null': 'true'},
   'd:FreeDateField_03': {'@m:type': 'Edm.DateTime', '@m:null': 'true'},
   'd:FreeDateField_04': {'@m:type': 'Edm.DateTime', '@m:null': 'true'},
   'd:FreeDateField_05': {'@m:type': 'Edm.DateTime', '@m:null': 'true'},
   'd:FreeNumberField_01': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_02': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_03': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_04': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_05': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_06': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_07': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeNumberField_08': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:FreeTextField_01': {'@m:null': 'true'},
   'd:FreeTextField_02': {'@m:null': 'true'},
   'd:FreeTextField_03': {'@m:null': 'true'},
   'd:FreeTextField_04': {'@m:null': 'true'},
   'd:FreeTextField_05': {'@m:null': 'true'},
   'd:FreeTextField_06': {'@m:null': 'true'},
   'd:FreeTextField_07': {'@m:null': 'true'},
   'd:FreeTextField_08': {'@m:null': 'true'},
   'd:FreeTextField_09': {'@m:null': 'true'},
   'd:FreeTextField_10': {'@m:null': 'true'},
   'd:GLCostsCode': {'@m:null': 'true'},
   'd:GLCostsDescription': {'@m:null': 'true'},
   'd:GLCosts': {'@m:type': 'Edm.Guid', '@m:null': 'true'},
   'd:GLRevenueCode': {'@m:null': 'true'},
   'd:GLRevenueDescription': {'@m:null': 'true'},
   'd:GLRevenue': {'@m:type': 'Edm.Guid', '@m:null': 'true'},
   'd:GLStockCode': {'@m:null': 'true'},
   'd:GLStockDescription': {'@m:null': 'true'},
   'd:GLStock': {'@m:type': 'Edm.Guid', '@m:null': 'true'},
   'd:IsBatchItem': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:IsFractionAllowedItem': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:IsMakeItem': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:IsNewContract': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:IsOnDemandItem': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:IsPackageItem': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:IsPurchaseItem': {'@m:type': 'Edm.Boolean', '#text': 'true'},
   'd:IsSalesItem': {'@m:type': 'Edm.Boolean', '#text': 'true'},
   'd:IsSerialItem': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:IsStockItem': {'@m:type': 'Edm.Boolean', '#text': 'true'},
   'd:IsSubcontractedItem': {'@m:type': 'Edm.Boolean', '#text': 'false'},
   'd:IsTaxableItem': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:IsTime': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:IsWebshopItem': {'@m:type': 'Edm.Byte', '#text': '0'},
   'd:ItemGroupCode': 'DEFAULT',
   'd:ItemGroupDescription': 'Default',
   'd:ItemGroup': {'@m:type': 'Edm.Guid',
    '#text': 'b5080978-d593-4d7e-8c46-e4b3c49a7254'},
   'd:Modified': {'@m:type': 'Edm.DateTime',
    '#text': '2022-03-06T23:12:21.49'},
   'd:ModifierFullName': 'Tomas Formanek',
   'd:Modifier': {'@m:type': 'Edm.Guid',
    '#text': 'dd16d9fa-a0ec-48f7-95b7-29b906e5676d'},
   'd:GrossWeight': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:NetWeight': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:NetWeightUnit': {'@m:null': 'true'},
   'd:Notes': {'@m:null': 'true'},
   'd:PictureName': {'@m:null': 'true'},
   'd:PictureUrl': 'https://start.exactonline.com//docs/images/placeholder_item.png',
   'd:PictureThumbnailUrl': 'https://start.exactonline.com//docs/images/placeholder_item.png',
   'd:SalesVatCodeDescription': {'@m:null': 'true'},
   'd:SalesVatCode': {'@m:null': 'true'},
   'd:SearchCode': 'pix',
   'd:SecurityLevel': {'@m:type': 'Edm.Int32', '#text': '10'},
   'd:StartDate': {'@m:type': 'Edm.DateTime', '#text': '2021-09-01T00:00:00'},
   'd:StatisticalCode': {'@m:null': 'true'},
   'd:StatisticalNetWeight': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:StatisticalUnits': {'@m:type': 'Edm.Double', '#text': '1'},
   'd:StatisticalValue': {'@m:type': 'Edm.Double', '@m:null': 'true'},
   'd:Stock': {'@m:type': 'Edm.Double', '#text': '0'},
   'd:Unit': {'@xml:space': 'preserve', '#text': 'pc'},
   'd:UnitDescription': 'Piece',
   'd:UnitType': 'O'}}}

    )