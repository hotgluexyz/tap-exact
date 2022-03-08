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
        th.Property("id", th.StringType),
        th.Property("title", th.ObjectType(
            th.Property("@type", th.StringType)
        )),
        th.Property("updated", th.DateTimeType),
        th.Property("author", th.ObjectType(
            th.Property("name", th.StringType),
        )),
        th.Property("link", th.ObjectType(
            th.Property("@rel", th.StringType),
            th.Property("@title", th.StringType),
            th.Property("@href", th.StringType),
        )),
        th.Property("category", th.ObjectType(
            th.Property("@scheme", th.StringType),
            th.Property("@term", th.StringType),
        )),
        th.Property("content", th.ObjectType(
            th.Property("d:AverageCost", th.ObjectType(
                th.Property("#text", th.StringType)
                
            )),
            th.Property("d:Code", th.StringType),
            th.Property("d.CopyRemarks", th.ObjectType(
                th.Property("#text", th.StringType),
            )),
            th.Property("d:CostPriceCurrency", th.StringType),
            th.Property("d:Created", th.ObjectType(
                th.Property("#text", th.DateTimeType)
            )),
            th.Property("d:Creator", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:CreatorFullName", th.StringType),
            th.Property("d:Description", th.StringType),
            th.Property("d:ExtraDescription", th.StringType),
            th.Property("d:ID", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsBatchItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsFractionAllowedItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsMakeItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsNewContract", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsOnDemandItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsPackageItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsPurchaseItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsSalesItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsSerialItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsStockItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsSubcontractedItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsTaxableItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsTime", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:IsWebshopItem", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:ItemGroup", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:ItemGroupCode", th.StringType),
            th.Property("d:ItemGroupDescription", th.StringType),
            th.Property("d:Modified", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:Modifier", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:ModifierFullName", th.StringType),
            th.Property("d:PictureThumbnailUrl", th.StringType),
            th.Property("d:PictureUrl", th.StringType),
            th.Property("d:SearchCode", th.StringType),
            th.Property("d:SecurityLevel", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:StandardSalesPrice", th.ObjectType(
                th.Property("#text", th.StringType)
            )),
            th.Property("d:StartDate", th.ObjectType(
                th.Property("#text", th.DateTimeType)
            )),
            th.Property("d:StatisticalUnits", th.ObjectType(
                th.Property("#text", th.DateTimeType)
            )),
            th.Property("d:Stock", th.ObjectType(
                th.Property("#text", th.DateTimeType)
            )),
            th.Property("d:Unit", th.ObjectType(
                th.Property("#text", th.DateTimeType)
            )),
            th.Property("d:UnitDescription", th.StringType),
            th.Property("d:UnitType", th.StringType),


            
        )),
    )

