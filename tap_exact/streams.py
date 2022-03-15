"""Stream type classes for tap-exact."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th
from tomlkit import string  # JSON Schema typing helpers

from tap_exact.client import ExactStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


# class UsersStream(ExactStream):
#     """Define custom stream."""
#     name = "users"
#     path = "/users"
#     primary_keys = ["id"]
#     replication_key = None
#     # Optionally, you may also use `schema_filepath` in place of `schema`:
#     # schema_filepath = SCHEMAS_DIR / "users.json"
#     schema = th.PropertiesList(
#         th.Property("name", th.StringType),
#         th.Property(
#             "id",
#             th.StringType,
#             description="The user's system ID"
#         ),
#         th.Property(
#             "age",
#             th.IntegerType,
#             description="The user's age in years"
#         ),
#         th.Property(
#             "email",
#             th.StringType,
#             description="The user's email address"
#         ),
#         th.Property("street", th.StringType),
#         th.Property("city", th.StringType),
#         th.Property(
#             "state",
#             th.StringType,
#             description="State name in ISO 3166-2 format"
#         ),
#         th.Property("zip", th.StringType),
#     ).to_dict()




class ItemsStream(ExactStream):
    name = "items"
    path = "/api/v1/64850/bulk/Logistics/Items?$select=ID,AverageCost,Barcode,Class_01,Class_02,Class_03,Class_04,Class_05,Class_06,Class_07,Class_08,Class_09,Class_10,Code,CopyRemarks,CostPriceCurrency,CostPriceNew,CostPriceStandard,Created,Creator,CreatorFullName,Description,Division,EndDate,ExtraDescription,FreeBoolField_01,FreeBoolField_02,FreeBoolField_03,FreeBoolField_04,FreeBoolField_05,FreeDateField_01,FreeDateField_02,FreeDateField_03,FreeDateField_04,FreeDateField_05,FreeNumberField_01,FreeNumberField_02,FreeNumberField_03,FreeNumberField_04,FreeNumberField_05,FreeNumberField_06,FreeNumberField_07,FreeNumberField_08,FreeTextField_01,FreeTextField_02,FreeTextField_03,FreeTextField_04,FreeTextField_05,FreeTextField_06,FreeTextField_07,FreeTextField_08,FreeTextField_09,FreeTextField_10,GLCosts,GLCostsCode,GLCostsDescription,GLRevenue,GLRevenueCode,GLRevenueDescription,GLStock,GLStockCode,GLStockDescription,GrossWeight,IsBatchItem,IsFractionAllowedItem,IsMakeItem,IsNewContract,IsOnDemandItem,IsPackageItem,IsPurchaseItem,IsSalesItem,IsSerialItem,IsStockItem,IsSubcontractedItem,IsTaxableItem,IsTime,IsWebshopItem,ItemGroup,ItemGroupCode,ItemGroupDescription,Modified,Modifier,ModifierFullName,NetWeight,NetWeightUnit,Notes,PictureName,PictureThumbnailUrl,PictureUrl,SalesVatCode,SalesVatCodeDescription,SearchCode,SecurityLevel,StandardSalesPrice,StartDate,StatisticalCode,StatisticalNetWeight,StatisticalUnits,StatisticalValue,Stock,Unit,UnitDescription,UnitType"
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
    ).to_dict()


class SalesOrderStream(ExactStream):
    name = "sales_order"
    path = "/api/v1/64850/bulk/SalesOrder/SalesOrders?$select=OrderID,AmountDC,AmountDiscount,AmountDiscountExclVat,AmountFC,AmountFCExclVat,ApprovalStatus,ApprovalStatusDescription,Approved,Approver,ApproverFullName,Created,Creator,CreatorFullName,Currency,DeliverTo,DeliverToContactPerson,DeliverToContactPersonFullName,DeliverToName,DeliveryAddress,DeliveryDate,DeliveryStatus,DeliveryStatusDescription,Description,OrderDate,OrderedBy,OrderedByName,OrderNumber,Salesperson,Status,StatusDescription,TaxSchedule,WarehouseCode,WarehouseDescription,WarehouseID,YourRef"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        
        th.Property("id", th.StringType),
        th.Property("updated", th.DateTimeType),
        th.Property("author", th.ObjectType(
            th.Property("name", th.StringType),
        )),
        th.Property("link", th.ObjectType(
            th.Property("@rel", th.StringType),
            th.Property("@title", th.StringType),
            th.Property("@href", th.StringType),
        )),
        th.Property("@title", th.StringType),
        th.Property("@href", th.StringType),
        th.Property("category", th.ObjectType(
            th.Property("@scheme", th.StringType),
            th.Property("@term", th.StringType),
        )),
        th.Property("content", th.ObjectType(
            th.Property("@type", th.StringType),
            th.Property("m:properties", th.ObjectType(
                th.Property("d:AmountDc", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:AmountDiscount", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:AmountDiscountExclVat", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ApprovalStatus", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ApprovalStatusDescription", th.StringType),
                th.Property("d:Approved", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Approver", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Created", th.ObjectType(
                    th.Property("#text", th.DateTimeType)
                )),
                th.Property("d:Creator", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:CreatorFullName", th.StringType),
                th.Property("d:Currency", th.StringType),
                th.Property("d:DeliverTo", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:DeliveryDate", th.ObjectType(
                    th.Property("#text", th.DateTimeType)
                )),
                th.Property("d:DeliveryStatus", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:DeliveryStatusDescription", th.StringType),
                th.Property("d:DeliveryAddress", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Description", th.StringType),
                th.Property("d:OrderDate", th.ObjectType(
                    th.Property("#text", th.DateTimeType)
                )),
                th.Property("d:OrderedBy", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:OrderedNyName", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:OrderID", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:OrderNumber", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Salesperson", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Status", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:StatusDescription", th.StringType),
                th.Property("d:WarehouseCode", th.StringType),
                th.Property("d:WarehouseDescription", th.StringType),
            )),
        )),
        

    ).to_dict()


class PurchaseOrdersStream(ExactStream):
    name = "purchase_orders"
    path = "//api/v1/64850/purchaseorder/PurchaseOrders?$select=PurchaseOrderID,AmountDC,AmountFC,Created,Creator,CreatorFullName,Currency,DeliveryAccount,DeliveryAccountCode,DeliveryAccountName,DeliveryAddress,DeliveryContact,DeliveryContactPersonFullName,Description,Division,Document,DocumentSubject,DropShipment,ExchangeRate,IncotermAddress,IncotermCode,IncotermVersion,InvoiceStatus,Modified,Modifier,ModifierFullName,OrderDate,OrderNumber,OrderStatus,PaymentCondition,PaymentConditionDescription,PurchaseAgent,PurchaseAgentFullName,PurchaseOrderLineCount,PurchaseOrderLines,ReceiptDate,ReceiptStatus,Remarks,SalesOrder,SalesOrderNumber,SelectionCode,SelectionCodeCode,SelectionCodeDescription,ShippingMethod,ShippingMethodCode,ShippingMethodDescription,Source,Supplier,SupplierCode,SupplierContact,SupplierContactPersonFullName,SupplierName,VATAmount,Warehouse,WarehouseCode,WarehouseDescription,YourRef&$expand=PurchaseOrderLines"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated", th.DateTimeType),
        th.Property("author", th.ObjectType(
            th.Property("name", th.StringType),
        )),
        th.Property("link", th.ArrayType(
            th.ObjectType(
                th.Property("@rel", th.StringType),
                th.Property("@title", th.StringType),
                th.Property("@href", th.StringType),
                th.Property("@type", th.StringType),
                th.Property("m:inline", th.ObjectType(
                    th.Property("feed", th.ObjectType(
                        th.Property("title", th.ObjectType(
                            th.Property("#text", th.StringType),
                        )),
                        th.Property("id", th.StringType),
                        th.Property("updated", th.DateTimeType),
                        th.Property("link", th.ObjectType(
                            th.Property("@rel", th.StringType),
                            th.Property("@title", th.StringType),
                            th.Property("@href", th.StringType),
                        )),
                        th.Property("entry", th.ObjectType(
                            th.Property("id", th.StringType),
                            th.Property("updated", th.DateTimeType),
                            th.Property("author", th.ObjectType(
                                th.Property("name", th.StringType)
                                
                            )),
                            th.Property("link", th.ObjectType(
                                th.Property("@rel", th.StringType),
                                th.Property("@title", th.StringType),
                                th.Property("@href", th.StringType),
                            )),
                            th.Property("content", th.ObjectType(
                                th.Property("m:properties", th.ObjectType(
                                    th.Property("d:AmountDc", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:AmountFc", th.ObjectType(
                                        th.Property("#text", th.StringType),
                                    )),
                                    th.Property("d:Created", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:Creator", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:CreatorFullName", th.StringType),
                                    th.Property("d:Description", th.StringType),
                                    th.Property("d:Discount", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Division", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:ID", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:InvoicedQuantity", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:InStock", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:IsBatchNumberItem", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:IsSerialNumberItem", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Item", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:ItemCode", th.StringType),
                                    th.Property("d:ItemDescription", th.StringType),
                                    th.Property("d:LineNumber", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Modified", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:Modifier", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:ModifierFullName", th.StringType),
                                    th.Property("d:NetPrice", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:PurchaseOrderID", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:ProjectedStock", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Quantity", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:QuantityInPurchaseUnits", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:ReceiptDate", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:ReceivedQuantity", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:SupplierItemCopyRemarks", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Unit", th.StringType),
                                    th.Property("d:UnitDescription", th.StringType),
                                    th.Property("d:UnitPrice", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:VATAmount", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                ))
                            )),
                            th.Property("category", th.ObjectType(
                                th.Property("@term", th.StringType),
                                th.Property("@scheme", th.StringType)
                            )),
                            th.Property("content", th.ObjectType(
                                th.Property("m:properties", th.ObjectType(
                                    th.Property("d:AmountDc", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:AmountFc", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Created", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:Creator", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:CreatorFullName", th.StringType),
                                    th.Property("d:Currency", th.StringType),
                                    th.Property("d:DeliveryAddress", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:DeliveryContact", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Description", th.StringType),
                                    th.Property("d:Division", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:InvoiceStatus", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Modified", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:Modifier", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("ModifierFullName", th.StringType),
                                    th.Property("d:OrderDate", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:OrderNumber", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:OrderStatus", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:PurchaseOrderID", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:PurchaseAgentFullName", th.StringType),
                                    th.Property("d:PurchaseAgentFullName", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:PurchaseOrderLineCount", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:ReceiptDate", th.ObjectType(
                                        th.Property("#text", th.DateTimeType)
                                    )),
                                    th.Property("d:Source", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Supplier", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:SupplierCode", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:Warehouse", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:WarehouseCode", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                    th.Property("d:WarehouseDescription", th.ObjectType(
                                        th.Property("#text", th.StringType)
                                    )),
                                ))
                            ))                      
                        ))                
                    ))
                ))
            )
        ))
    ).to_dict()


class WarehouseStream(ExactStream):
    name = "warehouses"
    path = "/api/v1/64850/inventory/ItemWarehouses?$select=ID,Created,Creator,CreatorFullName,CurrentStock,DefaultStorageLocation,DefaultStorageLocationCode,DefaultStorageLocationDescription,Division,Item,ItemCode,ItemDescription,ItemEndDate,ItemIsFractionAllowedItem,ItemIsStockItem,ItemStartDate,ItemUnit,ItemUnitDescription,MaximumStock,Modified,Modifier,ModifierFullName,OrderPolicy,Period,PlannedStockIn,PlannedStockOut,PlanningDetailsUrl,ProjectedStock,ReorderPoint,ReorderQuantity,ReplenishmentType,ReservedStock,SafetyStock,StorageLocationUrl,Warehouse,WarehouseCode,WarehouseDescription"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("content", th.ObjectType(
            th.Property("@type", th.StringType),
            th.Property("m:properties", th.ObjectType(
                th.Property("d:Created", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:CreatorFullName", th.CustomType({"type": ["array", "object", "string"]})),
                th.Property("d:CurrentStock", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ID", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Item", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ItemCode", th.StringType),
                th.Property("d:ItemDescription", th.StringType),
                th.Property("d:ItemStartDate", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ItemUnit", th.StringType),
                th.Property("d:ItemUnitDescription", th.StringType),
                th.Property("d:MaximumStock", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Modified", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Modifier", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ModifierFullName", th.CustomType({"type": ["array", "object", "string"]})),
                th.Property("d:ProjectedStock", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ReorderPoint", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:SafetyStock", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:StorageLocationUrl", th.StringType),
                th.Property("d:Warehouse", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:WarehouseCode", th.StringType),
                th.Property("d:WarehouseDescription", th.StringType),

            ))
        )
        )
      
    ).to_dict()


class SuppliersStream(ExactStream):
    name = "suppliers"
    path = "/api/v1/64850/logistics/SupplierItem?$select=ID,CopyRemarks,CountryOfOrigin,CountryOfOriginDescription,Created,Creator,CreatorFullName,Currency,CurrencyDescription,Division,DropShipment,EndDate,Item,ItemCode,ItemDescription,MainSupplier,MinimumQuantity,Modified,Modifier,ModifierFullName,Notes,PurchaseLeadTime,PurchasePrice,PurchaseUnit,PurchaseUnitDescription,PurchaseUnitFactor,PurchaseVATCode,PurchaseVATCodeDescription,StartDate,Supplier,SupplierCode,SupplierDescription,SupplierItemCode"
    primary_keys = ["id"]

    schema = th.PropertiesList(

        th.Property("content", th.ObjectType(
            th.Property("m:properties", th.ObjectType(
                th.Property("d:Creator", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:CreatorFullName", th.CustomType({"type": ["array", "object", "string"]})),
                th.Property("d:Currency", th.StringType),
                th.Property("d:CurrencyDescription", th.StringType),
                th.Property("d:Division", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:DropShipment", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ID", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Item", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ItemCode", th.StringType),
                th.Property("d:ItemDescription", th.StringType),
                th.Property("d:MinimumQuantity", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Modified", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Modifier", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ModifierFullName", th.CustomType({"type": ["array", "object", "string"]})),
                th.Property("d:PurchaseLeadTime", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:PurchasePrice", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:PurchaseUnit", th.StringType),
                th.Property("d:PurchaseUnitDescription", th.StringType),
                th.Property("d:StartDate", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Supplier", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:SupplierCode", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:SupplierDescription", th.StringType),
                th.Property("d:SupplierItemCode", th.StringType)

            ))
        ))
    ).to_dict()


class SalesOrderLinesStream(ExactStream):
    name = "sales_orderlines"
    path = "/api/v1/64850/bulk/SalesOrder/SalesOrderLines?$select=ID,AmountDC,AmountFC,CostCenter,CostCenterDescription,CostPriceFC,CostUnit,CostUnitDescription,CustomerItemCode,DeliveryDate,Description,Discount,Division,Item,ItemCode,ItemDescription,ItemVersion,ItemVersionDescription,LineNumber,NetPrice,Notes,OrderID,OrderNumber,Pricelist,PricelistDescription,Project,ProjectDescription,PurchaseOrder,PurchaseOrderLine,PurchaseOrderLineNumber,PurchaseOrderNumber,Quantity,ShopOrder,UnitCode,UnitDescription,UnitPrice,UseDropShipment,VATAmount,VATCode,VATCodeDescription,VATPercentage"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("content", th.ObjectType(
            th.Property("m:properties", th.ObjectType(
                th.Property("d:AmountDC", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:AmountFC", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:DeliveryDate", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Description", th.StringType),
                th.Property("d:DeliveryDate", th.ObjectType(
                    th.Property("d:Discount", th.StringType)
                )),
                th.Property("d:ID", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Item", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:ItemCode", th.StringType),
                th.Property("d:ItemDescription", th.StringType),
                th.Property("d:LineNumber", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:NetPrice", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:OrderID", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:OrderNumber", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:Quantity", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:UnitCode", th.StringType),
                th.Property("d:UnitDescription", th.StringType),
                th.Property("d:UnitPrice", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:UseDropShipment", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),
                th.Property("d:VATAmount", th.ObjectType(
                    th.Property("#text", th.StringType)
                )),

            ))
        ))
    ).to_dict()
