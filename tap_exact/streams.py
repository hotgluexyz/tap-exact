from singer_sdk import typing as th

from tap_exact.client import ExactStream

class ItemsStream(ExactStream):
    name = "items"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("AverageCost", th.StringType),
        th.Property("Code", th.StringType),
        th.Property("CopyRemarks",th.StringType,),
        th.Property("CostPriceCurrency", th.StringType),
        th.Property("CostPriceNew", th.StringType),
        th.Property("CostPriceStandard", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EndDate", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("ExtraDescription", th.StringType),
        th.Property("FreeBoolField_01", th.BooleanType),
        th.Property("FreeBoolField_02", th.BooleanType),
        th.Property("FreeBoolField_03", th.BooleanType),
        th.Property("FreeBoolField_04", th.BooleanType),
        th.Property("FreeBoolField_05", th.BooleanType),
        th.Property("FreeDateField_01", th.DateTimeType),
        th.Property("FreeDateField_02", th.DateTimeType),
        th.Property("FreeDateField_03", th.DateTimeType),
        th.Property("FreeDateField_04", th.DateTimeType),
        th.Property("FreeDateField_05", th.DateTimeType),
        th.Property("FreeNumberField_01", th.StringType),
        th.Property("FreeNumberField_02", th.StringType),
        th.Property("FreeNumberField_03", th.StringType),
        th.Property("FreeNumberField_04", th.StringType),
        th.Property("FreeNumberField_05", th.StringType),
        th.Property("FreeNumberField_06", th.StringType),
        th.Property("FreeNumberField_07", th.StringType),
        th.Property("FreeNumberField_08", th.StringType),
        th.Property("FreeTextField_01", th.StringType),
        th.Property("FreeTextField_02", th.StringType),
        th.Property("FreeTextField_03", th.StringType),
        th.Property("FreeTextField_04", th.StringType),
        th.Property("FreeTextField_05", th.StringType),
        th.Property("FreeTextField_06", th.StringType),
        th.Property("FreeTextField_07", th.StringType),
        th.Property("FreeTextField_08", th.StringType),
        th.Property("FreeTextField_09", th.StringType),
        th.Property("FreeTextField_10", th.StringType),
        th.Property("GLCosts", th.StringType),
        th.Property("GLCostsCode", th.StringType),
        th.Property("GLCostsDescription", th.StringType),
        th.Property("GLRevenueCode", th.StringType),
        th.Property("GLRevenue", th.StringType),
        th.Property("GLRevenueDescription", th.StringType),
        th.Property("GLStockCode", th.StringType),
        th.Property("GLStockDescription", th.StringType),
        th.Property("GLStock", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("IsBatchItem", th.StringType),
        th.Property("IsFractionAllowedItem",th.BooleanType,),
        th.Property("IsMakeItem", th.StringType),
        th.Property("IsNewContract",th.StringType,),
        th.Property("IsOnDemandItem",th.StringType,),
        th.Property("IsPackageItem",th.BooleanType,),
        th.Property("IsPurchaseItem",th.BooleanType,),
        th.Property("IsSalesItem", th.BooleanType,),
        th.Property("IsSerialItem", th.BooleanType),
        th.Property("IsStockItem", th.BooleanType),
        th.Property("IsSubcontractedItem", th.BooleanType),
        th.Property("IsTaxableItem", th.BooleanType),
        th.Property("IsTime", th.BooleanType),
        th.Property("Class_01", th.StringType),
        th.Property("Class_02", th.StringType),
        th.Property("Class_03", th.StringType),
        th.Property("Class_04", th.StringType),
        th.Property("Class_05", th.StringType),
        th.Property("Class_06", th.StringType),
        th.Property("Class_07", th.StringType),
        th.Property("Class_08", th.StringType),
        th.Property("Class_09", th.StringType),
        th.Property("Class_10", th.StringType),
        th.Property("Barcode",th.StringType),
        th.Property("IsWebshopItem",th.StringType,),
        th.Property("ItemGroup", th.StringType),
        th.Property("ItemGroupCode", th.StringType),
        th.Property("ItemGroupDescription", th.StringType),
        th.Property("Modified", th.StringType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("SecurityLevel",th.StringType,),
        th.Property("StandardSalesPrice",th.StringType,),
        th.Property("Stock", th.DateTimeType),
        th.Property("Unit", th.DateTimeType),
        th.Property("UnitDescription", th.StringType),
        th.Property("UnitType", th.StringType),
        th.Property("GrossWeight", th.StringType),
        th.Property("NetWeight", th.StringType),
        th.Property("NetWeightUnit", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("PictureName", th.StringType),
        th.Property("PictureUrl", th.StringType),
        th.Property("PictureThumbnailUrl", th.StringType),
        th.Property("SalesVatCode", th.StringType),
        th.Property("SalesVatCodeDescription", th.StringType),
        th.Property("SearchCode", th.StringType),
        th.Property("SecuriytLevel", th.StringType),
        th.Property("StartDate", th.StringType),
        th.Property("StatisticalCode", th.StringType),
        th.Property("StatisticalNetWeight", th.StringType),
        th.Property("StatisticalUnits", th.StringType),
        th.Property("StatisticalValue", th.StringType),
    ).to_dict()

    @property
    def path(self):
        current_division = self.config.get("current_division")
        return f"/api/v1/{current_division}/bulk/Logistics/Items?$select=ID,AverageCost,Barcode,Class_01,Class_02,Class_03,Class_04,Class_05,Class_06,Class_07,Class_08,Class_09,Class_10,Code,CopyRemarks,CostPriceCurrency,CostPriceNew,CostPriceStandard,Created,Creator,CreatorFullName,Description,Division,EndDate,ExtraDescription,FreeBoolField_01,FreeBoolField_02,FreeBoolField_03,FreeBoolField_04,FreeBoolField_05,FreeDateField_01,FreeDateField_02,FreeDateField_03,FreeDateField_04,FreeDateField_05,FreeNumberField_01,FreeNumberField_02,FreeNumberField_03,FreeNumberField_04,FreeNumberField_05,FreeNumberField_06,FreeNumberField_07,FreeNumberField_08,FreeTextField_01,FreeTextField_02,FreeTextField_03,FreeTextField_04,FreeTextField_05,FreeTextField_06,FreeTextField_07,FreeTextField_08,FreeTextField_09,FreeTextField_10,GLCosts,GLCostsCode,GLCostsDescription,GLRevenue,GLRevenueCode,GLRevenueDescription,GLStock,GLStockCode,GLStockDescription,GrossWeight,IsBatchItem,IsFractionAllowedItem,IsMakeItem,IsNewContract,IsOnDemandItem,IsPackageItem,IsPurchaseItem,IsSalesItem,IsSerialItem,IsStockItem,IsSubcontractedItem,IsTaxableItem,IsTime,IsWebshopItem,ItemGroup,ItemGroupCode,ItemGroupDescription,Modified,Modifier,ModifierFullName,NetWeight,NetWeightUnit,Notes,PictureName,PictureThumbnailUrl,PictureUrl,SalesVatCode,SalesVatCodeDescription,SearchCode,SecurityLevel,StandardSalesPrice,StartDate,StatisticalCode,StatisticalNetWeight,StatisticalUnits,StatisticalValue,Stock,Unit,UnitDescription,UnitType"

class SalesOrderStream(ExactStream):
    name = "sales_order"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("AmountDC",th.StringType,),
        th.Property("AmountFC",th.StringType,),
        th.Property("AmountFCExclVat",th.StringType,),
        th.Property("DeliverToContactPerson",th.StringType,),
        th.Property("DeliverToContactPersonFullName",th.StringType,),
        th.Property("DeliverToName",th.StringType,),
        th.Property("OrderedByName",th.StringType,),
        th.Property("TaxSchedule",th.StringType,),
        th.Property("WarohouseID",th.StringType,),
        th.Property("YourRef",th.StringType,),
        th.Property("AmountDiscount",th.StringType,),
        th.Property("AmountDiscountExclVat",th.StringType,),
        th.Property("ApprovalStatus",th.StringType,),
        th.Property("ApprovalStatusDescription", th.StringType),
        th.Property("Approved",th.StringType,),
        th.Property("Approver",th.StringType,),
        th.Property("ApproverFullName",th.StringType,),
        th.Property("Created",th.DateTimeType,),
        th.Property("Creator",th.StringType,),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("DeliverTo",th.StringType,),
        th.Property("DeliveryDate",th.DateTimeType,),
        th.Property("DeliveryStatus",th.StringType,),
        th.Property("DeliveryStatusDescription", th.StringType),
        th.Property("DeliveryAddress",th.StringType,),
        th.Property("Description", th.StringType),
        th.Property("OrderDate",th.DateTimeType,),
        th.Property("OrderedBy",th.StringType,),
        th.Property("OrderedNyName",th.StringType,),
        th.Property("OrderID",th.StringType,),
        th.Property("OrderNumber",th.StringType,),
        th.Property("Salesperson",th.StringType,),
        th.Property("Status",th.StringType,),
        th.Property("StatusDescription", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("WarehouseDescription", th.StringType),
        th.Property("WarehouseID", th.StringType),
    ).to_dict()

    @property
    def path(self):
        current_division = self.config.get("current_division")
        return f"/api/v1/{current_division}/bulk/SalesOrder/SalesOrders?$select=OrderID,AmountDC,AmountDiscount,AmountDiscountExclVat,AmountFC,AmountFCExclVat,ApprovalStatus,ApprovalStatusDescription,Approved,Approver,ApproverFullName,Created,Creator,CreatorFullName,Currency,DeliverTo,DeliverToContactPerson,DeliverToContactPersonFullName,DeliverToName,DeliveryAddress,DeliveryDate,DeliveryStatus,DeliveryStatusDescription,Description,OrderDate,OrderedBy,OrderedByName,OrderNumber,Salesperson,Status,StatusDescription,TaxSchedule,WarehouseCode,WarehouseDescription,WarehouseID,YourRef"

class PurchaseOrdersStream(ExactStream):
    name = "purchase_orders"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property('AmountDC',th.StringType),
        th.Property('AmountFC',th.StringType),
        th.Property('Created',th.DateTimeType),
        th.Property('Creator',th.StringType),
        th.Property('CreatorFullName',th.StringType),
        th.Property('Currency',th.StringType),
        th.Property('DeliveryAccount',th.StringType),
        th.Property('DeliveryAccountCode',th.StringType),
        th.Property('DeliveryAccountName',th.StringType),
        th.Property('DeliveryAddress',th.StringType),
        th.Property('DeliveryContact',th.StringType),
        th.Property('DeliveryContactPersonFullName',th.StringType),
        th.Property('Description',th.StringType),
        th.Property('Division',th.StringType),
        th.Property('Document',th.StringType),
        th.Property('DocumentSubject',th.StringType),
        th.Property('DropShipment',th.BooleanType),
        th.Property('ExchangeRate',th.StringType),
        th.Property('IncotermAddress',th.StringType),
        th.Property('IncotermCode',th.StringType),
        th.Property('IncotermVersion',th.StringType),
        th.Property('InvoiceStatus',th.StringType),
        th.Property('Modified',th.StringType),
        th.Property('Modifier',th.StringType),
        th.Property('ModifierFullName',th.StringType),
        th.Property('OrderDate',th.StringType),
        th.Property('OrderNumber',th.StringType),
        th.Property('OrderStatus',th.StringType),
        th.Property('PaymentCondition',th.StringType),
        th.Property('PaymentConditionDescription',th.StringType),
        th.Property('PurchaseOrderID',th.StringType),
        th.Property('PurchaseAgent',th.StringType),
        th.Property('PurchaseAgentFullName',th.StringType),
        th.Property('PurchaseOrderLineCount',th.StringType),
        th.Property('ReceiptDate',th.StringType),
        th.Property('ReceiptStatus',th.StringType),
        th.Property('Remarks',th.StringType),
        th.Property('SalesOrder',th.StringType),
        th.Property('SalesOrderNumber',th.StringType),
        th.Property('SelectionCode',th.StringType),
        th.Property('SelectionCodeCode',th.StringType),
        th.Property('SelectionCodeDescription',th.StringType),
        th.Property('ShippingMethod',th.StringType),
        th.Property('ShippingMethodCode',th.StringType),
        th.Property('ShippingMethodDescription',th.StringType),
        th.Property('Source',th.StringType),
        th.Property('Supplier',th.StringType),
        th.Property('SupplierCode',th.StringType),
        th.Property('SupplierContact',th.StringType),
        th.Property('SupplierContactPersonFullName',th.StringType),
        th.Property('SupplierName',th.StringType),
        th.Property('VATAmount',th.StringType),
        th.Property('Warehouse',th.StringType),
        th.Property('WarehouseCode',th.StringType),
        th.Property('WarehouseDescription',th.StringType),
        th.Property('YourRef',th.StringType),
    ).to_dict()

    @property
    def path(self):
        current_division = self.config.get("current_division")
        return f"/api/v1/{current_division}/purchaseorder/PurchaseOrders?$select=PurchaseOrderID,AmountDC,AmountFC,Created,Creator,CreatorFullName,Currency,DeliveryAccount,DeliveryAccountCode,DeliveryAccountName,DeliveryAddress,DeliveryContact,DeliveryContactPersonFullName,Description,Division,Document,DocumentSubject,DropShipment,ExchangeRate,IncotermAddress,IncotermCode,IncotermVersion,InvoiceStatus,Modified,Modifier,ModifierFullName,OrderDate,OrderNumber,OrderStatus,PaymentCondition,PaymentConditionDescription,PurchaseAgent,PurchaseAgentFullName,PurchaseOrderLineCount,PurchaseOrderLines,ReceiptDate,ReceiptStatus,Remarks,SalesOrder,SalesOrderNumber,SelectionCode,SelectionCodeCode,SelectionCodeDescription,ShippingMethod,ShippingMethodCode,ShippingMethodDescription,Source,Supplier,SupplierCode,SupplierContact,SupplierContactPersonFullName,SupplierName,VATAmount,Warehouse,WarehouseCode,WarehouseDescription,YourRef&$expand=PurchaseOrderLines"


class WarehouseStream(ExactStream):
    name = "warehouses"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("Created",th.StringType,),
        th.Property("CreatorFullName",th.CustomType({"type": ["array", "object", "string"]}),),
        th.Property("CurrentStock",th.StringType,),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("ItemStartDate",th.StringType,),
        th.Property("ItemUnit", th.StringType),
        th.Property("ItemUnitDescription", th.StringType),
        th.Property("MaximumStock",th.StringType,),
        th.Property("Modified",th.StringType,),
        th.Property("Modifier",th.StringType,),
        th.Property("ModifierFullName",th.CustomType({"type": ["array", "object", "string"]}),),
        th.Property("ProjectedStock",th.StringType,),
        th.Property("ReorderPoint",th.StringType,),
        th.Property("SafetyStock",th.StringType,),
        th.Property("StorageLocationUrl", th.StringType),
        th.Property("Warehouse",th.StringType,),
        th.Property("WarehouseCode", th.StringType),
        th.Property("WarehouseDescription", th.StringType),
        th.Property("ItemIsSotckItem", th.StringType),
        th.Property("ItemEndDate", th.DateTimeType),
        th.Property("PlanningDetailsUrl", th.StringType),
        th.Property("PlannedStockIn", th.StringType),
        th.Property("PlannedStockOut", th.StringType),
        th.Property("Creator", th.StringType),
        th.Property("DefaultStorageLocation", th.StringType),
        th.Property("DefaultStorageLocationCode", th.StringType),
        th.Property("DefaultStorageLocationDescription", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("OrderPolicy", th.StringType),
        th.Property("Period", th.StringType),
        th.Property("ReorderQuantity", th.StringType),
        th.Property("ReplenishmentType", th.StringType),
        th.Property("ReservedStock", th.StringType),
        th.Property("ItemIsFractionAllowedItem", th.BooleanType),
        th.Property("ItemIsStockItem", th.BooleanType),
    ).to_dict()

    @property
    def path(self):
        current_division = self.config.get("current_division")
        return f"/api/v1/{current_division}/inventory/ItemWarehouses?$select=ID,Created,Creator,CreatorFullName,CurrentStock,DefaultStorageLocation,DefaultStorageLocationCode,DefaultStorageLocationDescription,Division,Item,ItemCode,ItemDescription,ItemEndDate,ItemIsFractionAllowedItem,ItemIsStockItem,ItemStartDate,ItemUnit,ItemUnitDescription,MaximumStock,Modified,Modifier,ModifierFullName,OrderPolicy,Period,PlannedStockIn,PlannedStockOut,PlanningDetailsUrl,ProjectedStock,ReorderPoint,ReorderQuantity,ReplenishmentType,ReservedStock,SafetyStock,StorageLocationUrl,Warehouse,WarehouseCode,WarehouseDescription"


class SuppliersStream(ExactStream):
    name = "suppliers"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("Creator",th.StringType,),
        th.Property("CreatorFullName",th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("CurrencyDescription", th.StringType),
        th.Property("Division",th.StringType,),
        th.Property("DropShipment",th.StringType,),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("MinimumQuantity",th.StringType,),
        th.Property("Modified",th.StringType,),
        th.Property("Modifier",th.StringType,),
        th.Property("ModifierFullName",th.StringType),
        th.Property("PurchaseLeadTime",th.StringType,),
        th.Property("PurchasePrice",th.StringType,),
        th.Property("PurchaseUnit", th.StringType),
        th.Property("PurchaseUnitDescription", th.StringType),
        th.Property("StartDate",th.StringType,),
        th.Property("Supplier",th.StringType,),
        th.Property("SupplierCode",th.StringType,),
        th.Property("SupplierDescription", th.StringType),
        th.Property("SupplierItemCode", th.StringType),
        th.Property("CopyRemarks", th.StringType),
        th.Property("CountryOfOrigin", th.StringType),
        th.Property("CountryOfOriginDescription", th.StringType),
        th.Property("Created", th.StringType),
        th.Property("EndDate", th.BooleanType),
        th.Property("MainSupplier", th.BooleanType),
        th.Property("Notes", th.StringType),
        th.Property("PurchaseUnitFactor", th.StringType),
        th.Property("PurchaseVATCode", th.StringType),
        th.Property("PurchaseVATCodeDescription", th.StringType),
    ).to_dict()

    @property
    def path(self):
        current_division = self.config.get("current_division")
        return f"/api/v1/{current_division}/logistics/SupplierItem?$select=ID,CopyRemarks,CountryOfOrigin,CountryOfOriginDescription,Created,Creator,CreatorFullName,Currency,CurrencyDescription,Division,DropShipment,EndDate,Item,ItemCode,ItemDescription,MainSupplier,MinimumQuantity,Modified,Modifier,ModifierFullName,Notes,PurchaseLeadTime,PurchasePrice,PurchaseUnit,PurchaseUnitDescription,PurchaseUnitFactor,PurchaseVATCode,PurchaseVATCodeDescription,StartDate,Supplier,SupplierCode,SupplierDescription,SupplierItemCode"


class SalesOrderLinesStream(ExactStream):
    name = "sales_orderlines"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("AmountDC",th.StringType,),
        th.Property("AmountFC",th.StringType,),
        th.Property("DeliveryDate",th.StringType,),
        th.Property("Description", th.StringType),
        th.Property("DeliveryDate",th.DateTimeType),
        th.Property("Discount", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("LineNumber",th.StringType,),
        th.Property("NetPrice",th.StringType,),
        th.Property("OrderID",th.StringType,),
        th.Property("OrderNumber",th.StringType,),
        th.Property("Quantity",th.StringType,),
        th.Property("UnitCode", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("UnitPrice",th.StringType,),
        th.Property("UseDropShipment",th.StringType,),
        th.Property("VATAmount",th.StringType,),
        th.Property("CostCenter",th.StringType,),
        th.Property("CostCenterDescription",th.StringType,),
        th.Property("CostPriceFC",th.StringType,),
        th.Property("CostUnit",th.StringType,),
        th.Property("CostUnitDescription",th.StringType,),
        th.Property("CustomerItemCode",th.StringType,),
        th.Property("Division",th.StringType,),
        th.Property("ItemVersion",th.StringType,),
        th.Property("ItemVersionDescription",th.StringType,),
        th.Property("Notes",th.StringType,),
        th.Property("Pricelist",th.StringType,),
        th.Property("PricelistDescription",th.StringType,),
        th.Property("ProjectDescription",th.StringType,),
        th.Property("Project",th.StringType,),
        th.Property("PurchaseOrder",th.StringType,),
        th.Property("PurchaseOrderLine",th.StringType,),
        th.Property("PurchaseOrderLineNumber",th.StringType,),
        th.Property("PurchaseOrderNumber",th.StringType,),
        th.Property("ShopOrder",th.StringType,),
        th.Property("VATCode",th.StringType,),
        th.Property("VATCodeDescription",th.StringType,),
        th.Property("VATPercentage",th.StringType,),
    ).to_dict()

    @property
    def path(self):
        current_division = self.config.get("current_division")
        return f"/api/v1/{current_division}/bulk/SalesOrder/SalesOrderLines?$select=ID,AmountDC,AmountFC,CostCenter,CostCenterDescription,CostPriceFC,CostUnit,CostUnitDescription,CustomerItemCode,DeliveryDate,Description,Discount,Division,Item,ItemCode,ItemDescription,ItemVersion,ItemVersionDescription,LineNumber,NetPrice,Notes,OrderID,OrderNumber,Pricelist,PricelistDescription,Project,ProjectDescription,PurchaseOrder,PurchaseOrderLine,PurchaseOrderLineNumber,PurchaseOrderNumber,Quantity,ShopOrder,UnitCode,UnitDescription,UnitPrice,UseDropShipment,VATAmount,VATCode,VATCodeDescription,VATPercentage"