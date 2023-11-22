import json
from typing import Any, Dict, Iterable, List, Optional

import requests
import xmltodict
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from pendulum import parse

from tap_exact.auth import OAuth2Authenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from time import sleep
from singer_sdk.helpers._state import increment_state
import datetime
import re
from lxml import etree

REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"


class ExactStream(RESTStream):
    dont_use_current_division = False

    @property
    def url_base(self) -> str:
        url = self.config.get("auth_url") or "https://start.exactonline.nl/api/oauth2/token"

        try:
            url = re.findall("(.*)/oauth2", url)[0]
        except:
            raise ValueError("Invalid URL for auth_url. Please update your config.")

        if self.dont_use_current_division:
            return url.replace("/api", "")

        current_division = self.config.get("current_division")
        url = f"{url}/v1/{current_division}"
        return url

    records_jsonpath = "$.feed.entry[*]"
    ignore_parent_stream = False

    @property
    def default_warehouse_id(self):
        use_stock_multiple_warehouses = self.config.get("use_stock_multiple_warehouses")
        if not use_stock_multiple_warehouses and not self.config.get(
            "default_warehouse_id"
        ):
            raise Exception("There is no default_warehouse_code")
        else:
            return self.config.get("default_warehouse_id")

    @property
    def authenticator(self) -> OAuth2Authenticator:
        oauth_url = self.config.get("auth_url", "https://start.exactonline.nl/api/oauth2/token")
        if not oauth_url.endswith("/token"):
            oauth_url += "/token"
        return OAuth2Authenticator(self, self.config, auth_endpoint=oauth_url)

    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    @property
    def default_warehouse_uuid(self) -> str:
        if self.config.get("default_warehouse_id"):
            default_warehouse_id = self.config.get("default_warehouse_id")
            url = f"{self.url_base}/inventory/Warehouses"
            params = {"$filter": f"Code eq '{default_warehouse_id}'"}
            self.logger.info(f"PARAMS - {params}")
            headers = self.authenticator.auth_headers

            response = requests.request("GET", url=url, params=params, headers=headers)
            self.logger.info(f"RESPONSE TEXT : {response.text}")
            self.validate_response(response)
            res_json = self.xml_to_dict(response)

            warehouse_res = res_json["feed"].get("entry")
            if warehouse_res:
                warehouse_uuid = warehouse_res["content"]["m:properties"][
                    "d:ID"
                ]["#text"]
            else:
                raise Exception(f"No warehouses found with Code {default_warehouse_id}")

            self._tap._config["warehouse_uuid"] = warehouse_uuid
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
            return warehouse_uuid

        return None

    @property
    def sync_endpoint(self):
        if self.config.get("sync_endpoints") != None:
            return self.config.get("sync_endpoints")
        return False

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        res_json = self.xml_to_dict(response)
        if "link" in res_json.get("feed", {}).keys():
            link_dict = {}
            links = res_json["feed"]["link"]
            if type(links) == list:
                for record in res_json["feed"]["link"]:
                    link_dict[record["@rel"]] = record["@href"]
                if "next" in link_dict.keys():
                    next_link = link_dict["next"]
                    next_page_token = next_link.split("&")[-1]
                    next_page_token = next_page_token.split("=")[-1]
                    return next_page_token
        else:
            return None

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        if self.select:
            params["$select"] = self.select
        start_date = self.get_starting_time(context)
        start_date = start_date + datetime.timedelta(seconds=1)
        start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        filter = None
        date_filter = None 
        if (
            self.replication_key
            and self.replication_key != "Timestamp"
            and start_date
        ):
            date_filter = f"Modified gt datetime'{start_date}'"
            params["$filter"] = date_filter
        if self.config.get("sync_endpoints") != None:
            if hasattr(self, "filter"):
                filter = self.filter
            if filter and date_filter:
                params["$filter"] = f"{filter} and {date_filter}"
            elif filter or date_filter:
                params["$filter"] = filter or date_filter 
        if hasattr(self, "expand"):
            params["$expand"] = self.expand
        if next_page_token:
            params["$skiptoken"] = next_page_token
        return params

    def xml_to_dict(self, response):
        try:
            #clean invalid xml characters
            my_parser = etree.XMLParser(recover=True)
            xml = etree.fromstring(response.content, parser=my_parser)
            cleaned_xml_string = etree.tostring(xml)
            #parse xml to dict
            data = json.loads(json.dumps(xmltodict.parse(cleaned_xml_string)))
        except:
            data = json.loads(json.dumps(xmltodict.parse(response.content.decode("utf-8-sig").encode("utf-8"))))
        return data

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return extract_jsonpath(self.records_jsonpath, input=self.xml_to_dict(response))

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        content = row["content"]["m:properties"]
        new_content = {}
        for key in content:
            if type(content[key]) == type(""):
                new_content[key[2:]] = content[key]
            elif "Edm.Boolean" == content[key].get("@m:type"):
                if content[key].get("#text") == "true":
                    new_content[key[2:]] = True
                elif content[key].get("#text") == "false":
                    new_content[key[2:]] = False
                else:
                    new_content[key[2:]] = None
            else:
                new_content[key[2:]] = content[key].get("#text", None)
        row = new_content
        return row

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        use_sales_orders = (
            self.config.get("use_sales_orders")
            if self.config.get("use_sales_orders") != None
            else True
        )
        use_sales_invoices = (
            self.config.get("use_sales_invoices")
            if self.config.get("use_sales_invoices") != None
            else False
        )
        use_stock_multiple_warehouses = (
            self.config.get("use_stock_multiple_warehouses") or False
        )
        use_bill_of_materials = (
            self.config.get("use_bill_of_materials",True)
        )

        if (
            (self.name == "sales_order" and not use_sales_orders)
            or (self.name == "sales_invoices" and not use_sales_invoices)
            or (self.name == "warehouses" and use_stock_multiple_warehouses)
            or (
                self.name == "logistics_stock_positions"
                and not use_stock_multiple_warehouses
            )
            or (
                self.name =="bill_of_material_download"
                and not use_bill_of_materials
            )
        ):
            pass
        else:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    continue
                yield transformed_record

    def validate_response(self, response: requests.Response) -> None:
        sleep(1.01)
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
