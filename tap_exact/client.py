import json
from typing import Any, Dict, Iterable, List, Optional

import requests
import copy
import xmltodict
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_exact.auth import OAuth2Authenticator


class ExactStream(RESTStream):

    @property
    def url_base(self) -> str:
        refresh_token = self.config["refresh_token"].split(".")[0]
        if "NL" in refresh_token:
            exact_environment = "nl"
        elif "UK" in refresh_token:
            exact_environment = "co.uk"
        else:
            exact_environment = "com"

        url = f"https://start.exactonline.{exact_environment}"
        return url

    records_jsonpath = "$.feed.entry[*]"
    @property
    def default_warehouse_id(self):
        return self.config.get("default_warehouse_id")

    @property
    def authenticator(self) -> OAuth2Authenticator:
        oauth_url = f"{self.url_base}/api/oauth2/token"

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
            current_division = self.config.get("current_division")
            url=f"{self.url_base}/api/v1/{current_division}/inventory/Warehouses"
            params={"$filter": f"Code eq '{default_warehouse_id}'"}
            headers=self.authenticator.auth_headers
            json_path = "$.feed.entry.content[0].properties.code"

            response = requests.request("GET", url=url, params=params,headers=headers)
            res_json = self.xml_to_dict(response)
            return res_json["feed"]["entry"]["content"]["m:properties"]["d:ID"]["#text"]
        return None

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # not enought data on the test account to test pagination
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("__next", None)
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token

        # no datetime filter found on api's reference
        # and nothing found testing different possibilities
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def xml_to_dict(self, response):
        data = json.loads(json.dumps(xmltodict.parse(response.text)))
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
