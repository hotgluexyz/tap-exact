import json
from typing import Any, Dict, Iterable, List, Optional

import requests
import xmltodict
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_exact.auth import OAuth2Authenticator


class ExactStream(RESTStream):

    url_base = "https://start.exactonline.com"

    records_jsonpath = "$.feed.entry[*]"

    @property
    def authenticator(self) -> OAuth2Authenticator:
        oauth_url = "https://start.exactonline.com/api/oauth2/token"
        return OAuth2Authenticator(self, self.config, auth_endpoint=oauth_url)

    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def xml_to_dict(self, response):
        data = json.loads(json.dumps(xmltodict.parse(response.text)))
        return data

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(
            self.records_jsonpath, input=self.xml_to_dict(response)
        )
