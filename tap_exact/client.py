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

    def url_base(self) -> str:
        exact_environment = self.config["exact_environment"]
        url = f"https://start.exactonline.{exact_environment}"
        return url

    records_jsonpath = "$.feed.entry[*]"

    @property
    def authenticator(self) -> OAuth2Authenticator:
        exact_environment = self.config.get("exact_environment")
        oauth_url = f"https://start.exactonline.{exact_environment}/api/oauth2/token"

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
                new_content[key[2:]] = bool(content[key].get("#text", None))
            else:
                new_content[key[2:]] = content[key].get("#text", None)
        row = new_content
        return row

    def get_url(self, context: Optional[dict]) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        url = "".join([self.url_base(), self.path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for k, v in vals.items():
            search_text = "".join(["{", k, "}"])
            if search_text in url:
                url = url.replace(search_text, self._url_encode(v))
        return url