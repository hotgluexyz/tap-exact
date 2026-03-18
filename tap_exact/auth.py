import json
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.streams import Stream as RESTStreamBase
import backoff

class EmptyResponseError(Exception):
    """Raised when the response is empty"""

class OAuth2Authenticator(OAuthAuthenticator):
    def __init__(
        self,
        stream: RESTStreamBase,
        config_file: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._config_file = config_file
        self._tap = stream._tap

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        return {
            "refresh_token": self._tap._config["refresh_token"],
            "grant_type": "refresh_token",
            "client_id": self._tap._config["client_id"],
            "client_secret": self._tap._config["client_secret"],
        }

    @backoff.on_exception(backoff.expo,EmptyResponseError,max_tries=5,factor=2)
    def update_access_token_locally(self) -> None:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body, headers=headers
        )
        try:
            if (
                token_response.json().get("error_description") 
                == "Rate limit exceeded: access_token not expired"
            ):
                return None
        except Exception as e:
            raise EmptyResponseError(f"Failed converting response to a json, because response is empty")

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        #Log the refresh_token
        self.logger.info(f"Latest refresh token: {token_json['refresh_token']}")

        self.access_token = token_json["access_token"]

        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._tap._config["expires_in"] = int(token_json["expires_in"]) + now

        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)
