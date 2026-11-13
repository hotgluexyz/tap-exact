from hotglue_singer_sdk.exceptions import FatalAPIError


class TapExactDailyRateLimitExceededError(FatalAPIError):
    """Raised when the Exact Online daily API call limit for the division is exhausted."""


class TapExactInsufficientPermissionsError(FatalAPIError):
    """Raised when Exact answers 403: the token is valid but the connected user lacks
    rights for the division or endpoint (missing role or subscription module)."""
