"""Minimal in-memory OAuth 2.1 Authorization Server for benchmark-mcp.

Implements OAuthAuthorizationServerProvider with:
- Dynamic Client Registration (DCR)
- Simple password-based authorization (MCP_OAUTH_PASSWORD env var)
- In-memory storage (tokens lost on restart — acceptable for this use case)

Usage:
    Set MCP_OAUTH_PASSWORD env var (or Fly.io secret) to enable OAuth.
    When unset, the server runs without auth.
"""

import os
import time
import secrets
import logging
import hashlib
from urllib.parse import urlencode, urlunparse, urlparse

from pydantic import AnyUrl
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from mcp.server.auth.provider import (
    OAuthAuthorizationServerProvider,
    AuthorizationCode,
    AccessToken,
    RefreshToken,
    AuthorizationParams,
    AuthorizeError,
    TokenError,
    RegistrationError,
    construct_redirect_uri,
)

logger = logging.getLogger("benchmark-mcp.oauth")

MCP_OAUTH_PASSWORD = os.getenv("MCP_OAUTH_PASSWORD", "")
SERVER_URL = os.getenv("SERVER_URL", "https://benchmark-shoppings-br.fly.dev")

TOKEN_TTL = 3600 * 24  # 24h
CODE_TTL = 300  # 5 min


class BenchmarkOAuthProvider:
    """Self-contained OAuth provider with password-based authorization."""

    def __init__(self):
        self._clients: dict[str, OAuthClientInformationFull] = {}
        self._codes: dict[str, AuthorizationCode] = {}
        self._tokens: dict[str, AccessToken] = {}
        self._refresh_tokens: dict[str, RefreshToken] = {}
        # Map refresh_token -> access_token for revocation
        self._refresh_to_access: dict[str, str] = {}

    # ── Client Registration ───────────────────────────────────────────

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        if not client_info.client_id:
            client_info.client_id = secrets.token_urlsafe(24)
            client_info.client_secret = secrets.token_urlsafe(32)
            client_info.client_id_issued_at = int(time.time())
        self._clients[client_info.client_id] = client_info
        logger.info("Registered OAuth client: %s", client_info.client_id)

    # ── Authorization ─────────────────────────────────────────────────

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        """Returns URL to a simple login page."""
        # Build login page URL with all params needed to complete the flow
        login_params = {
            "client_id": client.client_id,
            "redirect_uri": str(params.redirect_uri),
            "code_challenge": params.code_challenge,
        }
        if params.state:
            login_params["state"] = params.state
        if params.scopes:
            login_params["scope"] = " ".join(params.scopes)
        if params.redirect_uri_provided_explicitly:
            login_params["redirect_uri_explicit"] = "1"

        return f"{SERVER_URL}/oauth/login?{urlencode(login_params)}"

    # ── Authorization Code ────────────────────────────────────────────

    def create_authorization_code(
        self,
        client_id: str,
        code_challenge: str,
        redirect_uri: str,
        redirect_uri_provided_explicitly: bool,
        scopes: list[str] | None = None,
    ) -> str:
        """Creates and stores an authorization code. Called by the login handler."""
        code = secrets.token_urlsafe(32)
        self._codes[code] = AuthorizationCode(
            code=code,
            client_id=client_id,
            code_challenge=code_challenge,
            redirect_uri=AnyUrl(redirect_uri),
            redirect_uri_provided_explicitly=redirect_uri_provided_explicitly,
            scopes=scopes or [],
            expires_at=time.time() + CODE_TTL,
        )
        logger.info("Created auth code for client %s", client_id)
        return code

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        code_obj = self._codes.get(authorization_code)
        if not code_obj:
            return None
        if code_obj.client_id != client.client_id:
            return None
        if time.time() > code_obj.expires_at:
            del self._codes[authorization_code]
            return None
        return code_obj

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        # Remove used code (one-time use)
        self._codes.pop(authorization_code.code, None)

        access = secrets.token_urlsafe(32)
        refresh = secrets.token_urlsafe(32)
        expires_at = int(time.time()) + TOKEN_TTL

        self._tokens[access] = AccessToken(
            token=access,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=expires_at,
        )
        self._refresh_tokens[refresh] = RefreshToken(
            token=refresh,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=expires_at + TOKEN_TTL,  # refresh lives longer
        )
        self._refresh_to_access[refresh] = access

        logger.info("Issued tokens for client %s", client.client_id)
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=TOKEN_TTL,
            refresh_token=refresh,
            scope=" ".join(authorization_code.scopes) if authorization_code.scopes else None,
        )

    # ── Refresh Token ─────────────────────────────────────────────────

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        rt = self._refresh_tokens.get(refresh_token)
        if not rt or rt.client_id != client.client_id:
            return None
        if rt.expires_at and time.time() > rt.expires_at:
            self._refresh_tokens.pop(refresh_token, None)
            return None
        return rt

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        # Revoke old tokens
        old_access = self._refresh_to_access.pop(refresh_token.token, None)
        if old_access:
            self._tokens.pop(old_access, None)
        self._refresh_tokens.pop(refresh_token.token, None)

        # Issue new tokens
        access = secrets.token_urlsafe(32)
        new_refresh = secrets.token_urlsafe(32)
        expires_at = int(time.time()) + TOKEN_TTL

        use_scopes = scopes or refresh_token.scopes

        self._tokens[access] = AccessToken(
            token=access,
            client_id=client.client_id,
            scopes=use_scopes,
            expires_at=expires_at,
        )
        self._refresh_tokens[new_refresh] = RefreshToken(
            token=new_refresh,
            client_id=client.client_id,
            scopes=use_scopes,
            expires_at=expires_at + TOKEN_TTL,
        )
        self._refresh_to_access[new_refresh] = access

        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=TOKEN_TTL,
            refresh_token=new_refresh,
            scope=" ".join(use_scopes) if use_scopes else None,
        )

    # ── Access Token Verification ─────────────────────────────────────

    async def load_access_token(self, token: str) -> AccessToken | None:
        at = self._tokens.get(token)
        if not at:
            return None
        if at.expires_at and time.time() > at.expires_at:
            self._tokens.pop(token, None)
            return None
        return at

    # ── Revocation ────────────────────────────────────────────────────

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        if isinstance(token, AccessToken):
            self._tokens.pop(token.token, None)
        elif isinstance(token, RefreshToken):
            old_access = self._refresh_to_access.pop(token.token, None)
            if old_access:
                self._tokens.pop(old_access, None)
            self._refresh_tokens.pop(token.token, None)


def verify_password(password: str) -> bool:
    """Constant-time password verification."""
    return secrets.compare_digest(password, MCP_OAUTH_PASSWORD)
