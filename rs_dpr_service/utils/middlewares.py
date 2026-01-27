# Copyright 2023-2025 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Common functions for fastapi middlewares.

NOTE: COPY-PASTED FROM RS-SERVER.
"""

import json
from collections.abc import Callable
from http import HTTPStatus
from typing import Any, TypedDict

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.concurrency import iterate_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from rs_dpr_service.utils.logging import Logging

# mypy: disable-error-code="index"


logger = Logging.default(__name__)


async def read_streaming_response(response: StreamingResponse) -> Any | None:
    """Read a json-formatted streaming response content"""
    body = [chunk async for chunk in response.body_iterator]
    splits = map(lambda x: x if isinstance(x, bytes) else x.encode(), body)  # type: ignore[union-attr]
    str_content = b"".join(splits).decode()
    py_content = json.loads(str_content) if str_content else None

    # Reset the StreamingResponse so it can be used again
    response.body_iterator = iterate_in_threadpool(iter(body))

    return py_content


class StacErrorResponse(TypedDict):
    """A JSON error response returned by the API, compliant with the STAC specification.

    The STAC API spec expects that `code` and `description` are both present in
    the payload.

    Attributes:
        code: A code representing the error, semantics are up to implementor.
        description: A description of the error.
    """

    code: str
    description: str


class Rfc7807ErrorResponse(TypedDict):
    """A JSON error response returned by the API, compliant with the RFC 7807 specification.

    Attributes:
        type: https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/{status_code}
        status: HTTP response status code
        detail: A description of the error.
    """

    type: str
    status: int
    detail: str


class HandleExceptionsMiddleware(BaseHTTPMiddleware):
    """
    Middleware to catch all exceptions and return a JSONResponse instead of raising them.
    This is useful in FastAPI when HttpExceptions are raised within the code but need to be handled gracefully.

    Attributes:
        rfc7807 (bool): If true, the returned content is compliant with RFC 7807. This is used by pygeoapi/ogc services.
        False by default = compliant to Stac specifications.
    """

    def __init__(self, app, rfc7807: bool = False, dispatch=None):
        """Constructor"""
        self.rfc7807: bool = rfc7807
        super().__init__(app, dispatch)

    @staticmethod
    def disable_default_exception_handler(app: FastAPI):
        """
        Disable the default FastAPI exception handler for HTTPException and StarletteHTTPException.
        We just re-raise the exceptions so they'll be handled by HandleExceptionsMiddleware.
        """

        @app.exception_handler(HTTPException)
        @app.exception_handler(StarletteHTTPException)
        async def exception_handler(_request: Request, _exc: HTTPException):
            """Implement disable_default_exception_handler"""
            # Note: we could raise(_exc) but it would increase the stack trace length with this module info.
            # We can just call raise because this function is called from an except clause.
            raise  # pylint: disable=misplaced-bare-raise

    async def dispatch(self, request: Request, call_next: Callable):
        try:
            # Call next middleware, get and return response, handle errors
            response = await call_next(request)
            return await self.handle_errors(response)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            return await self.handle_exceptions(request, exc)

    @staticmethod
    def format_code(status_code: int) -> str:
        """Convert e.g. HTTP_500_INTERNAL_SERVER_ERROR into 'InternalServerError'"""
        phrase = HTTPStatus(status_code).phrase
        return "".join(word.title() for word in phrase.split())

    @staticmethod
    def rfc7807_response(status_code: int, detail: str) -> Rfc7807ErrorResponse:
        """Return Rfc7807ErrorResponse instance"""
        return Rfc7807ErrorResponse(
            type=f"https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/{status_code}",
            status=status_code,
            detail=detail,
        )

    async def handle_errors(self, response: StreamingResponse) -> Response:
        """
        If no errors, just return the original response.
        In case of errors, log, format and return the response contents.
        """
        if not 400 <= response.status_code < 600:
            return response  # no error, return the original response

        # Read response content
        try:
            content = await read_streaming_response(response)

        # If we fail to read content, just return the original response
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(exc)
            return response

        # The content should be formated as a XxxErrorResponse
        formatted: Rfc7807ErrorResponse | StacErrorResponse | None = None
        try:
            if self.rfc7807:
                formatted = Rfc7807ErrorResponse(
                    type=str(content["type"]),
                    status=int(content["status"]),
                    detail=str(content["detail"]),
                )
            else:
                formatted = StacErrorResponse(code=str(content["code"]), description=str(content["description"]))
            if formatted != content:
                formatted = None
        except Exception:  # pylint: disable=broad-exception-caught # nosec B110
            pass

        # Else format the content
        if not formatted:
            description = json.dumps(content) if isinstance(content, (dict, list, set)) else str(content)
            if self.rfc7807:
                formatted = self.rfc7807_response(response.status_code, detail=description)
            else:
                formatted = StacErrorResponse(code=self.format_code(response.status_code), description=description)

        logger.error(f"{response.status_code}: {json.dumps(formatted)}")
        return JSONResponse(status_code=response.status_code, content=formatted)

    async def handle_exceptions(self, request: Request, exc: Exception) -> JSONResponse:
        """In case of exceptions, log the response contents"""

        # Log current stack trace
        logger.exception(exc)

        # Calculate HTTP response status code (int) and StacErrorResponse code (str) and description (str)
        if isinstance(exc, StarletteHTTPException):
            status_code = exc.status_code
            # Format int status code into str
            str_code = self.format_code(exc.status_code)
            description = str(exc.detail)

        else:
            # Use generic 400 or 500 code
            status_code = (
                status.HTTP_400_BAD_REQUEST
                if HandleExceptionsMiddleware.is_bad_request(request, exc)
                else status.HTTP_500_INTERNAL_SERVER_ERROR
            )
            str_code = exc.__class__.__name__
            description = str(exc)

        error_response: Rfc7807ErrorResponse | StacErrorResponse
        if self.rfc7807:
            error_response = self.rfc7807_response(status_code, detail=description)
        else:
            error_response = StacErrorResponse(code=str_code, description=description)
        return JSONResponse(status_code=status_code, content=error_response)

    @staticmethod
    def is_bad_request(request: Request, e: Exception) -> bool:
        """
        Determines if the request that raised this exception shall be considered as a bad request
        and return a 400 error code.

        This function can be overriden by the caller if needed with:
        HandleExceptionsMiddleware.is_bad_request = my_callable
        """
        return "bbox" in request.query_params and (
            str(e).endswith(" must have 4 or 6 values.") or str(e).startswith("could not convert string to float: ")
        )
